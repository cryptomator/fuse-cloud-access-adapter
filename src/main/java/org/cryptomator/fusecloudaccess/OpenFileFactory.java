package org.cryptomator.fusecloudaccess;

import com.google.common.base.Preconditions;
import jnr.constants.platform.OpenFlags;
import org.cryptomator.cloudaccess.api.CloudItemMetadata;
import org.cryptomator.cloudaccess.api.CloudItemType;
import org.cryptomator.cloudaccess.api.CloudPath;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@FileSystemScoped
class OpenFileFactory {

	private static final AtomicLong FILE_HANDLE_GEN = new AtomicLong();
	private static final Logger LOG = LoggerFactory.getLogger(OpenFileFactory.class);
	private static final int KEEP_IDLE_FILE_SECONDS = 10; // TODO make configurable

	/*
	 * activeFiles.compute is the primary barrier for synchronized access when creating/closing/moving OpenFiles
	 * OpenFile.close() as well as modifications to OpenFile.getOpenFileHandleCount() MUST be protected by this
	 * means of synchronization.
	 */
	private final ConcurrentMap<CloudPath, OpenFile> openFiles;
	private final Map<Long, OpenFile> fileHandles;
	private final CloudProvider provider;
	private final OpenFileUploader uploader;
	private final Path cacheDir;
	private final ScheduledExecutorService scheduler;

	@Inject
	OpenFileFactory(@Named("openFiles") ConcurrentMap<CloudPath, OpenFile> openFiles, CloudProvider provider, OpenFileUploader uploader, Path cacheDir, ScheduledExecutorService scheduler) {
		this.openFiles = openFiles;
		this.fileHandles = new HashMap<>();
		this.provider = provider;
		this.uploader = uploader;
		this.cacheDir = cacheDir;
		this.scheduler = scheduler;
	}

	/**
	 * @param path  path of the file to open
	 * @param flags file open options
	 * @return file handle used to identify and close open files.
	 */
	public long open(CloudPath path, Set<OpenFlags> flags, long initialSize, Instant lastModified) throws IOException {
		try {
			var openFile = openFiles.compute(path, (p, file) -> {
				if (file == null) {
					file = createOpenFile(p, initialSize);
				}
				file.getOpenFileHandleCount().incrementAndGet();
				file.setLastModified(lastModified);
				return file;
			});
			if (flags.contains(OpenFlags.O_TRUNC)) {
				openFile.truncate(0);
			}
			var handleId = FILE_HANDLE_GEN.incrementAndGet();
			fileHandles.put(handleId, openFile);
			return handleId;
		} catch (UncheckedIOException e) {
			throw new IOException(e);
		}
	}

	//visible for testing
	OpenFile createOpenFile(CloudPath path, long initialSize) {
		try {
			var tmpFile = cacheDir.resolve(UUID.randomUUID().toString());
			return OpenFile.create(path, tmpFile, provider, initialSize);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public Optional<OpenFile> get(long fileHandle) {
		return Optional.ofNullable(fileHandles.get(fileHandle));
	}

	public void move(CloudPath oldPath, CloudPath newPath) {
		for (CloudPath path : openFiles.keySet()) {
			if (path.startsWith(oldPath)) {
				moveSingleFile(path, newPath.resolve(oldPath.relativize(path)));
			}
		}
	}

	/**
	 * Updates existing cached data for <code>newPath</code> (if any) with contents formerly mapped to <code>oldPath</code>,
	 * invalidates <code>oldPath</code> and cancel pending uploads for the {@code newPath}
	 * <p>
	 * Cached data previously mapped to <code>newPath</code> will be discarded. No-op if no data cached for either path.
	 * <p>
	 *
	 * @param oldPath Path to a cached file before it has been moved
	 * @param newPath New path which is used to access the cached file
	 */
	void moveSingleFile(CloudPath oldPath, CloudPath newPath) {
		Preconditions.checkArgument(!oldPath.equals(newPath));
		uploader.cancelUpload(newPath);
		var activeFile = openFiles.remove(oldPath);
		LOG.debug("Moving {} from {} -> {}", activeFile, oldPath, newPath);
		openFiles.compute(newPath, (p, previouslyActiveFile) -> {
			assert previouslyActiveFile == null || previouslyActiveFile != activeFile; // if previousActiveFile is non-null, it must not be the same as activeFile!
			if (previouslyActiveFile != null) {
				LOG.debug("Closing {}. Replaced by move()", p);
				previouslyActiveFile.close();
			}
			if (activeFile != null) {
				LOG.debug("Setting path of {} to {}", activeFile, p);
				activeFile.setPath(newPath);
			}
			return activeFile;
		});
	}

	/**
	 * Invalidates any mapping for the given <code>path</code>.
	 * <p>
	 * Cached data for the given <code>path</code> will be discarded and any pending upload from a previous change is canceled.
	 * No-op if no data is cached for the given path.
	 *
	 * @param path Path to a cached file
	 */
	public void delete(CloudPath path) {
		uploader.cancelUpload(path);
		openFiles.computeIfPresent(path, (p, file) -> {
			LOG.debug("Closing deleted file {} {}", p, file);
			file.close();
			return null; // removes entry from map
		});
	}

	public void deleteDescendants(CloudPath parent) {
		for (CloudPath path : openFiles.keySet()) {
			if (path.startsWith(parent)) {
				delete(path);
			}
		}
	}

	/**
	 * Closes the fileHandle. If all handles for a given file are closed, the file contents are scheduled for persistence and the file will be marked for eventual eviction.
	 *
	 * @param handleId file handle
	 */
	public void close(long handleId) {
		OpenFile file = fileHandles.remove(handleId);
		if (file == null) {
			LOG.warn("No such file handle: {}", handleId);
			return;
		}
		var path = file.getPath();
		openFiles.computeIfPresent(path, (p, f) -> {
			if (f.getOpenFileHandleCount().decrementAndGet() == 0 && f.transitionToUploading()) { // was this the last file handle?
				uploader.scheduleUpload(f, this::scheduleClose);
			}
			if (f.getState() == OpenFile.State.UNMODIFIED) {
				scheduleClose(f);
			}
			return f; // DO NOT remove the mapping yet! this might be done in #scheduleClose
		});
	}

	private void scheduleClose(OpenFile file) {
		scheduler.schedule(() -> closeFileIfIdle(file.getPath()), KEEP_IDLE_FILE_SECONDS, TimeUnit.SECONDS);
	}

	private void closeFileIfIdle(CloudPath path) {
		openFiles.computeIfPresent(path, (p, activeFile) -> {
			if (activeFile.getOpenFileHandleCount().get() > 0 // file has been reopened
					|| activeFile.getState() != OpenFile.State.UNMODIFIED) { // file is scheduled for upload
				return activeFile; // keep the mapping
			} else {
				LOG.trace("Closing idle file {}", path);
				activeFile.close();
				return null; // remove mapping
			}
		});
	}

	/**
	 * Returns metadata from cache. This is not threadsafe and the returned metadata might refer to an
	 * file that got evicted just in this moment.
	 *
	 * @param path
	 * @return Optional metadata, which is present if cached
	 */
	public Optional<CloudItemMetadata> getCachedMetadata(CloudPath path) {
		AtomicReference<CloudItemMetadata> result = new AtomicReference<>();
		openFiles.computeIfPresent(path, (p, file) -> {
			var lastModified = Optional.of(file.getLastModified());
			var size = Optional.of(file.getSize());
			result.set(new CloudItemMetadata(path.getFileName().toString(), path, CloudItemType.FILE, lastModified, size));
			return file;
		});
		return Optional.ofNullable(result.get());
	}

}
