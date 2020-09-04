package org.cryptomator.fusecloudaccess;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import jnr.constants.platform.OpenFlags;
import org.cryptomator.cloudaccess.api.CloudItemMetadata;
import org.cryptomator.cloudaccess.api.CloudItemType;
import org.cryptomator.cloudaccess.api.CloudPath;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

class OpenFileFactory {

	private static final AtomicLong FILE_HANDLE_GEN = new AtomicLong();
	private static final Logger LOG = LoggerFactory.getLogger(OpenFileFactory.class);

	// activeFiles.compute is the primary barrier for synchronized access when creating/closing/moving OpenFiles
	private final ConcurrentMap<CloudPath, OpenFile> activeFiles;
	private final Cache<CloudPath, OpenFile> cachedFiles;
	private final Map<Long, OpenFile> fileHandles;
	private final CloudProvider provider;
	private final OpenFileUploader uploader;
	private final Path cacheDir;

	public OpenFileFactory(CloudProvider provider, OpenFileUploader uploader, Path cacheDir) {
		this(new ConcurrentHashMap<>(), provider, uploader, cacheDir);
	}

	// visible for testing
	OpenFileFactory(ConcurrentMap<CloudPath, OpenFile> activeFiles, CloudProvider provider, OpenFileUploader uploader, Path cacheDir) {
		this.activeFiles = activeFiles;
		this.cachedFiles = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.SECONDS).removalListener(this::removedFromCache).build();
		this.fileHandles = new HashMap<>();
		this.provider = provider;
		this.uploader = uploader;
		this.cacheDir = cacheDir;
	}

	/**
	 * @param path  path of the file to open
	 * @param flags file open options
	 * @return file handle used to identify and close open files.
	 */
	public long open(CloudPath path, Set<OpenFlags> flags, long initialSize, Instant lastModified) throws IOException {
		try {
			if (flags.contains(OpenFlags.O_RDWR) || flags.contains(OpenFlags.O_WRONLY)) {
				uploader.cancel(path);
			}
			var openFile = activeFiles.compute(path, (p, activeFile) -> {
				OpenFile file;
				if (activeFile != null) { // 1. use active file (if present)
					file = activeFile;
				} else if ((file = cachedFiles.getIfPresent(p)) != null) { // 2. reactivate cached file (if present)
					cachedFiles.invalidate(p);
					file.setLastModified(lastModified);
				} else { // 3. create new file
					file = createOpenFile(p, initialSize, lastModified);
				}
				file.getOpenFileHandleCount().incrementAndGet();
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
	OpenFile createOpenFile(CloudPath path, long initialSize, Instant lastModified) {
		try {
			var tmpFile = cacheDir.resolve(UUID.randomUUID().toString());
			return OpenFile.create(path, tmpFile, provider, initialSize, lastModified);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public Optional<OpenFile> get(long fileHandle) {
		return Optional.ofNullable(fileHandles.get(fileHandle));
	}

	/**
	 * Updates existing cached data for <code>newPath</code> (if any) with contents formerly mapped to <code>oldPath</code>,
	 * invalidates <code>oldPath</code> and cancel pending uploads for both paths (if any).
	 * <p>
	 * Cached data previously mapped to <code>newPath</code> will be discarded. No-op if no data cached for either path.
	 * <p>
	 *
	 * @param oldPath Path to a cached file before it has been moved
	 * @param newPath New path which is used to access the cached file
	 */
	public synchronized void moved(CloudPath oldPath, CloudPath newPath) {
		//uploader.cancel(oldPath);
		//uploader.cancel(newPath);
		var previouslyActiveFile = activeFiles.remove(newPath);
		if (previouslyActiveFile != null) {
			previouslyActiveFile.close();
		}
		var activeFile = activeFiles.remove(oldPath);
		if (activeFile != null) {
			activeFile.updatePath(newPath);
			activeFiles.put(newPath, activeFile);
			//TODO: if an upload for oldPath already existed, we need to trigger a new one for new path
		}
		cachedFiles.invalidate(newPath);
		var cachedFile = cachedFiles.getIfPresent(oldPath);
		if (cachedFile != null) {
			cachedFiles.put(newPath, cachedFile);
			cachedFile.updatePath(newPath);
			cachedFiles.invalidate(oldPath);
		}
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
		uploader.cancel(path); // TODO: how to cancel uploads?
		activeFiles.compute(path, (p, activeFile) -> {
			OpenFile file;
			if (activeFile != null) {
				file = activeFile;
			} else if ((file = cachedFiles.getIfPresent(path)) != null) {
				cachedFiles.invalidate(path);
			}
			file.close();
			return null; // removes entry from map
		});
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
		activeFiles.computeIfPresent(path, (p, activeFile) -> {
			if (activeFile.getOpenFileHandleCount().decrementAndGet() == 0) { // was this the last file handle?
				uploader.scheduleUpload(activeFile).thenAccept(this::onFinishedUpload);
			}
			return activeFile; // DO NOT remove the mapping yet! this will be done in #onFinishedUpload
		});
	}

	private void onFinishedUpload(CloudPath path) {
		activeFiles.compute(path, (p, activeFile) -> {
			if (activeFile.getOpenFileHandleCount().get() == 0) { // hasn't the file been re-opened in the meantime?
				cachedFiles.put(path, activeFile); // transition to cached state
				return null; // remove mapping
			} else { // file has been reopened in the meantime
				return activeFile; // keep the mapping
			}
		});
	}

	private void removedFromCache(RemovalNotification<CloudPath, OpenFile> removalNotification) {
		// manual removal may occur inside this class
		// we must not close the openFile unless it was removed due to automatic cache eviction
		if (removalNotification.wasEvicted()) {
			var openFile = removalNotification.getValue();
			LOG.info("clean up cached file {}", openFile.getPath());
			openFile.close();
		}
	}

	/**
	 * Returns metadata from cache. This is not threadsafe and the returned metadata might refer to an
	 * file that got evicted just in this moment.
	 * @param path
	 * @return OpenFile from either {@link #activeFiles} or {@link #cachedFiles}
	 */
	public Optional<CloudItemMetadata> getCachedMetadata(CloudPath path) {
		return getCachedFile(path).map(file -> {
			var lastModified = Optional.of(file.getLastModified());
			var size = Optional.of(file.getSize());
			return new CloudItemMetadata(path.getFileName().toString(), path, CloudItemType.FILE, lastModified, size);
		});
	}

	// visible for testing - do not use outside of #getCachedMetadata
	Optional<OpenFile> getCachedFile(CloudPath path) {
		var file = activeFiles.get(path);
		if (file != null) {
			return Optional.of(file);
		} else {
			return Optional.ofNullable(cachedFiles.getIfPresent(path));
		}
	}

}
