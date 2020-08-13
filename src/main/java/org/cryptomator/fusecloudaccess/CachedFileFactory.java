package org.cryptomator.fusecloudaccess;

import jnr.constants.platform.OpenFlags;
import org.cryptomator.cloudaccess.api.CloudItemMetadata;
import org.cryptomator.cloudaccess.api.CloudPath;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class CachedFileFactory {

	private static final Logger LOG = LoggerFactory.getLogger(CachedFileFactory.class);

	private final ConcurrentMap<CloudPath, CachedFile> cachedFiles = new ConcurrentHashMap<>();
	private final Map<Long, CachedFileHandle> fileHandles = new HashMap<>();
	private final CloudProvider provider;
	private final Path cacheDir;

	public CachedFileFactory(CloudProvider provider) {
		this.provider = provider;
		try {
			// TODO use configurable path instead?
			this.cacheDir = Files.createTempDirectory("fuse-cache");
		} catch (IOException e) {
			throw new IllegalStateException("Failed to create tmp dir", e);
		}
	}

	/**
	 * @param path  path of the file to open
	 * @param flags file open options
	 * @return file handle used to identify and close open files.
	 */
	public CachedFileHandle open(CloudPath path, Set<OpenFlags> flags, long initialSize, Instant lastModified) throws IOException {
		try {
			var cachedFile = cachedFiles.computeIfAbsent(path, p -> this.createCachedFile(p, initialSize, lastModified));
			var fileHandle = cachedFile.openFileHandle();
			if (flags.contains(OpenFlags.O_TRUNC)) {
				cachedFile.truncate(0);
			}
			fileHandles.put(fileHandle.getId(), fileHandle);
			return fileHandle;
		} catch (UncheckedIOException e) {
			throw new IOException(e);
		}
	}

	private CachedFile createCachedFile(CloudPath path, long initialSize, Instant lastModified) {
		try {
			var tmpFile = cacheDir.resolve(UUID.randomUUID().toString());
			return CachedFile.create(path, tmpFile, provider, initialSize, lastModified, p -> {
				try {
					Files.deleteIfExists(tmpFile);
				} catch (IOException e) {
					LOG.warn("Failed to clean up cached data {}", tmpFile);
				} finally {
					cachedFiles.remove(p);
				}
			});
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public Optional<CachedFileHandle> get(long fileHandle) {
		return Optional.ofNullable(fileHandles.get(fileHandle));
	}

	/**
	 * Closes the channel identified by the given fileHandle
	 *
	 * @param handleId file handle used to identify
	 */
	public CompletionStage<Void> close(long handleId) {
		CachedFileHandle handle = fileHandles.remove(handleId); // TODO Performance: schedule removal, but keep cached for a few seconds
		if (handle != null) {
			return handle.release();
		} else {
			return CompletableFuture.completedFuture(null);
		}
	}

	/**
	 * Updates existing cached data for <code>newPath</code> (if any) with contents formerly mapped to <code>oldPath</code> and invalidates <code>oldPath</code>.
	 * <p>
	 * Cached data previously mapped to <code>newPath</code> will be discarded. No-op if no data cached for either path.
	 *
	 * @param oldPath Path to a cached file before it has been moved
	 * @param newPath New path which is used to access the cached file
	 */
	public void moved(CloudPath oldPath, CloudPath newPath) {
		var cachedFile = cachedFiles.get(oldPath);
		cachedFiles.compute(newPath, (path, previouslyCachedFile) -> {
			if (previouslyCachedFile != null) {
				previouslyCachedFile.markDeleted();
			}
			if (cachedFile != null) {
				cachedFile.updatePath(newPath);
			}
			return cachedFile; // removes entry from map if
		});
	}

	/**
	 * Invalidates any mapping for the given <code>path</code>.
	 * <p>
	 * Cached data for the given <code>path</code> will be discarded. No-op if no data is cached for the given path.
	 *
	 * @param path Path to a cached file
	 */
	public void delete(CloudPath path) {
		cachedFiles.compute(path, (p, cachedFile) -> {
			if (cachedFile != null) {
				cachedFile.markDeleted();
			}
			return null; // removes entry from map
		});
	}

	Optional<CloudItemMetadata> getCachedMetadata(CloudPath path) {
		return Optional.ofNullable(cachedFiles.get(path)).map(CachedFile::getMetadata);
	}
}
