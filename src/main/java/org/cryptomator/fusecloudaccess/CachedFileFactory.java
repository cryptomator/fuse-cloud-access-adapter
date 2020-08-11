package org.cryptomator.fusecloudaccess;

import jnr.constants.platform.OpenFlags;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
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

	// path -> 1 cachedFile -> n fileHandles
	private final ConcurrentMap<Path, CachedFile> cachedFiles = new ConcurrentHashMap<>();
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
	public CachedFileHandle open(Path path, Set<OpenFlags> flags, long initialSize) throws IOException {
		try {
			var cachedFile = cachedFiles.computeIfAbsent(path, p -> this.createCachedFile(p, initialSize));
			var fileHandle = cachedFile.openFileHandle(flags);
			if (flags.contains(OpenFlags.O_TRUNC)) {
				cachedFile.truncate(0);
			}
			fileHandles.put(fileHandle.getId(), fileHandle);
			return fileHandle;
		} catch (UncheckedIOException e) {
			throw new IOException(e);
		}
	}

	private CachedFile createCachedFile(Path path, long initialSize) {
		try {
			var tmpFile = cacheDir.resolve(UUID.randomUUID().toString());
			return CachedFile.create(path, tmpFile, provider, initialSize);
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
		CachedFileHandle handle = fileHandles.remove(handleId);
		if (handle != null) {
			return handle.release();
		} else {
			return CompletableFuture.completedFuture(null);
		}
	}

}
