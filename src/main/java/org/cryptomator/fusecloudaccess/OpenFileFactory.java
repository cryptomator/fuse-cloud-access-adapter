package org.cryptomator.fusecloudaccess;

import com.google.common.io.ByteStreams;
import jnr.constants.platform.OpenFlags;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.ProgressListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

class OpenFileFactory {

	private static final Logger LOG = LoggerFactory.getLogger(OpenFileFactory.class);
	private static final String TMP_FILE_PREFIX = "file_";

	private final ConcurrentMap<Long, OpenFile> openFiles = new ConcurrentHashMap<>();
	private final AtomicLong fileHandleGen = new AtomicLong();
	private final CloudProvider provider;
	private final Path cacheDir;

	public OpenFileFactory(CloudProvider provider) {
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
	public long open(Path path, Set<OpenFlags> flags) {
		long fileHandle = fileHandleGen.getAndIncrement();
		var forWriting = flags.contains(OpenFlags.O_RDWR) || flags.contains(OpenFlags.O_WRONLY);
		final CompletionStage<Path> cacheFile;
		if (forWriting) {
			cacheFile = provider.read(path, ProgressListener.NO_PROGRESS_AWARE).thenCompose(in -> {
				var tmpFile = cacheDir.resolve(TMP_FILE_PREFIX + fileHandle);
				try (var src = in;
					 var dst = Files.newOutputStream(tmpFile, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
					ByteStreams.copy(src, dst);
					return CompletableFuture.completedFuture(tmpFile);
				} catch (IOException e) {
					return CompletableFuture.failedFuture(e);
				}
			});
		} else {
			cacheFile = CompletableFuture.failedFuture(new IllegalStateException("Cache file only loaded for write access."));
		}

		OpenFile file = new OpenFile(provider, path, flags, cacheFile);
		openFiles.put(fileHandle, file);
		LOG.trace("Opening file {} {}", fileHandle, file);
		return fileHandle;
	}

	public Optional<OpenFile> get(long fileHandle) {
		return Optional.ofNullable(openFiles.get(fileHandle));
	}

	/**
	 * Closes the channel identified by the given fileHandle
	 *
	 * @param fileHandle file handle used to identify
	 */
	public CompletionStage<Void> close(long fileHandle) {
		OpenFile file = openFiles.remove(fileHandle);
		if (file != null) {
			LOG.trace("Releasing file {} {}...", fileHandle, file);
			return file.flush().thenRun(() -> {
				LOG.debug("Released file {} {}", fileHandle, file);
			});
		} else {
			LOG.trace("No open file with handle {} found.", fileHandle);
			return CompletableFuture.completedFuture(null);
		}
	}

}
