package org.cryptomator.fusecloudaccess;

import com.google.common.io.ByteStreams;
import jnr.constants.platform.OpenFlags;
import jnr.ffi.Pointer;
import org.cryptomator.cloudaccess.api.CloudItemMetadata;
import org.cryptomator.cloudaccess.api.CloudItemType;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.ProgressListener;
import org.cryptomator.cloudaccess.api.exceptions.AlreadyExistsException;
import org.cryptomator.cloudaccess.api.exceptions.NotFoundException;
import org.cryptomator.cloudaccess.api.exceptions.TypeMismatchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFS;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

public class CloudAccessFS extends FuseStubFS implements FuseFS {

	private static final Logger LOG = LoggerFactory.getLogger(CloudAccessFS.class);

	private final CloudProvider provider;
	private final int timeoutMillis;
	private final CachedFileFactory cachedFileFactory;
	private final OpenDirFactory openDirFactory;

	public CloudAccessFS(CloudProvider provider, int timeoutMillis) {
		this(provider, timeoutMillis, new CachedFileFactory(provider), new OpenDirFactory(provider));
	}

	//Visible for testing
	CloudAccessFS(CloudProvider provider, int timeoutMillis, CachedFileFactory cachedFileFactory, OpenDirFactory openDirFactory) {
		this.provider = provider;
		this.timeoutMillis = timeoutMillis;
		this.cachedFileFactory = cachedFileFactory;
		this.openDirFactory = openDirFactory;
	}

	/**
	 * Method for async execution.
	 * <p>
	 * Only visible for testing.
	 *
	 * @param returnCode an integer {@link CompletionStage} to execute
	 * @return an integer representing one of the FUSE {@link ErrorCodes}
	 */
	int returnOrTimeout(CompletionStage<Integer> returnCode) {
		try {
			return returnCode.toCompletableFuture().get(timeoutMillis, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			LOG.warn("async call interrupted");
			Thread.currentThread().interrupt();
			return -ErrorCodes.EINTR();
		} catch (ExecutionException e) {
			LOG.error("encountered unhandled exception", e.getCause());
			return -ErrorCodes.EIO();
		} catch (TimeoutException e) {
			return -ErrorCodes.ETIMEDOUT();
		}
	}

	@Override
	public int getattr(String path, FileStat stat) {
		var cachedMetadata = cachedFileFactory.getCachedMetadata(Path.of(path));
		if (cachedMetadata.isPresent()) {
			Attributes.copy(cachedMetadata.get(), stat);
			return 0;
		}
		var returnCode = provider.itemMetadata(Path.of(path)).thenApply(metadata -> {
			Attributes.copy(metadata, stat);
			return 0;
		}).exceptionally(e -> {
			if (e.getCause() instanceof NotFoundException) {
				return -ErrorCodes.ENOENT();
			} else {
				LOG.error("getattr() failed", e);
				return -ErrorCodes.EIO();
			}
		});
		return returnOrTimeout(returnCode);
	}

	@Override
	public int opendir(String path, FuseFileInfo fi) {
		var returnCode = provider.itemMetadata(Path.of(path)).thenApply(metadata -> {
			if (metadata.getItemType() == CloudItemType.FOLDER) {
				long dirHandle = openDirFactory.open(Path.of(path));
				fi.fh.set(dirHandle);
				return 0;
			} else {
				return -ErrorCodes.ENOTDIR();
			}
		}).exceptionally(e -> {
			if (e.getCause() instanceof NotFoundException) {
				return -ErrorCodes.ENOENT();
			} else {
				LOG.error("open() failed", e);
				return -ErrorCodes.EIO();
			}
		});
		return returnOrTimeout(returnCode);
	}

	@Override
	public int readdir(String path, Pointer buf, FuseFillDir filler, long offset, FuseFileInfo fi) {
		if (offset > Integer.MAX_VALUE) {
			LOG.error("readdir() only supported for up to 2^31 entries, but attempted to read from offset {}", offset);
			return -ErrorCodes.EOVERFLOW();
		}

		var openDir = openDirFactory.get(fi.fh.get());
		if (openDir.isEmpty()) {
			return -ErrorCodes.EBADF();
		}

		var returnCode = openDir.get().list(buf, filler, (int) offset).exceptionally(e -> {
			LOG.error("readdir() failed", e); // TODO distinguish causes
			return -ErrorCodes.EIO();
		});
		return returnOrTimeout(returnCode);
	}

	@Override
	public int releasedir(String path, FuseFileInfo fi) {
		openDirFactory.close(fi.fh.get());
		return 0;
	}

	@Override
	public int open(String path, FuseFileInfo fi) {
		var returnCode = provider.itemMetadata(Path.of(path)).thenApply(metadata -> {
			final var type = metadata.getItemType();
			if (type == CloudItemType.FILE) {
				try {
					var size = metadata.getSize().orElse(0l);
					var lastModified = metadata.getLastModifiedDate().orElse(Instant.EPOCH);
					var handle = cachedFileFactory.open(Path.of(path), BitMaskEnumUtil.bitMaskToSet(OpenFlags.class, fi.flags.longValue()), size, lastModified);
					fi.fh.set(handle.getId());
					return 0;
				} catch (IOException e) {
					return -ErrorCodes.EIO();
				}
			} else if (type == CloudItemType.FOLDER) {
				return -ErrorCodes.EISDIR();
			} else {
				LOG.error("Attempted to open() {}, which is not a file.", path);
				return -ErrorCodes.EIO();
			}
		}).exceptionally(e -> {
			if (e.getCause() instanceof NotFoundException) {
				return -ErrorCodes.ENOENT();
			} else {
				LOG.error("open() failed", e);
				return -ErrorCodes.EIO();
			}
		});
		return returnOrTimeout(returnCode);
	}

	@Override
	public int release(String path, FuseFileInfo fi) {
		var returnCode = cachedFileFactory.close(fi.fh.get()).thenApply(ignored -> 0).exceptionally(e -> {
			LOG.error("release() failed", e);
			return -ErrorCodes.EIO();
		});
		return returnOrTimeout(returnCode);
	}

	@Override
	public int rename(String oldpath, String newpath) {
		var returnCode = provider.move(Path.of(oldpath), Path.of(newpath), true)
				.thenApply(p -> {
					cachedFileFactory.moved(Path.of(oldpath), Path.of(newpath));
					return 0;
				})
				.exceptionally(completionThrowable -> {
					var e = completionThrowable.getCause();
					if (e instanceof NotFoundException) {
						return -ErrorCodes.ENOENT();
					} else if (e instanceof AlreadyExistsException) {
						return -ErrorCodes.EEXIST();
					} else {
						LOG.error("rename() failed", e);
						return -ErrorCodes.EIO();
					}
				});
		return returnOrTimeout(returnCode);
	}

	@Override
	public int mkdir(String path, long mode) {
		var returnCode = provider.createFolder(Path.of(path))
				.thenApply(p -> 0)
				.exceptionally(completionThrowable -> {
					var e = completionThrowable.getCause();
					if (e instanceof AlreadyExistsException) {
						return -ErrorCodes.EEXIST();
					} else {
						LOG.error("mkdir() failed", e);
						return -ErrorCodes.EIO();
					}
				});
		return returnOrTimeout(returnCode);
	}

	@Override
	public int create(String rawPath, long mode, FuseFileInfo fi) {
		final var path = Path.of(rawPath);
		var returnCode = provider
				.write(path, false, InputStream.nullInputStream(), ProgressListener.NO_PROGRESS_AWARE)
				.handle((metadata, exception) -> {
					if (exception == null) {
						// no exception means: 0-byte file successfully created
						return CompletableFuture.completedFuture(metadata);
					} else if (exception instanceof AlreadyExistsException) {
						// in case of an already existing file, return the existing file's metadata
						return provider.itemMetadata(path);
					} else {
						return CompletableFuture.<CloudItemMetadata>failedFuture(exception);
					}
				})
				.thenCompose(Function.identity())
				.thenApply(metadata -> {
					try {
						var size = metadata.getSize().orElse(0l);
						var lastModified = metadata.getLastModifiedDate().orElse(Instant.EPOCH);
						var handle = cachedFileFactory.open(path, BitMaskEnumUtil.bitMaskToSet(OpenFlags.class, fi.flags.longValue()), size, lastModified);
						fi.fh.set(handle.getId());
						return 0;
					} catch (IOException e) {
						return -ErrorCodes.EIO();
					}
				})
				.exceptionally(e -> {
					if (e.getCause() instanceof NotFoundException) {
						return -ErrorCodes.ENOENT();
					} else if (e.getCause() instanceof TypeMismatchException) {
						return -ErrorCodes.EISDIR();
					} else {
						LOG.error("create() failed", e);
						return -ErrorCodes.EIO();
					}
				});
		return returnOrTimeout(returnCode);
	}

	@Override
	public int chmod(String path, long mode) {
		// TODO: This must be implemented! Otherwise TextEdit.app fails to save text files.
		return 0;
	}

	@Override
	public int rmdir(String path) {
		return deleteResource(Path.of(path), "rmdir() failed");
	}

	@Override
	public int unlink(String path) {
		return deleteResource(Path.of(path), "unlink() failed");
	}

	// visible for testing
	int deleteResource(Path path, String msgOnError) {
		var returnCode = provider.delete(path)
				.thenApply(ignored -> {
					cachedFileFactory.delete(path);
					return 0;
				})
				.exceptionally(completionThrowable -> {
					final var e = completionThrowable.getCause();
					if (e instanceof NotFoundException) {
						return -ErrorCodes.ENOENT();
					} else {
						LOG.error(msgOnError, e);
						return -ErrorCodes.EIO();
					}
				});
		return returnOrTimeout(returnCode);
	}

	@Override
	public int read(String path, Pointer buf, long size, long offset, FuseFileInfo fi) {
		var openFile = cachedFileFactory.get(fi.fh.get());
		if (openFile.isEmpty()) {
			return -ErrorCodes.EBADF();
		}
		var returnCode = openFile.get().read(buf, offset, size).exceptionally(e -> {
			if (e instanceof NotFoundException) {
				return -ErrorCodes.ENOENT();
			} else if (e instanceof EOFException) {
				return 0; // offset was at or beyond EOF
			} else {
				LOG.error("read() failed", e);
				return -ErrorCodes.EIO();
			}
		});
		return returnOrTimeout(returnCode);
	}

	@Override
	public int write(String path, Pointer buf, long size, long offset, FuseFileInfo fi) {
		var openFile = cachedFileFactory.get(fi.fh.get());
		if (openFile.isEmpty()) {
			return -ErrorCodes.EBADF();
		}
		var returnCode = openFile.get().write(buf, offset, size).exceptionally(e -> {
			if (e instanceof NotFoundException) {
				return -ErrorCodes.ENOENT();
			} else {
				LOG.error("write() failed", e);
				return -ErrorCodes.EIO();
			}
		});
		return returnOrTimeout(returnCode);
	}

	@Override
	public int truncate(String path, long size) {
		var file = Path.of(path);
		LOG.info("TRUNCATE {} to {}", path, size);
		var returnCode = provider.read(file, 0, size, ProgressListener.NO_PROGRESS_AWARE).thenCompose(in -> {
			// TODO optimize tmp file path
			try (var ch = FileChannel.open(Files.createTempFile("foo", "bar"), StandardOpenOption.READ, StandardOpenOption.WRITE)) {
				long copied = ByteStreams.copy(in, Channels.newOutputStream(ch));
				if (copied < size) {
					// TODO append
				}
				ch.position(0);
				return provider.write(file, true, Channels.newInputStream(ch), ProgressListener.NO_PROGRESS_AWARE).thenApply(ignored -> 0);
			} catch (IOException e) {
				return CompletableFuture.failedFuture(e);
			}
		});
		return returnOrTimeout(returnCode);
	}

	@Override
	public int ftruncate(String path, long size, FuseFileInfo fi) {
		var handle = cachedFileFactory.get(fi.fh.get());
		if (handle.isEmpty()) {
			return -ErrorCodes.EBADF();
		}
		var returnCode = handle.get().truncate(size).thenApply(ignored -> 0).exceptionally(e -> {
			LOG.error("ftruncate() failed", e);
			return -ErrorCodes.EIO();
		});
		return returnOrTimeout(returnCode);
	}
}
