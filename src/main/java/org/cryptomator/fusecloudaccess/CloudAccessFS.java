package org.cryptomator.fusecloudaccess;

import jnr.constants.platform.OpenFlags;
import jnr.ffi.Pointer;
import org.cryptomator.cloudaccess.api.CloudItemType;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.ProgressListener;
import org.cryptomator.cloudaccess.api.exceptions.AlreadyExistsException;
import org.cryptomator.cloudaccess.api.exceptions.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFS;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

public class CloudAccessFS extends FuseStubFS implements FuseFS {

	private static final Logger LOG = LoggerFactory.getLogger(CloudAccessFS.class);

	private final CloudProvider provider;
	private final int timeoutMillis;
	private final OpenFileFactory openFileFactory;
	private final OpenDirFactory openDirFactory;

	public CloudAccessFS(CloudProvider provider, int timeoutMillis) {
		this(provider, timeoutMillis, new OpenFileFactory(provider), new OpenDirFactory(provider));
	}

	//Visible for testing
	CloudAccessFS(CloudProvider provider, int timeoutMillis, OpenFileFactory openFileFactory, OpenDirFactory openDirFactory) {
		this.provider = provider;
		this.timeoutMillis = timeoutMillis;
		this.openFileFactory = openFileFactory;
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
			long fileHandle = openFileFactory.open(Path.of(path), BitMaskEnumUtil.bitMaskToSet(OpenFlags.class, fi.flags.longValue()));
			fi.fh.set(fileHandle);
			return 0;
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
		openFileFactory.close(fi.fh.get());
		return 0;
	}

	@Override
	public int rename(String oldpath, String newpath) {
		//TODO: do we have to check if one of these things are already opened in this filesystem?
		//TODO: What should be default if the source already exists?
		var returnCode = provider.move(Path.of(oldpath), Path.of(newpath), false)
				.thenApply(p -> 0)
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
		Function<Path, Integer> openFunk = p -> {
			long handle = openFileFactory.open(p, BitMaskEnumUtil.bitMaskToSet(OpenFlags.class, fi.flags.longValue()));
			fi.fh.set(handle);
			return 0;
		};
		final var path = Path.of(rawPath);

		var returnCode = provider.write(path, false, InputStream.nullInputStream(), ProgressListener.NO_PROGRESS_AWARE)
				.thenApply(itemMetadata -> openFunk.apply(itemMetadata.getPath()))
				.exceptionally(completionThrowable -> {
					var e = completionThrowable.getCause();
					if (e instanceof AlreadyExistsException) {
						return openFunk.apply(path); //by contract the file is opened if it already exists
					} else if (e instanceof NotFoundException) {
						return -ErrorCodes.ENOENT();
						//TODO: If TypeMismatchException is thrown, the type can be either directory or unknown. Hence, returning EISDIR is not always correct and we cannot find out which type the resource is
						//} else if (e instanceof TypeMismatchException) {
						//	return -ErrorCodes.EISDIR();
					} else {
						LOG.error("create() failed", e);
						return -ErrorCodes.EIO();
					}
				});
		return returnOrTimeout(returnCode);
	}
//
//	@Override
//	public int rmdir(String path) {
//		return super.rmdir(path);
//	}
//
//	@Override
//	public int unlink(String path) {
//		return super.unlink(path);
//	}

	@Override
	public int read(String path, Pointer buf, long size, long offset, FuseFileInfo fi) {
		var openFile = openFileFactory.get(fi.fh.get());
		if (openFile.isEmpty()) {
			return -ErrorCodes.EBADF();
		}
		var returnCode = openFile.get().read(buf, offset, size).exceptionally(e -> {
			if (e instanceof NotFoundException) {
				return -ErrorCodes.ENOENT();
			} else {
				LOG.error("read() failed", e);
				return -ErrorCodes.EIO();
			}
		});
		return returnOrTimeout(returnCode);
	}

//	@Override
//	public int write(String path, Pointer buf, long size, long offset, FuseFileInfo fi) {
//		return super.write(path, buf, size, offset, fi);
//	}

}
