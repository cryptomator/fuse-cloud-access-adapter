package org.cryptomator.fusecloudaccess;

import com.google.common.base.Preconditions;
import jnr.constants.platform.OpenFlags;
import jnr.ffi.Pointer;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFS;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CloudAccessFS extends FuseStubFS implements FuseFS {

	private static final Logger LOG = LoggerFactory.getLogger(CloudAccessFS.class);

	private final CloudProvider provider;
	private final int timeoutMillis;
	private final OpenFileFactory openFileFactory;
	private final OpenDirFactory openDirFactory;

	public CloudAccessFS(CloudProvider provider, int timeoutMillis) {
		this.provider = provider;
		this.timeoutMillis = timeoutMillis;
		this.openFileFactory = new OpenFileFactory(provider);
		this.openDirFactory = new OpenDirFactory(provider);
	}

	/**
	 * Method for async execution.
	 *
	 * @apiNote Only visible for testing.
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
			if (e.getCause() instanceof NoSuchFileException) {
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
			long dirHandle = openDirFactory.open(Path.of(path));
			fi.fh.set(dirHandle);
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
			if (e.getCause() instanceof NoSuchFileException) {
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

//	@Override
//	public int rename(String oldpath, String newpath) {
//		return super.rename(oldpath, newpath);
//	}
//
//	@Override
//	public int mkdir(String path, long mode) {
//		return super.mkdir(path, mode);
//	}
//
//	@Override
//	public int create(String path, long mode, FuseFileInfo fi) {
//		return super.create(path, mode, fi);
//	}
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
			if (e.getCause() instanceof NoSuchFileException) {
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
