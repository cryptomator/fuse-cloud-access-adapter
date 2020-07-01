package org.cryptomator.fusecloudaccess;

import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import jnr.ffi.Pointer;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.ProgressListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFS;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

public class CloudAccessFS extends FuseStubFS implements FuseFS {

	private static final Logger LOG = LoggerFactory.getLogger(CloudAccessFS.class);

	private final CloudProvider provider;
	private final int timeoutMillis;

	public CloudAccessFS(CloudProvider provider, int timeoutMillis) {
		this.provider = provider;
		this.timeoutMillis = timeoutMillis;
	}

	@Override
	public int getattr(String path, FileStat stat) {
		try {
			var metadata = provider.itemMetadata(Path.of(path)).toCompletableFuture().get(timeoutMillis, TimeUnit.MILLISECONDS);
			Attributes.copy(metadata, stat);
			return 0;
		} catch (InterruptedException e) {
			LOG.warn("getattr() interrupted");
			Thread.currentThread().interrupt();
			return -ErrorCodes.EINTR();
		} catch (ExecutionException e) {
			LOG.error("getattr()", e.getCause());
			return -ErrorCodes.EIO();
		} catch (TimeoutException e) {
			return -ErrorCodes.ETIMEDOUT();
		}
	}

	@Override
	public int readdir(String path, Pointer buf, FuseFillDir filler, long offset, FuseFileInfo fi) {
		try {
			// TODO paginated listing (keep track of `open`ed directories and store page tockens?):
			var itemList = provider.listExhaustively(Path.of(path)).toCompletableFuture().get(timeoutMillis, TimeUnit.MILLISECONDS);
			// TODO check filler return code:
			filler.apply(buf, ".", null, 0);
			filler.apply(buf, "..", null, 0);
			for (var item : itemList.getItems()) {
				if (filler.apply(buf, item.getName(), null, 0) != 0) {
					return -ErrorCodes.ENOMEM();
				}
			}
			return 0;
		} catch (InterruptedException e) {
			LOG.warn("readdir() interrupted");
			Thread.currentThread().interrupt();
			return -ErrorCodes.EINTR();
		} catch (ExecutionException e) {
			LOG.error("readdir()", e.getCause());
			return -ErrorCodes.EIO();
		} catch (TimeoutException e) {
			return -ErrorCodes.ETIMEDOUT();
		}
	}

	@Override
	public int open(String path, FuseFileInfo fi) {
		// TODO
		return 0;
	}

//	@Override
//	public int release(String path, FuseFileInfo fi) {
//		return super.release(path, fi);
//	}
//
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
		// TODO we need partial read
		// provider.read(Path.of(path), ProgressListener.NO_PROGRESS_AWARE)....
		return super.read(path, buf, size, offset, fi);
	}

//	@Override
//	public int write(String path, Pointer buf, long size, long offset, FuseFileInfo fi) {
//		return super.write(path, buf, size, offset, fi);
//	}

}
