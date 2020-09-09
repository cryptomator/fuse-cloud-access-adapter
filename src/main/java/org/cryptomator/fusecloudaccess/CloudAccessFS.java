package org.cryptomator.fusecloudaccess;

import jnr.constants.platform.OpenFlags;
import jnr.ffi.Pointer;
import org.cryptomator.cloudaccess.api.CloudItemMetadata;
import org.cryptomator.cloudaccess.api.CloudItemType;
import org.cryptomator.cloudaccess.api.CloudPath;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.ProgressListener;
import org.cryptomator.cloudaccess.api.exceptions.AlreadyExistsException;
import org.cryptomator.cloudaccess.api.exceptions.NotFoundException;
import org.cryptomator.cloudaccess.api.exceptions.TypeMismatchException;
import org.cryptomator.fusecloudaccess.locks.DataLock;
import org.cryptomator.fusecloudaccess.locks.LockManager;
import org.cryptomator.fusecloudaccess.locks.PathLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFS;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;
import ru.serce.jnrfuse.struct.Statvfs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Instant;
import java.util.EnumSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;

public class CloudAccessFS extends FuseStubFS implements FuseFS {

	private static final Logger LOG = LoggerFactory.getLogger(CloudAccessFS.class);
	private static final int BLOCKSIZE = 4096;

	private final CloudProvider provider;
	private final int timeoutMillis;
	private final OpenFileUploader openFileUploader;
	private final OpenFileFactory openFileFactory;
	private final OpenDirFactory openDirFactory;
	private final LockManager lockManager;

	// TODO: use DI instead of constructor madness
	// TODO: no, really. WE NEED DI!!!!
	public CloudAccessFS(CloudProvider provider, Path cacheDir, CloudPath cloudUploadDir, int timeoutMillis) {
		this(provider, cacheDir, cloudUploadDir, timeoutMillis, new StampedLock());
	}

	private CloudAccessFS(CloudProvider provider, Path cacheDir, CloudPath cloudUploadDir, int timeoutMillis, StampedLock moveLock) {
		this(provider, cacheDir, timeoutMillis, moveLock, new OpenFileUploader(provider, cacheDir, cloudUploadDir, moveLock));
	}

	private CloudAccessFS(CloudProvider provider, Path cacheDir, int timeoutMillis, StampedLock moveLock, OpenFileUploader openFileUploader) {
		this(provider, timeoutMillis, openFileUploader, new OpenFileFactory(provider, openFileUploader, cacheDir, moveLock), new OpenDirFactory(provider), new LockManager());
	}

	//Visible for testing
	CloudAccessFS(CloudProvider provider, int timeoutMillis, OpenFileUploader openFileUploader, OpenFileFactory openFileFactory, OpenDirFactory openDirFactory, LockManager lockManager) {
		this.provider = provider;
		this.timeoutMillis = timeoutMillis;
		this.openFileUploader = openFileUploader;
		this.openFileFactory = openFileFactory;
		this.openDirFactory = openDirFactory;
		this.lockManager = lockManager;
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
	public int statfs(String path, Statvfs stbuf) {
		long total = 1_000_000_000; // 1 GB TODO: get info from cloud or config
		long avail = 500_000_000; // 500 MB TODO: get info from cloud or config
		long tBlocks = total / BLOCKSIZE;
		long aBlocks = avail / BLOCKSIZE;
		stbuf.f_bsize.set(BLOCKSIZE);
		stbuf.f_frsize.set(BLOCKSIZE);
		stbuf.f_blocks.set(tBlocks);
		stbuf.f_bavail.set(aBlocks);
		stbuf.f_bfree.set(aBlocks);
		stbuf.f_namemax.set(146); // no shortening atm
		LOG.trace("statfs {} ({} / {})", path, avail, total);
		return 0;
	}

	@Override
	public int getattr(String path, FileStat stat) {
		try (PathLock pathLock = lockManager.createPathLock(path).forReading(); //
			 DataLock dataLock = pathLock.lockDataForReading()) {
			var returnCode = getattrInternal(CloudPath.of(path), stat);
			LOG.trace("getattr {} (modified: {}.{}, size: {})", path, stat.st_mtim.tv_sec, stat.st_mtim.tv_nsec, stat.st_size);
			return returnOrTimeout(returnCode);
		} catch (Exception e) {
			LOG.error("getattr() failed", e);
			return -ErrorCodes.EIO();
		}
	}

	private CompletionStage<Integer> getattrInternal(CloudPath path, FileStat stat) {
		return getMetadataFromCacheOrCloud(path) //
				.thenApply(metadata -> {
					Attributes.copy(metadata, stat);
					return 0;
				}) //
				.exceptionally(e -> {
					if (e instanceof NotFoundException) {
						return -ErrorCodes.ENOENT();
					} else {
						LOG.error("getattr() failed", e);
						return -ErrorCodes.EIO();
					}
				});
	}

	/**
	 * Reads metadata. Prefers locally cached metadata and fetches metadata from the cloud as a fallback.
 	 * @param path
	 * @return
	 */
	private CompletionStage<CloudItemMetadata> getMetadataFromCacheOrCloud(CloudPath path) {
		return openFileFactory
				.getCachedMetadata(path)
				.<CompletionStage<CloudItemMetadata>>map(CompletableFuture::completedFuture)
				.orElse(provider.itemMetadata(path));
	}

	@Override
	public int opendir(String path, FuseFileInfo fi) {
		try (PathLock pathLock = lockManager.createPathLock(path).forReading(); //
			 DataLock dataLock = pathLock.lockDataForReading()) {
			var returnCode = opendirInternal(CloudPath.of(path), fi);
			LOG.trace("opendir {} (handle: {})", path, fi.fh.get());
			return returnOrTimeout(returnCode);
		} catch (Exception e) {
			LOG.error("opendir() failed", e);
			return -ErrorCodes.EIO();
		}
	}

	private CompletionStage<Integer> opendirInternal(CloudPath path, FuseFileInfo fi) {
		return getMetadataFromCacheOrCloud(path) //
				.thenApply(metadata -> {
					if (metadata.getItemType() == CloudItemType.FOLDER) {
						long dirHandle = openDirFactory.open(path);
						fi.fh.set(dirHandle);
						return 0;
					} else {
						return -ErrorCodes.ENOTDIR();
					}
				}) //
				.exceptionally(e -> {
					if (e instanceof NotFoundException) {
						return -ErrorCodes.ENOENT();
					} else {
						LOG.error("open() failed", e);
						return -ErrorCodes.EIO();
					}
				});
	}

	@Override
	public int readdir(String path, Pointer buf, FuseFillDir filler, long offset, FuseFileInfo fi) {
		try (PathLock pathLock = lockManager.createPathLock(path).forReading(); //
			 DataLock dataLock = pathLock.lockDataForReading()) {
			var returnCode = readdirInternal(CloudPath.of(path), buf, filler, offset, fi);
			LOG.trace("readdir {} (handle: {}, offset: {})", path, fi.fh.get(), offset);
			return returnOrTimeout(returnCode);
		} catch (Exception e) {
			LOG.error("readdir() failed", e);
			return -ErrorCodes.EIO();
		}
	}

	private CompletionStage<Integer> readdirInternal(CloudPath path, Pointer buf, FuseFillDir filler, long offset, FuseFileInfo fi) {
		if (offset > Integer.MAX_VALUE) {
			LOG.error("readdir() only supported for up to 2^31 entries, but attempted to read from offset {}", offset);
			return CompletableFuture.completedFuture(-ErrorCodes.EOVERFLOW());
		}

		var openDir = openDirFactory.get(fi.fh.get());
		if (openDir.isEmpty()) {
			return CompletableFuture.completedFuture(-ErrorCodes.EBADF());
		}

		return openDir.get().list(buf, filler, (int) offset).exceptionally(e -> {
			if (e instanceof NotFoundException) {
				return -ErrorCodes.ENOENT();
			} else if (e instanceof TypeMismatchException) {
				return -ErrorCodes.ENOTDIR();
			} else {
				LOG.error("readdir() failed", e);
				return -ErrorCodes.EIO();
			}
		});
	}

	@Override
	public int releasedir(String path, FuseFileInfo fi) {
		try (PathLock pathLock = lockManager.createPathLock(path).forReading(); //
			 DataLock dataLock = pathLock.lockDataForReading()) {
			openDirFactory.close(fi.fh.get());
			LOG.trace("releasedir {} (handle: {})", path, fi.fh.get());
			return 0;
		} catch (Exception e) {
			LOG.error("releasedir() failed", e);
			return -ErrorCodes.EIO();
		}
	}

	@Override
	public int open(String path, FuseFileInfo fi) {
		try (PathLock pathLock = lockManager.createPathLock(path).forReading(); //
			 DataLock dataLock = pathLock.lockDataForReading()) {
			var returnCode = openInternal(CloudPath.of(path), fi);
			LOG.trace("open {} (handle: {})", path, fi.fh.get());
			return returnOrTimeout(returnCode);
		} catch (Exception e) {
			LOG.error("open() failed", e);
			return -ErrorCodes.EIO();
		}
	}

	private CompletionStage<Integer> openInternal(CloudPath path, FuseFileInfo fi) {
		return getMetadataFromCacheOrCloud(path).thenApply(metadata -> {
					final var type = metadata.getItemType();
					if (type == CloudItemType.FILE) {
						try {
							var size = metadata.getSize().orElse(0l);
							var lastModified = metadata.getLastModifiedDate().orElse(Instant.EPOCH);
							var handle = openFileFactory.open(path, BitMaskEnumUtil.bitMaskToSet(OpenFlags.class, fi.flags.longValue()), size, lastModified);
							fi.fh.set(handle);
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
				}) //
				.exceptionally(e -> {
					if (e instanceof NotFoundException) {
						return -ErrorCodes.ENOENT();
					} else {
						LOG.error("open() failed", e);
						return -ErrorCodes.EIO();
					}
				});
	}

	@Override
	public int release(String path, FuseFileInfo fi) {
		try (PathLock pathLock = lockManager.createPathLock(path).forReading(); //
			 DataLock dataLock = pathLock.lockDataForReading()) {
			openFileFactory.close(fi.fh.get());
			LOG.trace("release {} (handle: {})", path, fi.fh.get());
			return 0;
		} catch (Exception e) {
			LOG.error("release() failed", e);
			return -ErrorCodes.EIO();
		}
	}

	@Override
	public int rename(String oldpath, String newpath) {
		try (PathLock oldPathLock = lockManager.createPathLock(oldpath).forWriting(); //
			 DataLock oldDataLock = oldPathLock.lockDataForWriting(); //
			 PathLock newPathLock = lockManager.createPathLock(newpath).forWriting(); //
			 DataLock newDataLock = newPathLock.lockDataForWriting()) {
			var returnCode = renameInternal(CloudPath.of(oldpath), CloudPath.of(newpath));
			LOG.trace("rename {} to {}", oldpath, newpath);
			return returnOrTimeout(returnCode);
		} catch (Exception e) {
			LOG.error("rename() failed", e);
			return -ErrorCodes.EIO();
		}
	}

	private CompletionStage<Integer> renameInternal(CloudPath oldPath, CloudPath newPath) {
		openFileFactory.move(oldPath, newPath);
		return provider.move(oldPath, newPath, true) //
				.thenApply(ignored -> 0) //
				.exceptionally(e -> {
					if (e instanceof NotFoundException) {
						return -ErrorCodes.ENOENT();
					} else if (e instanceof AlreadyExistsException) {
						return -ErrorCodes.EEXIST();
					} else {
						LOG.error("rename() failed", e);
						return -ErrorCodes.EIO();
					}
				});
	}

	@Override
	public int mkdir(String path, long mode) {
		try (PathLock pathLock = lockManager.createPathLock(path).forWriting(); //
			 DataLock dataLock = pathLock.lockDataForWriting()) {
			var returnCode = mkdirInternal(CloudPath.of(path), mode);
			LOG.trace("mkdir {} (mode: {})", path, mode);
			return returnOrTimeout(returnCode);
		} catch (Exception e) {
			LOG.error("mkdir() failed", e);
			return -ErrorCodes.EIO();
		}
	}

	private CompletionStage<Integer> mkdirInternal(CloudPath path, long mode) {
		return provider.createFolder(path) //
				.thenApply(p -> 0) //
				.exceptionally(e -> {
					if (e instanceof AlreadyExistsException) {
						return -ErrorCodes.EEXIST();
					} else {
						LOG.error("mkdir() failed", e);
						return -ErrorCodes.EIO();
					}
				});
	}

	@Override
	public int create(String path, long mode, FuseFileInfo fi) {
		try (PathLock pathLock = lockManager.createPathLock(path).forWriting(); //
			 DataLock dataLock = pathLock.lockDataForWriting()) {
			var returnCode = createInternal(CloudPath.of(path), mode, fi);
			LOG.trace("create {} (handle: {}, mode: {})", path, fi.fh.get(), mode);
			return returnOrTimeout(returnCode);
		} catch (Exception e) {
			LOG.error("create() failed", e);
			return -ErrorCodes.EIO();
		}
	}

	private CompletionStage<Integer> createInternal(CloudPath path, long mode, FuseFileInfo fi) {
		return provider.write(path, false, InputStream.nullInputStream(), 0l, ProgressListener.NO_PROGRESS_AWARE) //
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
				}) //
				.thenCompose(Function.identity()) //
				.thenApply(metadata -> {
					try {
						var size = metadata.getSize().orElse(0l);
						var lastModified = metadata.getLastModifiedDate().orElse(Instant.EPOCH);
						var handle = openFileFactory.open(path, BitMaskEnumUtil.bitMaskToSet(OpenFlags.class, fi.flags.longValue()), size, lastModified);
						fi.fh.set(handle);
						return 0;
					} catch (IOException e) {
						return -ErrorCodes.EIO();
					}
				}) //
				.exceptionally(e -> {
					if (e instanceof NotFoundException) {
						return -ErrorCodes.ENOENT();
					} else if (e instanceof TypeMismatchException) {
						return -ErrorCodes.EISDIR();
					} else {
						LOG.error("create() failed", e);
						return -ErrorCodes.EIO();
					}
				});
	}

	@Override
	public int chmod(String path, long mode) {
		// TODO: This must be implemented! Otherwise TextEdit.app fails to save text files.
		LOG.trace("chmod {} (mode: {})", path, mode);
		return 0;
	}

	@Override
	public int rmdir(String path) {
		try (PathLock pathLock = lockManager.createPathLock(path.toString()).forWriting(); //
			 DataLock dataLock = pathLock.lockDataForWriting()) {
			var returnCode = deleteInternal(CloudPath.of(path));
			LOG.trace("rmdir {}", path);
			return returnOrTimeout(returnCode);
		} catch (Exception e) {
			LOG.error("rmdir() failed", e);
			return -ErrorCodes.EIO();
		}
	}

	@Override
	public int unlink(String path) {
		try (PathLock pathLock = lockManager.createPathLock(path.toString()).forWriting(); //
			 DataLock dataLock = pathLock.lockDataForWriting()) {
			var returnCode = deleteInternal(CloudPath.of(path));
			LOG.trace("unlink {}", path);
			return returnOrTimeout(returnCode);
		} catch (Exception e) {
			LOG.error("unlink() failed", e);
			return -ErrorCodes.EIO();
		}
	}

	// visible for testing
	CompletionStage<Integer> deleteInternal(CloudPath path) {
		openFileFactory.delete(path);
		return provider.delete(path) //
				.thenApply(ignored -> 0) //
				.exceptionally(e -> {
					if (e instanceof NotFoundException) {
						return -ErrorCodes.ENOENT();
					} else {
						LOG.error("delete() failed", e);
						return -ErrorCodes.EIO();
					}
				});
	}

	@Override
	public int read(String path, Pointer buf, long size, long offset, FuseFileInfo fi) {
		try (PathLock pathLock = lockManager.createPathLock(path).forReading(); //
			 DataLock dataLock = pathLock.lockDataForReading()) {
			var returnCode = readInternal(fi.fh.get(), buf, size, offset);
			LOG.trace("read {} (handle: {}, size: {}, offset: {})", path, fi.fh.get(), size, offset);
			return returnOrTimeout(returnCode);
		} catch (Exception e) {
			LOG.error("read() failed", e);
			return -ErrorCodes.EIO();
		}
	}

	private CompletionStage<Integer> readInternal(long fileHandle, Pointer buf, long size, long offset) {
		var openFile = openFileFactory.get(fileHandle);
		if (openFile.isEmpty()) {
			return CompletableFuture.completedFuture(-ErrorCodes.EBADF());
		}
		return openFile.get().read(buf, offset, size).exceptionally(e -> {
			if (e instanceof NotFoundException) {
				return -ErrorCodes.ENOENT();
			} else {
				LOG.error("read() failed", e);
				return -ErrorCodes.EIO();
			}
		});
	}

	@Override
	public int write(String path, Pointer buf, long size, long offset, FuseFileInfo fi) {
		try (PathLock pathLock = lockManager.createPathLock(path).forReading(); //
			 DataLock dataLock = pathLock.lockDataForWriting()) {
			LOG.trace("write {} (handle: {}, size: {}, offset: {})", path, fi.fh.get(), size, offset);
			return writeInternal(fi.fh.get(), buf, size, offset);
		} catch (Exception e) {
			LOG.error("write() failed", e);
			return -ErrorCodes.EIO();
		}
	}

	private int writeInternal(long fileHandle, Pointer buf, long size, long offset) throws IOException {
		var openFile = openFileFactory.get(fileHandle);
		if (openFile.isEmpty()) {
			return -ErrorCodes.EBADF();
		}
		return openFile.get().write(buf, offset, size);
	}

	@Override
	public int truncate(String path, long size) {
		try (PathLock pathLock = lockManager.createPathLock(path).forReading(); //
			 DataLock dataLock = pathLock.lockDataForWriting()) {
			LOG.trace("truncate {} (size: {})", path, size);
			truncateInternal(CloudPath.of(path), size);
			return 0;
		} catch (IOException e) {
			LOG.error("truncate() failed", e);
			return -ErrorCodes.EIO();
		}
	}

	private void truncateInternal(CloudPath path, long size) throws IOException {
		var fileHandle = openFileFactory.open(path, EnumSet.of(OpenFlags.O_WRONLY), size, Instant.now());
		openFileFactory.get(fileHandle).get().truncate(size);
		openFileFactory.close(fileHandle);
	}

	@Override
	public int ftruncate(String path, long size, FuseFileInfo fi) {
		try (PathLock pathLock = lockManager.createPathLock(path).forReading(); //
			 DataLock dataLock = pathLock.lockDataForWriting()) {
			LOG.trace("ftruncate {} (handle: {}, size: {}", path, fi.fh.get(), size);
			return ftruncateInternal(fi.fh.get(), size);
		} catch (Exception e) {
			LOG.error("ftruncate() failed", e);
			return -ErrorCodes.EIO();
		}
	}

	private int ftruncateInternal(long fileHandle, long size) throws IOException {
		var handle = openFileFactory.get(fileHandle);
		if (handle.isEmpty()) {
			return -ErrorCodes.EBADF();
		}
		handle.get().truncate(size);
		return 0;
	}

	@Override
	public void destroy(Pointer initResult) {
		LOG.debug("Waiting for pending uploads...");
		try {
			while (true) {
				try {
					openFileUploader.awaitPendingUploads(30, TimeUnit.SECONDS); // TODO make configurable
					break;
				} catch (TimeoutException e) {
					LOG.info("Still uploading...");
				}
			}
			LOG.debug("All done.");
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			LOG.error("Pending uploads interrupted.", e);
		}
	}

}
