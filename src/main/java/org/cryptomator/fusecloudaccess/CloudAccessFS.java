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
import org.cryptomator.cloudaccess.api.exceptions.QuotaNotAvailableException;
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

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.EnumSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

@FileSystemScoped
public class CloudAccessFS extends FuseStubFS implements FuseFS {

	private static final Logger LOG = LoggerFactory.getLogger(CloudAccessFS.class);
	private static final int BLOCKSIZE = 4096;

	private final CloudProvider provider;
	private final CloudAccessFSConfig config;
	private final ScheduledExecutorService scheduler;
	private final OpenFileUploader openFileUploader;
	private final OpenFileFactory openFileFactory;
	private final OpenDirFactory openDirFactory;
	private final LockManager lockManager;

	@Inject
	CloudAccessFS(CloudProvider provider, CloudAccessFSConfig config, ScheduledExecutorService scheduler, OpenFileUploader openFileUploader, OpenFileFactory openFileFactory, OpenDirFactory openDirFactory, LockManager lockManager) {
		this.provider = provider;
		this.config = config;
		this.scheduler = scheduler;
		this.openFileUploader = openFileUploader;
		this.openFileFactory = openFileFactory;
		this.openDirFactory = openDirFactory;
		this.lockManager = lockManager;
	}

	public static CloudAccessFS createNewFileSystem(CloudProvider provider) {
		return DaggerCloudAccessFSComponent.builder() //
				.cloudProvider(provider) //
				.build() //
				.filesystem();
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
			return returnCode.toCompletableFuture().get(config.getProviderResponseTimeoutSeconds(), TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			LOG.warn("async call interrupted");
			Thread.currentThread().interrupt();
			return -ErrorCodes.EINTR();
		} catch (ExecutionException e) {
			LOG.error("encountered unhandled exception", e.getCause());
			return -ErrorCodes.EIO();
		} catch (TimeoutException e) {
			LOG.error("operation timed out", e);
			return -ErrorCodes.ETIMEDOUT();
		}
	}

	@Override
	public Pointer init(Pointer conn) {
		//check upload dir on server
		var returnCode = returnOrTimeout(initInternal());
		if (returnCode != 0) {
			throw new IllegalStateException("Unable to create remote temporary upload dir.");
		}
		//check local cache dir
		try {
			Files.createDirectory(config.getCacheDir());
		} catch (FileAlreadyExistsException e) { // dis ok
			LOG.debug("init(): Local cache directory already exists.");
		} catch (IOException e) {
			LOG.error("init() failed: Unable to create local cache directory.");
			throw new IllegalStateException("Unable to create local cache dir.");
		}
		//check local lost and found dir
		if (!Files.exists(config.getLostAndFoundDir())) {
			LOG.error("init() failed: Local lost+found directory does not exist.");
			throw new IllegalStateException("Lost+Found dir does not exists.");
		}
		return null;
	}

	private CompletionStage<Integer> initInternal() {
		return provider.createFolderIfNonExisting(config.getUploadDir())
				.thenApply(ignored -> 0)
				.exceptionally(e -> {
					//LOG.error("init() failed: Unable to create/use tmp upload directory. Local changes won't be uploaded and always moved to " + config.getLostAndFoundDir() + ".", e);
					LOG.error("init() failed: Unable to create upload directory.");
					return -ErrorCodes.EIO();
				});
	}

	@Override
	public int statfs(String path, Statvfs stbuf) {
		long total = config.getTotalQuota();
		long avail = config.getAvailableQuota();

		try {
			var quota = provider.quota(CloudPath.of("/")).toCompletableFuture().join();
			avail = quota.getAvailableBytes();
			if (quota.getTotalBytes().isPresent()) {
				total = quota.getTotalBytes().get();
			} else if (quota.getUsedBytes().isPresent()) {
				total = quota.getAvailableBytes() + quota.getUsedBytes().get();
			} else {
				LOG.info("Quota used and total is not available, falling back to default for total available");
			}
		} catch (QuotaNotAvailableException e) {
			LOG.debug("Quota is not available, falling back to default");
		}

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
			var getattrCode = getattrInternal(CloudPath.of(path), stat);
			var returnCode = returnOrTimeout(getattrCode);
			LOG.trace("getattr {} (modified: {}.{}, size: {}) [{}]", path, stat.st_mtim.tv_sec, stat.st_mtim.tv_nsec, stat.st_size, returnCode);
			return returnCode;
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
	 *
	 * @param path
	 * @return
	 */
	private CompletionStage<CloudItemMetadata> getMetadataFromCacheOrCloud(CloudPath path) {
		return openFileFactory //
				.getCachedMetadata(path) //
				.<CompletionStage<CloudItemMetadata>>map(CompletableFuture::completedFuture) //
				.orElse(provider.itemMetadata(path));
	}

	@Override
	public int opendir(String path, FuseFileInfo fi) {
		try (PathLock pathLock = lockManager.createPathLock(path).forReading(); //
			 DataLock dataLock = pathLock.lockDataForReading()) {
			var opendirCode = opendirInternal(CloudPath.of(path), fi);
			var returnCode = returnOrTimeout(opendirCode);
			LOG.trace("opendir {} (handle: {}) [{}]", path, fi.fh.get(), returnCode);
			return returnCode;
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
						LOG.error("opendir() failed", e);
						return -ErrorCodes.EIO();
					}
				});
	}

	@Override
	public int readdir(String path, Pointer buf, FuseFillDir filler, long offset, FuseFileInfo fi) {
		try (PathLock pathLock = lockManager.createPathLock(path).forReading(); //
			 DataLock dataLock = pathLock.lockDataForReading()) {
			var readdirCode = readdirInternal(CloudPath.of(path), buf, filler, offset, fi);
			var returnCode = returnOrTimeout(readdirCode);
			LOG.trace("readdir {} (handle: {}, offset: {}) [{}]", path, fi.fh.get(), offset, returnCode);
			return returnCode;
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
			LOG.trace("releasedir {} (handle: {}) [0]", path, fi.fh.get());
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
			var openCode = openInternal(CloudPath.of(path), fi);
			var returnCode = returnOrTimeout(openCode);
			LOG.trace("open {} (handle: {}) [{}]", path, fi.fh.get(), returnCode);
			return returnCode;
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
		}).exceptionally(e -> {
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
			LOG.trace("release {} (handle: {}) [0]", path, fi.fh.get());
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
			var renameCode = renameInternal(CloudPath.of(oldpath), CloudPath.of(newpath));
			var returnCode = returnOrTimeout(renameCode);
			LOG.trace("rename {} to {} [{}]", oldpath, newpath, returnCode);
			return returnCode;
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
			var mkdirCode = mkdirInternal(CloudPath.of(path), mode);
			var returnCode = returnOrTimeout(mkdirCode);
			LOG.trace("mkdir {} (mode: {}) [{}]", path, mode, returnCode);
			return returnCode;
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
			var createCode = createInternal(CloudPath.of(path), mode, fi);
			var returnCode = returnOrTimeout(createCode);
			LOG.trace("create {} (handle: {}, mode: {}) [{}]", path, fi.fh.get(), mode, returnCode);
			return returnCode;
		} catch (Exception e) {
			LOG.error("create() failed", e);
			return -ErrorCodes.EIO();
		}
	}

	private CompletionStage<Integer> createInternal(CloudPath path, long mode, FuseFileInfo fi) {
		var modifiedDate = Instant.now().truncatedTo(ChronoUnit.SECONDS);
		return provider.write(path, false, InputStream.nullInputStream(), 0l, Optional.of(modifiedDate), ProgressListener.NO_PROGRESS_AWARE) //
				.handle((nullReturn, exception) -> {
					if (exception == null) {
						return createInternalNonExisting(path, mode, fi, modifiedDate);
					} else if (exception instanceof AlreadyExistsException) {
						return createInternalExisting(path, mode, fi);
					} else {
						return CompletableFuture.<Integer>failedFuture(exception);
					}
				}) //
				.thenCompose(Function.identity()) //
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

	private CompletionStage<Integer> createInternalNonExisting(CloudPath path, long mode, FuseFileInfo fi, Instant modifiedDate) {
		try {
			var size = 0;
			var handle = openFileFactory.open(path, BitMaskEnumUtil.bitMaskToSet(OpenFlags.class, fi.flags.longValue()), size, modifiedDate);
			fi.fh.set(handle);
			return CompletableFuture.completedFuture(0);
		} catch (IOException e) {
			return CompletableFuture.completedFuture(-ErrorCodes.EIO());
		}
	}

	private CompletionStage<Integer> createInternalExisting(CloudPath path, long mode, FuseFileInfo fi) {
		return provider.itemMetadata(path) //
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
				});
	}

	//This must be implemented, otherwise certain applications (e.g. TextEdit.app) fail to save text files.
	@Override
	public int chmod(String path, long mode) {
		LOG.trace("chmod {} (mode: {})", path, mode);
		return 0;
	}

	@Override
	public int rmdir(String path) {
		try (PathLock pathLock = lockManager.createPathLock(path.toString()).forWriting(); //
			 DataLock dataLock = pathLock.lockDataForWriting()) {
			var rmdirCode = rmdirInternal(CloudPath.of(path));
			var returnCode = returnOrTimeout(rmdirCode);
			LOG.trace("rmdir {} [{}]", path, returnCode);
			return returnCode;
		} catch (Exception e) {
			LOG.error("rmdir() failed", e);
			return -ErrorCodes.EIO();
		}
	}

	CompletionStage<Integer> rmdirInternal(CloudPath path) {
		openFileFactory.deleteDescendants(path);
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
	public int unlink(String path) {
		try (PathLock pathLock = lockManager.createPathLock(path.toString()).forWriting(); //
			 DataLock dataLock = pathLock.lockDataForWriting()) {
			var unlinkCode = unlinkInternal(CloudPath.of(path));
			var returnCode = returnOrTimeout(unlinkCode);
			LOG.trace("unlink {} [{}]", path, returnCode);
			return returnCode;
		} catch (Exception e) {
			LOG.error("unlink() failed", e);
			return -ErrorCodes.EIO();
		}
	}

	// visible for testing
	CompletionStage<Integer> unlinkInternal(CloudPath path) {
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
			var readCode = readInternal(fi.fh.get(), buf, size, offset);
			var returnCode = returnOrTimeout(readCode);
			LOG.trace("read {} (handle: {}, size: {}, offset: {}) [{}]", path, fi.fh.get(), size, offset, returnCode);
			return returnCode;
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
			var writeCode = writeInternal(fi.fh.get(), buf, size, offset);
			var returnCode = returnOrTimeout(writeCode);
			LOG.trace("write {} (handle: {}, size: {}, offset: {}) [{}]", path, fi.fh.get(), size, offset, returnCode);
			return returnCode;
		} catch (Exception e) {
			LOG.error("write() failed", e);
			return -ErrorCodes.EIO();
		}
	}

	private CompletableFuture<Integer> writeInternal(long fileHandle, Pointer buf, long size, long offset) {
		var openFile = openFileFactory.get(fileHandle);
		if (openFile.isEmpty()) {
			return CompletableFuture.completedFuture(-ErrorCodes.EBADF());
		}
		return openFile.get().write(buf, offset, size).exceptionally(e -> {
			if (e instanceof NotFoundException) {
				return -ErrorCodes.ENOENT();
			} else {
				LOG.error("write() failed", e);
				return -ErrorCodes.EIO();
			}
		});
	}

	@Override
	public int truncate(String path, long size) {
		try (PathLock pathLock = lockManager.createPathLock(path).forReading(); //
			 DataLock dataLock = pathLock.lockDataForWriting()) {
			truncateInternal(CloudPath.of(path), size);
			LOG.trace("truncate {} (size: {}) [0]", path, size);
			return 0;
		} catch (Exception e) {
			LOG.error("truncate() failed", e);
			return -ErrorCodes.EIO();
		}
	}

	private void truncateInternal(CloudPath path, long size) throws IOException {
		var fileHandle = openFileFactory.open(path, EnumSet.of(OpenFlags.O_WRONLY), size, Instant.now().truncatedTo(ChronoUnit.SECONDS));
		openFileFactory.get(fileHandle).get().truncate(size);
		openFileFactory.close(fileHandle);
	}

	@Override
	public int ftruncate(String path, long size, FuseFileInfo fi) {
		try (PathLock pathLock = lockManager.createPathLock(path).forReading(); //
			 DataLock dataLock = pathLock.lockDataForWriting()) {
			var returnCode = ftruncateInternal(fi.fh.get(), size);
			LOG.trace("ftruncate {} (handle: {}, size: {} [{}]", path, fi.fh.get(), size, returnCode);
			return returnCode;
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
					openFileUploader.awaitPendingUploads(config.getPendingUploadTimeoutSeconds(), TimeUnit.SECONDS);
					break;
				} catch (TimeoutException e) {
					LOG.debug("Still uploading...");
				}
			}
			scheduler.shutdown();
			LOG.debug("All done.");
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			LOG.error("Pending uploads interrupted.", e);
		}
	}

}
