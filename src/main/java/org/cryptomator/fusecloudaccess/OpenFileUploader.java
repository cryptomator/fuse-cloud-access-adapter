package org.cryptomator.fusecloudaccess;

import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import org.cryptomator.cloudaccess.api.CloudPath;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.ProgressListener;
import org.cryptomator.fusecloudaccess.locks.LockManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Prepares and schedules upload of (possibly) changed files to the cloud.
 * <p>
 * It should be ensured, that the files which are currently processed are not modified by other thread or processes.
 */
@FileSystemScoped
class OpenFileUploader {

	private static final Logger LOG = LoggerFactory.getLogger(OpenFileUploader.class);

	private final CloudProvider provider;
	private final Path cacheDir;
	private final CloudPath cloudUploadDir;
	private final ExecutorService executorService;
	private final ConcurrentMap<CloudPath, Future<?>> tasks;
	private final LockManager lockManager;

	@Inject
	OpenFileUploader(CloudProvider provider, Path cacheDir, CloudPath cloudUploadDir, ExecutorService executorService, @Named("uploadTasks") ConcurrentMap<CloudPath, Future<?>> tasks, LockManager lockManager) {
		this.provider = provider;
		this.cacheDir = cacheDir;
		this.cloudUploadDir = cloudUploadDir;
		this.executorService = executorService;
		this.tasks = tasks;
		this.lockManager = lockManager;
	}

	/**
	 * Schedules {@link OpenFile} to be uploaded to the set {@link CloudProvider} by first creating a temporary copy of
	 * it and start the upload by reading from the copy.
	 *
	 * @param file       OpenFile object with reference to a real file
	 * @param onFinished Callback invoked after successful upload
	 */
	public void scheduleUpload(OpenFile file, Consumer<OpenFile> onFinished) {
		Preconditions.checkState(file.getState() == OpenFile.State.UPLOADING, "File not marked as UPLOADING");
		LOG.debug("starting upload {} {}", file.getPath(), file);
		Consumer<OpenFile> decoratedOnFinished = f -> {
			tasks.remove(f.getPath());
			if (f.transitionToUnmodified()) {
				onFinished.accept(f);
			} else if (f.transitionToReuploading()) {
				scheduleUpload(file, onFinished);
			}
		};
		var task = executorService.submit(new ScheduledUpload(provider, file, decoratedOnFinished, cacheDir, cloudUploadDir, lockManager));
		var previousTask = tasks.put(file.getPath(), task);
		assert previousTask == null : "Must not schedule new upload before finishing previous one";
	}

	/**
	 * Cancels a pending upload (if any). No-op otherwise.
	 *
	 * @param path
	 * @return <code>true</code> if a pending upload has been canceled, <code>false</code> otherwise
	 */
	public boolean cancelUpload(CloudPath path) {
		LOG.trace("Cancel possible upload for {}", path);
		var task = tasks.remove(path);
		if (task != null) {
			task.cancel(true);
			LOG.debug("Cancelled upload for {}", path);
			return true;
		} else {
			return false;
		}
	}

	public void awaitPendingUploads(long timeout, TimeUnit timeUnit) throws InterruptedException, TimeoutException {
		executorService.shutdown();
		if (!executorService.awaitTermination(timeout, timeUnit)) {
			throw new TimeoutException("Uploads still running.");
		}
	}

	static class ScheduledUpload implements Callable<Void> {

		private final CloudProvider provider;
		private final OpenFile openFile;
		private final Consumer<OpenFile> onFinished;
		private final Path cacheDir;
		private final CloudPath cloudUploadDir;
		private final LockManager lockManager;

		public ScheduledUpload(CloudProvider provider, OpenFile openFile, Consumer<OpenFile> onFinished, Path cacheDir, CloudPath cloudUploadDir, LockManager lockManager) {
			this.provider = provider;
			this.openFile = openFile;
			this.onFinished = onFinished;
			this.cacheDir = cacheDir;
			this.cloudUploadDir = cloudUploadDir;
			this.lockManager = lockManager;
		}

		@Override
		public Void call() throws IOException {
			assert openFile.getState() == OpenFile.State.UPLOADING;
			String tmpFileName = UUID.randomUUID() + ".tmp";
			Path localTmpFile = cacheDir.resolve(tmpFileName);
			CloudPath cloudTmpFile = cloudUploadDir.resolve(tmpFileName);
			try {
				openFile.persistTo(localTmpFile)
						.thenCompose((ignored) -> {
							assert Files.exists(localTmpFile);
							try {
								var size = openFile.getSize();
								var lastModified = openFile.getLastModified();
								var in = Files.newInputStream(localTmpFile);
								var uploadTask = provider.write(cloudTmpFile, true, in, size, Optional.of(lastModified), ProgressListener.NO_PROGRESS_AWARE);
								return uploadTask.whenComplete((result, exception) -> Closeables.closeQuietly(in));
							} catch (IOException e) {
								return CompletableFuture.failedFuture(e);
							}
						})
						.toCompletableFuture().get();
				// since this is async code, we need a new path lock for this move:
				try (var lock = lockManager.createPathLock(openFile.getPath().toString()).forWriting()) {
					LOG.debug("Finishing upload of {} to {}", openFile, openFile.getPath());
					provider.move(cloudTmpFile, openFile.getPath(), true).toCompletableFuture().get();
				}
				return null;
			} catch (CancellationException e) {    //OK
				LOG.debug("Canceled upload for {}.", openFile.getPath());
				return null;
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new InterruptedIOException("Upload interrupted.");
			} catch (ExecutionException e) {
				LOG.error("Upload of " + openFile.getPath() + " failed.", e);
				// TODO copy file to some lost+found dir
				throw new IOException("Upload failed.", e);
			} finally {
				Files.deleteIfExists(localTmpFile);
				onFinished.accept(openFile);
			}
		}

	}

}
