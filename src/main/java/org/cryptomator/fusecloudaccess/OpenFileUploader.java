package org.cryptomator.fusecloudaccess;

import com.google.common.io.Closeables;
import org.cryptomator.cloudaccess.api.CloudPath;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.ProgressListener;
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
import java.util.concurrent.locks.StampedLock;
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
	private final StampedLock moveLock;

	@Inject
	OpenFileUploader(CloudProvider provider, Path cacheDir, CloudPath cloudUploadDir, ExecutorService executorService, @Named("uploadTasks") ConcurrentMap<CloudPath, Future<?>> tasks, StampedLock moveLock) {
		this.provider = provider;
		this.cacheDir = cacheDir;
		this.cloudUploadDir = cloudUploadDir;
		this.executorService = executorService;
		this.tasks = tasks;
		this.moveLock = moveLock;
	}

	/**
	 * Schedules {@link OpenFile} to be uploaded to the set {@link CloudProvider} by first creating a temporary copy of
	 * it and start the upload by reading from the copy.
	 *
	 * @param file       OpenFile object with reference to a real file
	 * @param onFinished Callback invoked after successful upload
	 */
	public void scheduleUpload(OpenFile file, Consumer<OpenFile> onFinished) {
		if (!file.isDirty()) {
			LOG.trace("Upload of {} skipped. Unmodified.", file.getPath());
			onFinished.accept(file);
			return; // no-op
		}
		Consumer<OpenFile> decoratedOnFinished = f -> {
			tasks.remove(f.getPath());
			onFinished.accept(f);
		};
		var task = executorService.submit(new ScheduledUpload(provider, file, decoratedOnFinished, cacheDir, cloudUploadDir, moveLock));
		var previousTask = tasks.put(file.getPath(), task);
		if (previousTask != null) {
			previousTask.cancel(true);
		}
	}

	/**
	 * Cancels a pending upload (if any). No-op otherwise.
	 *
	 * @param path
	 * @return <code>true</code> if a pending upload has been canceled, <code>false</code> otherwise
	 */
	public boolean cancelUpload(CloudPath path) {
		LOG.trace("Cancel possible upload for {}", path);
		var task = tasks.get(path);
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
		private final StampedLock moveLock;

		public ScheduledUpload(CloudProvider provider, OpenFile openFile, Consumer<OpenFile> onFinished, Path cacheDir, CloudPath cloudUploadDir, StampedLock moveLock) {
			this.provider = provider;
			this.openFile = openFile;
			this.onFinished = onFinished;
			this.cacheDir = cacheDir;
			this.cloudUploadDir = cloudUploadDir;
			this.moveLock = moveLock;
		}

		@Override
		public Void call() throws IOException {
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
								var upload = provider.write(cloudTmpFile, true, in, size, Optional.of(lastModified), ProgressListener.NO_PROGRESS_AWARE);
								return CompletionUtils.runAlways(upload, () -> Closeables.closeQuietly(in));
							} catch (IOException e) {
								return CompletableFuture.failedFuture(e);
							}
						})
						.thenCompose(ignored -> {
							return CompletionUtils.runLocked(moveLock, () -> {
								return provider.move(cloudTmpFile, openFile.getPath(), true);
							});
						})
						.toCompletableFuture().get();
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
