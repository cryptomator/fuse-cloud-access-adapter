package org.cryptomator.fusecloudaccess;

import org.cryptomator.cloudaccess.api.CloudPath;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.ProgressListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Prepares and schedules upload of (possibly) changed files to the cloud.
 * <p>
 * It should be ensured, that the files which are currently processed are not modified by other thread or processes.
 */
class OpenFileUploader {

	private static final Logger LOG = LoggerFactory.getLogger(OpenFileUploader.class);

	private final CloudProvider provider;
	private final ConcurrentMap<CloudPath, Future<?>> tasks;
	private final Path cacheDir;
	private final ExecutorService executorService;

	public OpenFileUploader(CloudProvider provider, Path cacheDir) {
		this(provider, cacheDir, Executors.newSingleThreadExecutor(), new ConcurrentHashMap<>());
	}

	OpenFileUploader(CloudProvider provider, Path cacheDir, ExecutorService executorService, ConcurrentMap<CloudPath, Future<?>> tasks) {
		this.provider = provider;
		this.tasks = tasks;
		this.cacheDir = cacheDir;
		this.executorService = executorService;
	}

	/**
	 * Schedules {@link OpenFile} to be uploaded to the set {@link CloudProvider} by first creating a temporary copy of
	 * it and start the upload by reading from the copy.
	 *
	 * @param file      OpenFile object with reference to a real file
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
		var task = executorService.submit(new ScheduledUpload(provider, file, decoratedOnFinished, cacheDir));
		var previousTask = tasks.put(file.getPath(), task);
		if (previousTask != null) {
			previousTask.cancel(true);
		}
	}

	/**
	 * Cancels a pending upload (if any). No-op otherwise.
	 * @param path
	 * @return <code>true</code> if a pending upload has been canceled, <code>false</code> otherwise
	 */
	public boolean cancelUpload(CloudPath path) {
		LOG.trace("Cancel possible pending upload for {}", path);
		var task = tasks.get(path);
		if (task != null) {
			task.cancel(true);
			LOG.debug("Cancelled pending upload for {}", path);
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
		private final Path tmpDir;

		public ScheduledUpload(CloudProvider provider, OpenFile openFile, Consumer<OpenFile> onFinished, Path tmpDir) {
			this.provider = provider;
			this.openFile = openFile;
			this.onFinished = onFinished;
			this.tmpDir = tmpDir;
		}

		@Override
		public Void call() throws IOException {
			Path tmpFile = tmpDir.resolve(UUID.randomUUID() + ".tmp");
			try {
				openFile.persistTo(tmpFile).toCompletableFuture().get();
				assert Files.exists(tmpFile);
				var size = Files.size(tmpFile);
				try (var in = Files.newInputStream(tmpFile)) {
					provider.write(openFile.getPath(), true, in, size, ProgressListener.NO_PROGRESS_AWARE) //
							.toCompletableFuture() //
							.get();
				}
				return null;
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new InterruptedIOException("Upload interrupted.");
			} catch (ExecutionException e) {
				LOG.error("Upload of " + openFile.getPath() + " failed.", e);
				// TODO copy file to some lost+found dir
				throw new IOException("Upload failed.", e);
			} finally {
				Files.deleteIfExists(tmpFile);
				onFinished.accept(openFile);
			}
		}

	}

}
