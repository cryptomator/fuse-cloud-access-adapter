package org.cryptomator.fusecloudaccess;

import org.cryptomator.cloudaccess.api.CloudPath;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.ProgressListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Prepares and schedules upload of (possibly) changed files to the cloud.
 * <p>
 * It should be ensured, that the files which are currently processed are not modified by other thread or processes.
 */
class OpenFileUploader {

	private static final Logger LOG = LoggerFactory.getLogger(OpenFileUploader.class);

	private final CloudProvider provider;
	private final ConcurrentMap<Long, CompletionStage<Void>> scheduledUploads = new ConcurrentHashMap<>();
	private final AtomicLong idGenerator = new AtomicLong();
	private final Path cacheDir;

	OpenFileUploader(CloudProvider provider, Path cacheDir) {
		this.provider = provider;
		this.cacheDir = cacheDir;
	}

	/**
	 * Schedules {@link OpenFile} to be uploaded to the set {@link CloudProvider} by first creating a temporary copy of
	 * it and start the upload by reading from the copy.
	 * <p>
	 * This method relies on that the underlying files in the file system do not change during the complete copy.
	 * Generally it is not considered harmful, if a file is during upload replaced by a new version, since this version is also scheduled for an upload.
	 *
	 * @param file OpenFile object with reference to a real file.
	 * @return A void {@link CompletionStage} indicating when the upload operation is successful completed.
	 */
	public CompletionStage<Void> scheduleUpload(OpenFile file) {
		if (!file.isDirty()) {
			LOG.trace("Upload of {} skipped. Unmodified.", file.getPath());
			return CompletableFuture.completedFuture(null);
		}
		try {
			Path toUpload = cacheDir.resolve(UUID.randomUUID() + ".tmp");
			Files.copy(file.asPersistableStream(), toUpload);
			file.setDirty(false);
			return scheduleUpload(file.getPath(), Files.newInputStream(toUpload, StandardOpenOption.DELETE_ON_CLOSE));
		} catch (IOException e) {
			LOG.error("Upload of " + file.getPath() + " failed.", e);
			return CompletableFuture.failedFuture(e);
		}
	}

	private CompletionStage<Void> scheduleUpload(CloudPath path, InputStream in) {
		LOG.debug("uploading {}...", path);
		long id = idGenerator.incrementAndGet();
		var task = provider.write(path, true, in, ProgressListener.NO_PROGRESS_AWARE)
				.thenRun(() -> {
					LOG.debug("uploaded successfully: {}", path);
				}).exceptionally(e -> {
					LOG.error("Upload of " + path + " failed.", e);
					// TODO copy file to some lost+found dir
					return null;
				}).thenRun(() -> {
					scheduledUploads.remove(id);
					try {
						in.close();
					} catch (IOException e) {
						LOG.error("Unable to close channel to temporary file, will be closed on program exit.", e);
					}
				});
		scheduledUploads.put(id, task);
		return task;
	}

	public CompletionStage<Void> awaitPendingUploads() {
		return CompletableFuture.allOf(scheduledUploads.values().toArray(CompletableFuture[]::new));
	}

}
