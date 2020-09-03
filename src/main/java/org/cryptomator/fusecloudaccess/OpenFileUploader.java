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

/**
 * Prepares and schedules upload of (possibly) changed files to the cloud.
 * <p>
 * It should be ensured, that the files which are currently processed are not modified by other thread or processes.
 */
class OpenFileUploader {

	private static final Logger LOG = LoggerFactory.getLogger(OpenFileUploader.class);

	private final CloudProvider provider;
	private final ConcurrentMap<CloudPath, CompletionStage<CloudPath>> scheduledUploads = new ConcurrentHashMap<>();
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
	public CompletionStage<CloudPath> scheduleUpload(OpenFile file) {
		if (!file.isDirty()) {
			LOG.trace("Upload of {} skipped. Unmodified.", file.getPath());
			return CompletableFuture.completedFuture(null);
		}
		try {
			//TODO: cancel previous uploads
			Path toUpload = cacheDir.resolve(UUID.randomUUID() + ".tmp");
			file.persistTo(toUpload);
			file.setDirty(false);
			var size = Files.size(toUpload);
			var in = Files.newInputStream(toUpload, StandardOpenOption.DELETE_ON_CLOSE);
			return scheduleUpload(file, in, size);
		} catch (IOException e) {
			LOG.error("Upload of " + file.getPath() + " failed.", e);
			return CompletableFuture.failedFuture(e);
		}
	}

	private CompletionStage<CloudPath> scheduleUpload(OpenFile file, InputStream in, long size) {
		var path = file.getPath();
		LOG.debug("uploading {}...", path);
		var task = provider.write(path, true, in, size, ProgressListener.NO_PROGRESS_AWARE)
				.thenRun(() -> LOG.debug("uploaded successfully: {}", path))
				.exceptionally(e -> {
					LOG.error("Upload of " + path + " failed.", e);
					// TODO copy file to some lost+found dir
					file.setDirty(true); // might cause an additional upload
					return null;
				}).thenApply(ignored -> {
					scheduledUploads.remove(path); //TODO: document it (behaviour)
					try {
						in.close();
					} catch (IOException e) {
						LOG.error("Unable to close channel to temporary file, will be closed on program exit.", e);
					}
					return path;
				});
		scheduledUploads.put(path, task);
		return task;
	}

	//TODO: cancel does not close the input stream of the canceled job. CHANGE THIS FOR GODS SAKE!
	public synchronized void cancel(CloudPath path) {
		LOG.trace("Cancel possible pending upload for {}", path);
		boolean result = scheduledUploads.get(path).toCompletableFuture().cancel(true); //TODO: testen, testen, testen
		if (!result) {
			LOG.debug("Upload for {} could not be canceled.", path);
		}
	}

	public CompletionStage<Void> awaitPendingUploads() {
		return CompletableFuture.allOf(scheduledUploads.values().toArray(CompletableFuture[]::new));
	}

}
