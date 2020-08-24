package org.cryptomator.fusecloudaccess;

import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.ProgressListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

class OpenFileUploader {

	private static final Logger LOG = LoggerFactory.getLogger(OpenFileUploader.class);

	private final CloudProvider provider;
	private final ConcurrentMap<Long, CompletionStage<Void>> scheduledUploads = new ConcurrentHashMap<>();
	private final AtomicLong idGenerator = new AtomicLong();

	OpenFileUploader(CloudProvider provider) {
		this.provider = provider;
	}

	public CompletionStage<Void> scheduleUpload(OpenFile file) {
		if (!file.isDirty()) {
			LOG.trace("Upload of {} skipped. Unmodified.", file.getPath());
			return CompletableFuture.completedFuture(null);
		}
		try {
			var in = file.asPersistableStream();
			return scheduleUpload(file, in);
		} catch (IOException e) {
			LOG.error("Upload of " + file.getPath() + " failed.", e);
			return CompletableFuture.failedFuture(e);
		}
	}

	private CompletionStage<Void> scheduleUpload(OpenFile file, InputStream in) {
		assert file.isDirty();
		var path = file.getPath();
		LOG.debug("uploading {}...", path);
		long id = idGenerator.incrementAndGet();
		var task = provider.write(path, true, in, ProgressListener.NO_PROGRESS_AWARE)
				.thenRun(() -> {
					LOG.debug("uploaded successfully: {}", path);
					file.setDirty(false);
				}).exceptionally(e -> {
					LOG.error("Upload of " + path + " failed.", e);
					// TODO copy file to some lost+found dir
					return null;
				}).thenRun(() -> {
					scheduledUploads.remove(id);
				});
		scheduledUploads.put(id, task);
		return task;
	}

	public CompletionStage<Void> awaitPendingUploads() {
		return CompletableFuture.allOf(scheduledUploads.values().toArray(CompletableFuture[]::new));
	}

}
