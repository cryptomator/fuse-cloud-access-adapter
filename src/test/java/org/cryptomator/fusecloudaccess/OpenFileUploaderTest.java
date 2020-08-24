package org.cryptomator.fusecloudaccess;

import org.cryptomator.cloudaccess.api.CloudPath;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class OpenFileUploaderTest {

	private CloudProvider provider;
	private OpenFileUploader uploader;
	private OpenFile file;
	private CloudPath path;

	@BeforeEach
	public void setup() {
		this.provider = Mockito.mock(CloudProvider.class);
		this.uploader = new OpenFileUploader(provider);
		this.file = Mockito.mock(OpenFile.class);
		this.path = Mockito.mock(CloudPath.class, "/path/in/cloud");
		Mockito.when(file.getPath()).thenReturn(path);
	}

	@Test
	@DisplayName("noop when scheduling unmodified file")
	public void testUploadUnmodified() {
		Mockito.when(file.isDirty()).thenReturn(false);

		uploader.scheduleUpload(file);

		Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> uploader.awaitPendingUploads().toCompletableFuture().get());
		Mockito.verify(provider, Mockito.never()).write(Mockito.any(), Mockito.anyBoolean(), Mockito.any(), Mockito.any());
	}

	@Test
	@DisplayName("wait for scheduled upload of modified file")
	public void testUploadModified() throws IOException {
		var in = Mockito.mock(InputStream.class);
		Mockito.when(file.isDirty()).thenReturn(true);
		Mockito.when(file.asPersistableStream()).thenReturn(in);
		Mockito.when(provider.write(Mockito.eq(path), Mockito.eq(true), Mockito.eq(in), Mockito.any())).thenReturn(CompletableFuture.completedFuture(null));

		uploader.scheduleUpload(file);

		Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> uploader.awaitPendingUploads().toCompletableFuture().get());
		Mockito.verify(provider).write(Mockito.eq(path), Mockito.eq(true), Mockito.eq(in), Mockito.any());
	}

	@Test
	@DisplayName("Error during upload")
	public void testFailingUpload() {
		var e = new CloudProviderException("fail.");
		Mockito.when(file.isDirty()).thenReturn(true);
		Mockito.when(provider.write(Mockito.any(), Mockito.anyBoolean(), Mockito.any(), Mockito.any())).thenReturn(CompletableFuture.failedFuture(e));

		uploader.scheduleUpload(file);

		Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> uploader.awaitPendingUploads().toCompletableFuture().get());
	}

	@Test
	@DisplayName("wait on 0 pending uploads")
	public void awaitPendingUploadsWithEmptyQueue() {
		Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> uploader.awaitPendingUploads().toCompletableFuture().get());
	}

}