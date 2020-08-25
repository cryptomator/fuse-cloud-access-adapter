package org.cryptomator.fusecloudaccess;

import org.cryptomator.cloudaccess.api.CloudPath;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.exceptions.CloudProviderException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
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

		var futureResult = uploader.scheduleUpload(file);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertNull(result);
		Mockito.verify(provider, Mockito.never()).write(Mockito.any(), Mockito.anyBoolean(), Mockito.any(), Mockito.any());
	}

	@Test
	@DisplayName("wait for scheduled upload of modified file")
	public void testUploadModified() throws IOException {
		var in = Mockito.mock(InputStream.class);
		Mockito.when(file.isDirty()).thenReturn(true);
		Mockito.when(file.asPersistableStream()).thenReturn(in);
		Mockito.when(provider.write(Mockito.eq(path), Mockito.eq(true), Mockito.eq(in), Mockito.any())).thenReturn(CompletableFuture.completedFuture(null));

		var futureResult = uploader.scheduleUpload(file);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertNull(result);
		Mockito.verify(provider).write(Mockito.eq(path), Mockito.eq(true), Mockito.eq(in), Mockito.any());
	}

	@Test
	@DisplayName("Error during upload")
	public void testFailingUpload() {
		var e = new CloudProviderException("fail.");
		Mockito.when(file.isDirty()).thenReturn(true);
		Mockito.when(provider.write(Mockito.any(), Mockito.anyBoolean(), Mockito.any(), Mockito.any())).thenReturn(CompletableFuture.failedFuture(e));

		var futureResult = uploader.scheduleUpload(file);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertNull(result);
	}

	@Test
	@DisplayName("wait on 0 pending uploads")
	public void testAwaitPendingUploadsWithEmptyQueue() {
		var futureResult = uploader.awaitPendingUploads();
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertNull(result);
	}

	@Nested
	@DisplayName("with two scheduled uploads ...")
	public class WithScheduledUploads {

		private CloudPath path1 = Mockito.mock(CloudPath.class, "/path/in/cloud/1");
		private CloudPath path2 = Mockito.mock(CloudPath.class, "/path/in/cloud/2");
		private OpenFile file1 = Mockito.mock(OpenFile.class);
		private OpenFile file2 = Mockito.mock(OpenFile.class);
		private CompletableFuture upload1 = new CompletableFuture();
		private CompletableFuture upload2 = new CompletableFuture();

		@BeforeEach
		public void setup() {
			Mockito.when(file1.getPath()).thenReturn(path1);
			Mockito.when(file2.getPath()).thenReturn(path2);
			Mockito.when(file1.isDirty()).thenReturn(true);
			Mockito.when(file2.isDirty()).thenReturn(true);
			Mockito.when(provider.write(Mockito.eq(path1), Mockito.anyBoolean(), Mockito.any(), Mockito.any())).thenReturn(upload1);
			Mockito.when(provider.write(Mockito.eq(path2), Mockito.anyBoolean(), Mockito.any(), Mockito.any())).thenReturn(upload2);
			uploader.scheduleUpload(file1);
			uploader.scheduleUpload(file2);
		}

		@Test
		@DisplayName("wait for all")
		public void testAwaitPendingUploads() {
			upload1.complete(null);
			upload2.complete(null);

			var futureResult = uploader.awaitPendingUploads();
			var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

			Assertions.assertNull(result);
		}

		@Test
		@DisplayName("wait for all, despite one failing")
		public void waitForPendingUploadsWithOneFailing() {
			upload1.complete(null);
			upload2.completeExceptionally(new CloudProviderException("fail."));

			var futureResult = uploader.awaitPendingUploads();
			var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

			Assertions.assertNull(result);
		}

	}

}