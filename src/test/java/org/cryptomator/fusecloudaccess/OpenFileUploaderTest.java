package org.cryptomator.fusecloudaccess;

import org.cryptomator.cloudaccess.api.CloudPath;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.exceptions.CloudProviderException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

public class OpenFileUploaderTest {

	private CloudProvider provider;
	private OpenFileUploader uploader;
	private OpenFile file;
	private CloudPath path;

	@TempDir
	Path cacheDir;

	@BeforeEach
	public void setup() {
		this.provider = Mockito.mock(CloudProvider.class);
		this.uploader = new OpenFileUploader(provider, cacheDir);
		this.file = Mockito.mock(OpenFile.class);
		this.path = Mockito.mock(CloudPath.class, "/path/in/cloud");

		var fileName = Mockito.mock(CloudPath.class, "cloud");
		Mockito.when(file.getPath()).thenReturn(path);
		Mockito.when(path.getFileName()).thenReturn(fileName);
		Mockito.when(fileName.toString()).thenReturn("cloud");
	}

	@Test
	@DisplayName("noop when scheduling unmodified file")
	public void testUploadUnmodified() {
		Mockito.when(file.isDirty()).thenReturn(false);

		var futureResult = uploader.scheduleUpload(file);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertNull(result);
		Mockito.verify(provider, Mockito.never()).write(Mockito.any(), Mockito.anyBoolean(), Mockito.any(), Mockito.anyLong(), Mockito.any());
	}

	@Test
	@DisplayName("wait for scheduled upload of modified file")
	public void testUploadModified() throws IOException {
		Mockito.when(file.isDirty()).thenReturn(true);
		Mockito.doAnswer(invocation -> {
			Files.createFile(invocation.getArgument(0));
			return null;
		}).when(file).persistTo(Mockito.any());
		Mockito.when(provider.write(Mockito.eq(path), Mockito.eq(true), Mockito.any(), Mockito.anyLong(), Mockito.any())).thenReturn(CompletableFuture.completedFuture(null));

		var futureResult = uploader.scheduleUpload(file);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertEquals(file.getPath(), result);
		Mockito.verify(provider).write(Mockito.eq(path), Mockito.eq(true), Mockito.any(), Mockito.anyLong(), Mockito.any());
		Mockito.verify(file).setDirty(false);
	}

	@Test
	@DisplayName("Error during upload")
	public void testFailingUpload() throws IOException {
		var e = new CloudProviderException("fail.");
		Mockito.when(file.isDirty()).thenReturn(true);
		Mockito.doAnswer(invocation -> {
			Files.createFile(invocation.getArgument(0));
			return null;
		}).when(file).persistTo(Mockito.any());
		Mockito.when(provider.write(Mockito.any(), Mockito.anyBoolean(), Mockito.any(), Mockito.anyLong(), Mockito.any())).thenReturn(CompletableFuture.failedFuture(e));

		var futureResult = uploader.scheduleUpload(file);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertEquals(file.getPath(), result);
		Mockito.verify(file).setDirty(false);
		Mockito.verify(file).setDirty(true);
	}

	@Test
	@DisplayName("wait on 0 pending uploads")
	public void testAwaitPendingUploadsWithEmptyQueue() {
		var futureResult = uploader.awaitPendingUploads();
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertNull(result);
	}

	@Test
	@DisplayName("temp directory is cleared after failed upload")
	public void testTempDirIsClearedOnFailure() throws IOException {
		assert isEmptyDir(cacheDir);

		var e = new CloudProviderException("fail.");
		Mockito.when(file.isDirty()).thenReturn(true);
		Mockito.doAnswer(invocation -> {
			Files.createFile(invocation.getArgument(0));
			return null;
		}).when(file).persistTo(Mockito.any());
		Mockito.when(provider.write(Mockito.any(), Mockito.anyBoolean(), Mockito.any(), Mockito.anyLong(), Mockito.any())).thenReturn(CompletableFuture.failedFuture(e));

		var futureResult = uploader.scheduleUpload(file);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertEquals(file.getPath(), result);
		Assertions.assertTrue(isEmptyDir(cacheDir));
	}

	@Test
	@DisplayName("temp directory is cleared after successful upload")
	public void testTempDirIsClearedOnSuccess() throws IOException {
		assert isEmptyDir(cacheDir);

		Mockito.when(file.isDirty()).thenReturn(true);
		Mockito.doAnswer(invocation -> {
			Files.createFile(invocation.getArgument(0));
			return null;
		}).when(file).persistTo(Mockito.any());
		Mockito.when(provider.write(Mockito.eq(path), Mockito.eq(true), Mockito.any(), Mockito.anyLong(), Mockito.any())).thenReturn(CompletableFuture.completedFuture(null));

		var futureResult = uploader.scheduleUpload(file);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertEquals(file.getPath(), result);
		Assertions.assertTrue(isEmptyDir(cacheDir));
	}

	private boolean isEmptyDir(Path dir) throws IOException {
		try (Stream<Path> entries = Files.list(dir)) {
			return !entries.findFirst().isPresent();
		}
	}

	@Nested
	@DisplayName("with two scheduled uploads ...")
	public class WithScheduledUploads {

		private CloudPath path1 = Mockito.mock(CloudPath.class, "/path/in/cloud/1");
		private CloudPath path2 = Mockito.mock(CloudPath.class, "/path/in/cloud/2");
		private CloudPath fileName1 = Mockito.mock(CloudPath.class, "1");
		private CloudPath fileName2 = Mockito.mock(CloudPath.class, "2");
		private OpenFile file1 = Mockito.mock(OpenFile.class);
		private OpenFile file2 = Mockito.mock(OpenFile.class);
		private CompletableFuture upload1 = new CompletableFuture();
		private CompletableFuture upload2 = new CompletableFuture();

		@BeforeEach
		public void setup() throws IOException {
			Mockito.when(path1.getFileName()).thenReturn(fileName1);
			Mockito.when(path2.getFileName()).thenReturn(fileName2);
			Mockito.when(fileName1.toString()).thenReturn("1");
			Mockito.when(fileName2.toString()).thenReturn("2");
			Mockito.when(file1.getPath()).thenReturn(path1);
			Mockito.when(file2.getPath()).thenReturn(path2);
			Mockito.when(file1.isDirty()).thenReturn(true);
			Mockito.when(file2.isDirty()).thenReturn(true);
			Mockito.doAnswer(invocation -> {
				Files.createFile(invocation.getArgument(0));
				return null;
			}).when(file1).persistTo(Mockito.any());
			Mockito.doAnswer(invocation -> {
				Files.createFile(invocation.getArgument(0));
				return null;
			}).when(file2).persistTo(Mockito.any());
			Mockito.when(provider.write(Mockito.eq(path1), Mockito.anyBoolean(), Mockito.any(), Mockito.anyLong(), Mockito.any())).thenReturn(upload1);
			Mockito.when(provider.write(Mockito.eq(path2), Mockito.anyBoolean(), Mockito.any(), Mockito.anyLong(), Mockito.any())).thenReturn(upload2);
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