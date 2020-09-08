package org.cryptomator.fusecloudaccess;

import org.cryptomator.cloudaccess.api.CloudPath;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.exceptions.CloudProviderException;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
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
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public class OpenFileUploaderTest {

	private CloudProvider provider;
	private Path cacheDir;
	private ExecutorService executorService;
	private ConcurrentMap<CloudPath, Future<?>> tasks;
	private OpenFileUploader uploader;
	private OpenFile file;

	@BeforeEach
	public void setup() {
		this.provider = Mockito.mock(CloudProvider.class);
		this.cacheDir = Mockito.mock(Path.class);
		this.executorService = Mockito.mock(ExecutorService.class);
		this.tasks = Mockito.mock(ConcurrentMap.class);
		this.uploader = new OpenFileUploader(provider, cacheDir, executorService, tasks);
		this.file = Mockito.mock(OpenFile.class);
	}

	@Test
	@DisplayName("noop when scheduling unmodified file")
	public void testUploadUnmodified() {
		Mockito.when(file.isDirty()).thenReturn(false);

		uploader.scheduleUpload(file, ignored -> {
		});

		Mockito.verify(executorService, Mockito.never()).submit(Mockito.any(Runnable.class));
		Mockito.verify(tasks, Mockito.never()).put(Mockito.any(), Mockito.any());
	}

	@Test
	@DisplayName("scheduling modified file (no previous upload)")
	public void testUploadModified() {
		var cloudPath = Mockito.mock(CloudPath.class, "/path/in/cloud");
		var task = Mockito.mock(Future.class);
		Mockito.when(file.isDirty()).thenReturn(true);
		Mockito.when(file.getPath()).thenReturn(cloudPath);
		Mockito.when(executorService.submit(Mockito.any(OpenFileUploader.ScheduledUpload.class))).thenReturn(task);

		uploader.scheduleUpload(file, ignored -> {
		});

		Mockito.verify(executorService).submit(Mockito.any(OpenFileUploader.ScheduledUpload.class));
		Mockito.verify(tasks).put(cloudPath, task);
	}

	@Test
	@DisplayName("scheduling modified file (cancel previous upload)")
	public void testUploadModifiedAndCancelPrevious() {
		var cloudPath = Mockito.mock(CloudPath.class, "/path/in/cloud");
		var task = Mockito.mock(Future.class);
		var previousTask = Mockito.mock(Future.class);
		Mockito.when(file.isDirty()).thenReturn(true);
		Mockito.when(file.getPath()).thenReturn(cloudPath);
		Mockito.when(executorService.submit(Mockito.any(OpenFileUploader.ScheduledUpload.class))).thenReturn(task);
		Mockito.when(tasks.put(cloudPath, task)).thenReturn(previousTask);

		uploader.scheduleUpload(file, ignored -> {
		});

		Mockito.verify(executorService).submit(Mockito.any(OpenFileUploader.ScheduledUpload.class));
		Mockito.verify(tasks).put(cloudPath, task);
		Mockito.verify(previousTask).cancel(false);
	}

	@Test
	@DisplayName("cancel pending upload")
	public void testCancelExistingUpload() {
		var cloudPath = Mockito.mock(CloudPath.class, "/path/in/cloud");
		var task = Mockito.mock(Future.class);
		Mockito.when(tasks.get(cloudPath)).thenReturn(task);

		var canceled = uploader.cancelUpload(cloudPath);

		Assertions.assertTrue(canceled);
		Mockito.verify(task).cancel(false);
	}

	@Test
	@DisplayName("cancel non-existing upload")
	public void testCancelNonexistingUpload() {
		var cloudPath = Mockito.mock(CloudPath.class, "/path/in/cloud");

		var canceled = uploader.cancelUpload(cloudPath);

		Assertions.assertFalse(canceled);
	}

	@Nested
	@DisplayName("awaitPendingUploads(...)")
	public class Termination {

		private ExecutorService executorService;
		private OpenFileUploader uploader;

		@BeforeEach
		public void setup() {
			this.executorService = Executors.newSingleThreadExecutor();
			this.uploader = new OpenFileUploader(provider, cacheDir, executorService, tasks);
		}

		@Test
		@DisplayName("0 pending uploads")
		public void testAwaitNoPendingUploads() {
			Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> {
				uploader.awaitPendingUploads(1, TimeUnit.SECONDS);
			});
			Assertions.assertTrue(executorService.isTerminated());
		}

		@Test
		@DisplayName("1 pending upload")
		public void testAwaitPendingUploads() {
			executorService.submit(() -> {
			});
			Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> {
				uploader.awaitPendingUploads(1, TimeUnit.SECONDS);
			});
			Assertions.assertTrue(executorService.isTerminated());
		}

		@Test
		@DisplayName("timeout")
		public void testAwaitPendingUploads1() {
			executorService.submit(() -> {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			});
			Assertions.assertThrows(TimeoutException.class, () -> {
				Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> {
					uploader.awaitPendingUploads(10, TimeUnit.MILLISECONDS);
				});
			});
			Assertions.assertFalse(executorService.isTerminated());
		}

	}

	@Nested
	public class ScheduledUploadTest {

		CloudProvider provider;
		OpenFile openFile;
		Consumer<OpenFile> onFinished;
		private Path tmpDir;
		private OpenFileUploader.ScheduledUpload upload;

		@BeforeEach
		public void setup(@TempDir Path tmpDir) {
			this.tmpDir = tmpDir;
			this.provider = Mockito.mock(CloudProvider.class);
			this.openFile = Mockito.mock(OpenFile.class);
			this.onFinished = Mockito.mock(Consumer.class);
			this.upload = new OpenFileUploader.ScheduledUpload(provider, openFile, onFinished, tmpDir);
		}

		@Test
		@DisplayName("upload fails due to I/O error during upload preparation")
		public void testIOErrorDuringPersistTo() throws IOException {
			var e = new IOException("fail");
			Mockito.when(openFile.persistTo(Mockito.any())).thenReturn(CompletableFuture.failedFuture(e));

			var thrown = Assertions.assertThrows(IOException.class, () -> {
				upload.call();
			});

			MatcherAssert.assertThat(thrown.getCause(), CoreMatchers.instanceOf(ExecutionException.class));
			Assertions.assertSame(e, thrown.getCause().getCause());
			Mockito.verify(onFinished).accept(Mockito.any());
			Assertions.assertEquals(0l, Files.list(tmpDir).count());
		}

		@Test
		@DisplayName("upload fails due to CloudProviderException during actual upload")
		public void testCloudProviderExceptionDuringUpload() throws IOException {
			var e = new CloudProviderException("fail");
			var cloudPath = Mockito.mock(CloudPath.class, "/path/to/file");
			var lastModified = Instant.now();
			Mockito.when(openFile.persistTo(Mockito.any())).thenAnswer(invocation -> {
				Path path = invocation.getArgument(0);
				Files.write(path, new byte[42]);
				return CompletableFuture.completedFuture(null);
			});
			Mockito.when(openFile.getPath()).thenReturn(cloudPath);
			Mockito.when(openFile.getLastModified()).thenReturn(lastModified);
			Mockito.when(openFile.getSize()).thenReturn(42l);
			Mockito.when(provider.write(Mockito.eq(cloudPath), Mockito.eq(true), Mockito.any(), Mockito.eq(42l), Mockito.eq(Optional.of(lastModified)), Mockito.any()))
					.thenReturn(CompletableFuture.failedFuture(e));

			var thrown = Assertions.assertThrows(IOException.class, () -> {
				upload.call();
			});

			MatcherAssert.assertThat(thrown.getCause(), CoreMatchers.instanceOf(ExecutionException.class));
			Assertions.assertSame(e, thrown.getCause().getCause());
			Mockito.verify(onFinished).accept(Mockito.any());
			Assertions.assertEquals(0l, Files.list(tmpDir).count());
		}

		@Test
		@DisplayName("upload succeeds")
		public void testSuccessfulUpload() throws IOException {
			var cloudPath = Mockito.mock(CloudPath.class, "/path/to/file");
			var lastModified = Instant.now();
			Mockito.when(openFile.persistTo(Mockito.any())).thenAnswer(invocation -> {
				Path path = invocation.getArgument(0);
				Files.write(path, new byte[42]);
				return CompletableFuture.completedFuture(null);
			});
			Mockito.when(openFile.getPath()).thenReturn(cloudPath);
			Mockito.when(openFile.getLastModified()).thenReturn(lastModified);
			Mockito.when(openFile.getSize()).thenReturn(42l);
			Mockito.when(provider.write(Mockito.eq(cloudPath), Mockito.eq(true), Mockito.any(), Mockito.eq(42l), Mockito.eq(Optional.of(lastModified)), Mockito.any()))
					.thenReturn(CompletableFuture.completedFuture(null));

			upload.call();

			Mockito.verify(onFinished).accept(openFile);
			Assertions.assertEquals(0l, Files.list(tmpDir).count());
		}

	}

}