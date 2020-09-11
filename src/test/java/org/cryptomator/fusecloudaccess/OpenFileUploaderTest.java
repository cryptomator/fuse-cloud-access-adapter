package org.cryptomator.fusecloudaccess;

import org.cryptomator.cloudaccess.api.CloudPath;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.exceptions.CloudProviderException;
import org.cryptomator.fusecloudaccess.locks.LockManager;
import org.cryptomator.fusecloudaccess.locks.PathLock;
import org.cryptomator.fusecloudaccess.locks.PathLockBuilder;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
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
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;

public class OpenFileUploaderTest {

	private CloudProvider provider;
	private Path cacheDir;
	private CloudPath cloudUploadDir;
	private ExecutorService executorService;
	private ConcurrentMap<CloudPath, Future<?>> tasks;
	private LockManager lockManager;
	private OpenFileUploader uploader;
	private OpenFile file;

	@BeforeEach
	public void setup() {
		this.provider = Mockito.mock(CloudProvider.class);
		this.cacheDir = Mockito.mock(Path.class);
		this.executorService = Mockito.mock(ExecutorService.class);
		this.tasks = Mockito.mock(ConcurrentMap.class);
		this.lockManager = Mockito.mock(LockManager.class);
		this.uploader = new OpenFileUploader(provider, cacheDir, cloudUploadDir, executorService, tasks, lockManager);
		this.file = Mockito.mock(OpenFile.class);
		this.cloudUploadDir = CloudPath.of("/upload/path/in/cloud");
		Mockito.when(file.getState()).thenReturn(OpenFile.State.UPLOADING);
	}


	@Test
	@DisplayName("scheduling upload")
	public void testUploadModified() {
		var cloudPath = Mockito.mock(CloudPath.class, "/path/in/cloud");
		var task = Mockito.mock(Future.class);
		Mockito.when(file.getPath()).thenReturn(cloudPath);
		Mockito.when(executorService.submit(Mockito.any(OpenFileUploader.ScheduledUpload.class))).thenReturn(task);

		uploader.scheduleUpload(file, ignored -> {
		});

		Mockito.verify(executorService).submit(Mockito.any(OpenFileUploader.ScheduledUpload.class));
		Mockito.verify(tasks).put(cloudPath, task);
	}

	@Test
	@DisplayName("cancel pending upload")
	public void testCancelExistingUpload() {
		var cloudPath = Mockito.mock(CloudPath.class, "/path/in/cloud");
		var task = Mockito.mock(Future.class);
		Mockito.when(tasks.remove(cloudPath)).thenReturn(task);

		var canceled = uploader.cancelUpload(cloudPath);

		Assertions.assertTrue(canceled);
		Mockito.verify(task).cancel(true);
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
			this.uploader = new OpenFileUploader(provider, cacheDir, cloudUploadDir, executorService, tasks, lockManager);
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

		private Path tmpDir;
		private CloudProvider provider;
		private OpenFile openFile;
		private Consumer<OpenFile> onFinished;
		private OpenFileUploader.ScheduledUpload upload;
		private PathLockBuilder pathLockBuilder;
		private PathLock pathLock;

		@BeforeEach
		public void setup(@TempDir Path tmpDir) {
			this.tmpDir = tmpDir;
			this.provider = Mockito.mock(CloudProvider.class);
			this.openFile = Mockito.mock(OpenFile.class);
			this.onFinished = Mockito.mock(Consumer.class);
			this.pathLockBuilder = Mockito.mock(PathLockBuilder.class);
			this.pathLock = Mockito.mock(PathLock.class);
			this.upload = new OpenFileUploader.ScheduledUpload(provider, openFile, onFinished, tmpDir, cloudUploadDir, lockManager);
			Mockito.when(lockManager.createPathLock(Mockito.any())).thenReturn(pathLockBuilder);
			Mockito.when(pathLockBuilder.forWriting()).thenReturn(pathLock);
			Mockito.when(openFile.getState()).thenReturn(OpenFile.State.UPLOADING);
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
			Mockito.verifyNoMoreInteractions(lockManager);
		}

		@Test
		@DisplayName("upload fails due to CloudProviderException during actual upload")
		public void testCloudProviderExceptionDuringUpload() throws IOException {
			var e = new CloudProviderException("fail");
			var cloudPath = Mockito.mock(CloudPath.class, "/path/to/file");
			Mockito.when(openFile.persistTo(Mockito.any())).thenAnswer(invocation -> {
				Path path = invocation.getArgument(0);
				Files.write(path, new byte[42]);
				return CompletableFuture.completedFuture(null);
			});
			Mockito.when(openFile.getPath()).thenReturn(cloudPath);
			Mockito.when(openFile.getLastModified()).thenReturn(Instant.EPOCH);
			Mockito.when(openFile.getSize()).thenReturn(42l);
			Mockito.when(provider.write(Mockito.any(), Mockito.eq(true), Mockito.any(), Mockito.eq(42l), Mockito.eq(Optional.of(Instant.EPOCH)), Mockito.any()))
					.thenReturn(CompletableFuture.failedFuture(e));

			var thrown = Assertions.assertThrows(IOException.class, () -> {
				upload.call();
			});

			MatcherAssert.assertThat(thrown.getCause(), CoreMatchers.instanceOf(ExecutionException.class));
			Assertions.assertSame(e, thrown.getCause().getCause());
			Mockito.verify(onFinished).accept(Mockito.any());
			Assertions.assertEquals(0l, Files.list(tmpDir).count());
			Mockito.verifyNoMoreInteractions(lockManager);
		}

		@Test
		@DisplayName("upload succeeds")
		public void testSuccessfulUpload() throws IOException {
			var cloudPath = Mockito.mock(CloudPath.class, "/path/to/file");
			Mockito.when(openFile.persistTo(Mockito.any())).thenAnswer(invocation -> {
				Path path = invocation.getArgument(0);
				Files.write(path, new byte[42]);
				return CompletableFuture.completedFuture(null);
			});
			Mockito.when(openFile.getPath()).thenReturn(cloudPath);
			Mockito.when(openFile.getLastModified()).thenReturn(Instant.EPOCH);
			Mockito.when(openFile.getSize()).thenReturn(42l);
			Mockito.when(provider.write(Mockito.any(), Mockito.eq(true), Mockito.any(), Mockito.eq(42l), Mockito.eq(Optional.of(Instant.EPOCH)), Mockito.any())).thenReturn(CompletableFuture.completedFuture(null));
			Mockito.when(provider.move(Mockito.any(), Mockito.eq(cloudPath), Mockito.eq(true))).thenReturn(CompletableFuture.completedFuture(cloudPath));

			upload.call();

			Mockito.verify(onFinished).accept(openFile);
			Assertions.assertEquals(0l, Files.list(tmpDir).count());
			Mockito.verify(lockManager).createPathLock(cloudPath.toString());
			Mockito.verify(pathLock).close();
		}

		@Test
		@DisplayName("upload succeeds despite concurrent move")
		public void testSuccessfulUploadWithMove() throws IOException, BrokenBarrierException, InterruptedException {
			var cloudPath1 = Mockito.mock(CloudPath.class, "/path/to/file");
			var cloudPath2 = Mockito.mock(CloudPath.class, "/path/to/other/file");
			var persistedBarrier = new CyclicBarrier(2);
			var uploadedBarrier = new CyclicBarrier(2);
			Mockito.when(openFile.persistTo(Mockito.any())).thenAnswer(invocation -> {
				Path path = invocation.getArgument(0);
				Files.write(path, new byte[42]);
				persistedBarrier.await();
				return CompletableFuture.completedFuture(null);
			});
			Mockito.when(openFile.getLastModified()).thenReturn(Instant.EPOCH);
			Mockito.when(openFile.getSize()).thenReturn(42l);
			Mockito.when(provider.write(Mockito.any(), Mockito.eq(true), Mockito.any(), Mockito.eq(42l), Mockito.eq(Optional.of(Instant.EPOCH)), Mockito.any()))
					.thenAnswer(invocation -> {
						uploadedBarrier.await();
						return CompletableFuture.completedFuture(null);
					});
			Mockito.when(provider.move(Mockito.any(), Mockito.eq(cloudPath2), Mockito.eq(true))).thenReturn(CompletableFuture.completedFuture(cloudPath2));
			Mockito.when(openFile.getPath()).thenReturn(cloudPath1); // initial target path
			Assumptions.assumeFalse(cloudPath1.equals(cloudPath2));

			Future<Void> pendingUpload = CompletableFuture.runAsync(() -> {
				try {
					assert openFile.getPath() == cloudPath1;
					upload.call();
					assert openFile.getPath() == cloudPath2;
				} catch (IOException e) {
					e.printStackTrace();
				}
			});
			persistedBarrier.await();
			Mockito.when(openFile.getPath()).thenReturn(cloudPath2); // set a new target path
			uploadedBarrier.await();
			Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> pendingUpload.get());

			Mockito.verify(onFinished).accept(openFile);
			Assertions.assertEquals(0l, Files.list(tmpDir).count());
			Mockito.verify(provider).move(Mockito.any(), Mockito.eq(cloudPath2), Mockito.eq(true));
			Mockito.verify(lockManager, Mockito.never()).createPathLock(cloudPath1.toString());
			Mockito.verify(lockManager).createPathLock(cloudPath2.toString());
			Mockito.verify(pathLock).close();
		}

	}

}