package org.cryptomator.fusecloudaccess;

import jnr.ffi.Runtime;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.exceptions.CloudProviderException;
import org.cryptomator.cloudaccess.api.exceptions.NotFoundException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.struct.FileStat;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class CloudAccessFSTest {

	static final int TIMEOUT = 100;

	private CloudAccessFS cloudFs;

	@BeforeEach
	public void setup() {
		CloudProvider cloudProvider = Mockito.mock(CloudProvider.class);
		cloudFs = new CloudAccessFS(cloudProvider, TIMEOUT);
	}

	@DisplayName("testReturnOrTimoutOnRegularExecution")
	@Test
	public void testRegular() {
		int expectedResult = 1337;
		var future = CompletableFuture.completedFuture(expectedResult);
		Assertions.assertEquals(expectedResult, cloudFs.returnOrTimeout(future));
	}

	@DisplayName("testReturnOrTimoutOnInterrupt")
	@Test
	public void testInterrupt() throws InterruptedException {
		AtomicInteger actualResult = new AtomicInteger();
		Thread t = new Thread(() -> {
			actualResult.set(cloudFs.returnOrTimeout(new CompletableFuture<>()));
		});
		t.start();
		t.interrupt();
		t.join();
		Assertions.assertEquals(-ErrorCodes.EINTR(), actualResult.get());
	}

	@DisplayName("testReturnOrTimeoutOnExecutionException")
	@Test
	public void testExecution() {
		CompletableFuture future = CompletableFuture.completedFuture(0)
				.thenApply(i -> {
					throw new TestException();
				});
		Assertions.assertEquals(-ErrorCodes.EIO(), cloudFs.returnOrTimeout(future));
	}

	@DisplayName("testReturnOrTimeoutOnTimeout")
	@Test
	public void testTimeout() {
		Assertions.assertEquals(-ErrorCodes.ETIMEDOUT(), cloudFs.returnOrTimeout(new CompletableFuture<>()));
	}

	private static class TestException extends RuntimeException {
		TestException() {
			super();
			setStackTrace(new StackTraceElement[] {});
		}
	}

	//TODO: use good displayable names!
	static class GetAttrTests{

		private static final Runtime RUNTIME = Runtime.getSystemRuntime();
		private static final Path PATH = Path.of("some/path/to/resource");

		private CloudProvider provider;
		private CloudAccessFS cloudFs;
		private FileStat fileStat;

		@BeforeEach
		public void setup(){
			provider = Mockito.mock(CloudProvider.class);
			cloudFs = new CloudAccessFS(provider,CloudAccessFSTest.TIMEOUT);
			fileStat = new FileStat(RUNTIME);
		}

		@Test
		public void testGetAttrSuccess(){
			Mockito.when(provider.itemMetadata(PATH))
					.thenReturn(CompletableFuture.completedFuture(CloudItemMetadataProvider.ofPath(PATH)));

			Assertions.assertEquals(0,cloudFs.getattr(PATH.toString(),fileStat));
		}

		@Test
		public void testGetAttrNotFound(){
			Mockito.when(provider.itemMetadata(PATH))
					.thenReturn(CompletableFuture.failedFuture(new NotFoundException()));

			Assertions.assertEquals(-ErrorCodes.ENOENT(),cloudFs.getattr(PATH.toString(),fileStat));
		}

		@Test
		public void testGetAttrCloudError(){
			Mockito.when(provider.itemMetadata(PATH))
					.thenReturn(CompletableFuture.failedFuture(new CloudProviderException()));

			Assertions.assertEquals(-ErrorCodes.EIO(),cloudFs.getattr(PATH.toString(),fileStat));
		}

		@Test
		public void testGetAttrSomeException(){
			Mockito.when(provider.itemMetadata(PATH))
					.thenReturn(CompletableFuture.failedFuture(new RuntimeException()));

			Assertions.assertEquals(-ErrorCodes.EIO(),cloudFs.getattr(PATH.toString(),fileStat));
		}
	}
}
