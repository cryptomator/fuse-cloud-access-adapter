package org.cryptomator.fusecloudaccess;

import org.cryptomator.cloudaccess.api.CloudProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import ru.serce.jnrfuse.ErrorCodes;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class CloudAccessFSTest {

	private static final int TIMEOUT = 100;

	private CloudAccessFS cloudFs;

	@BeforeEach
	public void setup() {
		CloudProvider cloudProvider = Mockito.mock(CloudProvider.class);
		cloudFs = new CloudAccessFS(cloudProvider, TIMEOUT);
	}

	@DisplayName("returnOrTimeout(...)")
	@ParameterizedTest
	@MethodSource("provideCompletionStages")
	public void testReturnOrTimeout(CompletionStage<Integer> task, int expectedReturnCode) {
		Assertions.assertEquals(expectedReturnCode, cloudFs.returnOrTimeout(task));
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


	private static Stream<Arguments> provideCompletionStages() {
		return Stream.of(
				Arguments.of(futureOfTimeout(), -ErrorCodes.ETIMEDOUT()), //Timeout
				Arguments.of(futureOfExecutionException(), -ErrorCodes.EIO()), //generic error
				Arguments.of(CompletableFuture.completedStage(1337), 1337) //input==output
		);
	}

	private static CompletionStage<Integer> futureOfExecutionException() {
		return CompletableFuture.completedFuture(0)
				.thenApply(i -> {
					throw new TestException();
				});
	}

	private static CompletionStage<Integer> futureOfTimeout() {
		return CompletableFuture.completedFuture(0)
				.thenApplyAsync(i -> {
					try {
						Thread.currentThread().sleep(TIMEOUT + 1000);
					} catch (InterruptedException e) {
						// don't care
					}
					return 0;
				});
	}

	private static class TestException extends RuntimeException {
		TestException() {
			super();
			setStackTrace(new StackTraceElement[]{});
		}
	}


}
