package org.cryptomator.fusecloudaccess;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.StampedLock;

public class CompletionUtilsTest {

	@Test
	public void testRunLockedSuccessfully() throws ExecutionException, InterruptedException {
		var lock = Mockito.mock(StampedLock.class);

		var futureResult = CompletionUtils.runLocked(lock, () -> {
			Mockito.verify(lock).writeLock();
			Mockito.verify(lock, Mockito.never()).unlock(Mockito.anyLong());
			return CompletableFuture.completedFuture(42);
		});
		var result = futureResult.toCompletableFuture().get();

		Assertions.assertEquals(42, result);
		Mockito.verify(lock).unlock(Mockito.anyLong());
	}

	@Test
	public void testRunLockedExceptionally() {
		var e = new Exception("fail.");
		var lock = Mockito.mock(StampedLock.class);

		var futureResult = CompletionUtils.runLocked(lock, () -> {
			Mockito.verify(lock).writeLock();
			Mockito.verify(lock, Mockito.never()).unlock(Mockito.anyLong());
			return CompletableFuture.failedFuture(e);
		});
		var thrown = Assertions.assertThrows(ExecutionException.class, () -> futureResult.toCompletableFuture().get());

		Assertions.assertEquals(e, thrown.getCause());
		Mockito.verify(lock).unlock(Mockito.anyLong());
	}

	@Test
	public void testRunAlwaysSuccessfully() throws ExecutionException, InterruptedException {
		var runnable = Mockito.mock(Runnable.class);

		var futureResult = CompletionUtils.runAlways(CompletableFuture.completedFuture(42), runnable);
		var result = futureResult.toCompletableFuture().get();

		Assertions.assertEquals(42, result);
		Mockito.verify(runnable).run();
	}

	@Test
	public void testRunAlwaysExceptionally() {
		var e = new Exception("fail.");
		var runnable = Mockito.mock(Runnable.class);

		var futureResult = CompletionUtils.runAlways(CompletableFuture.failedFuture(e), runnable);
		var thrown = Assertions.assertThrows(ExecutionException.class, () -> futureResult.toCompletableFuture().get());

		Assertions.assertEquals(e, thrown.getCause());
		Mockito.verify(runnable).run();
	}

}