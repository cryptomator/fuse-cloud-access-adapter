package org.cryptomator.fusecloudaccess;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;
import java.util.function.Supplier;

class CompletionUtils {

	public static <T> CompletionStage<T> runLocked(StampedLock lock, Supplier<CompletionStage<T>> completionStage) {
		long stamp = lock.writeLock();
		return runAlways(completionStage.get(), () -> lock.unlock(stamp));
	}

	public static <T> CompletionStage<T> runAlways(CompletionStage<T> completionStage, Runnable runAlways) {
		return completionStage.handle((result, exception) -> {
			runAlways.run();
			if (exception != null) {
				return CompletableFuture.<T>failedFuture(exception);
			} else {
				return CompletableFuture.completedFuture(result);
			}
		}).thenCompose(Function.identity());
	}

}
