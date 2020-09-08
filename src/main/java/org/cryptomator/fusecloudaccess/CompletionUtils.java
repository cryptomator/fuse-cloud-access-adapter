package org.cryptomator.fusecloudaccess;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

class CompletionUtils {

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
