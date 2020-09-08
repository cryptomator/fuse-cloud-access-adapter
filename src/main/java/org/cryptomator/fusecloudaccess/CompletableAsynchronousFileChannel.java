package org.cryptomator.fusecloudaccess;

import com.google.common.base.Preconditions;
import jnr.ffi.Pointer;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;

class CompletableAsynchronousFileChannel {

	private static final int BUFFER_SIZE = 4 * 1024 * 1024; // 4 MiB

	private final AsynchronousFileChannel fc;

	public CompletableAsynchronousFileChannel(AsynchronousFileChannel fc) {
		this.fc = fc;
	}

	/**
	 * Reads <code>count</code> bytes beginning from <code>position</code> into <code>ptr</code>.
	 *
	 * @param ptr       The buffer where to put read bytes
	 * @param position  The position in the file channel
	 * @param count     The number of bytes to read
	 * @param totalRead Already read bytes (MUST BE 0) - used internally during recursion
	 * @return A completable future which returns the total number of read bytes,
	 * which is equal to <code>count</code> unless reaching EOF.
	 */
	public CompletableFuture<Long> readToPointer(Pointer ptr, long position, long count, long totalRead) {
		Preconditions.checkArgument(position >= 0);
		Preconditions.checkArgument(count > 0);
		Preconditions.checkArgument(totalRead >= 0);
		int n = (int) Math.min(BUFFER_SIZE, count); // int-cast: n <= BUFFER_SIZE
		ByteBuffer buffer = ByteBuffer.allocate(n);
		return this.read(buffer, position).thenCompose(read -> {
			assert read <= n;
			if (read == -1) { // EOF
				return CompletableFuture.completedFuture(totalRead);
			}
			buffer.flip();
			ptr.put(totalRead, buffer.array(), buffer.position(), buffer.limit());
			if (read == count // DONE, read requested number of bytes
					|| read < n) { // EOF
				return CompletableFuture.completedFuture(totalRead + read);
			} else { // CONTINUE, further bytes to be read
				assert read < count;
				assert read == n;
				return this.readToPointer(ptr, position + read, count - read, totalRead + read);
			}
		});
	}

	public CompletableFuture<Integer> read(ByteBuffer dst, long position) {
		CompletableFuture<Integer> future = new CompletableFuture<>();
		fc.read(dst, position, future, new FutureCompleter());
		return future;
	}

	public CompletableFuture<Integer> write(ByteBuffer src, long position) {
		CompletableFuture<Integer> future = new CompletableFuture<>();
		fc.write(src, position, future, new FutureCompleter());
		return future;
	}

	private static class FutureCompleter implements CompletionHandler<Integer, CompletableFuture<Integer>> {

		@Override
		public void completed(Integer result, CompletableFuture<Integer> future) {
			future.complete(result);
		}

		@Override
		public void failed(Throwable exc, CompletableFuture<Integer> future) {
			future.completeExceptionally(exc);
		}
	}

}
