package org.cryptomator.fusecloudaccess;

import com.google.common.base.Preconditions;
import jnr.ffi.Pointer;

import java.io.IOException;
import java.io.InputStream;
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
	 * @return The total number of bytes read, which is <code>count</code> unless reaching EOF.
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

	/**
	 * Transfers up to <code>count</code> bytes from <code>in</code> to this file channel starting at <code>position</code>.
	 *
	 * @param src              The source to read from
	 * @param position         The position in the file channel
	 * @param count            The number of bytes to read
	 * @param totalTransferred Already trasnferred bytes (MUST BE 0) - used internally during recursion
	 * @return The total number of bytes transferred, which is <code>count</code> unless reaching EOF.
	 */
	public CompletableFuture<Long> transferFrom(InputStream src, long position, long count, long totalTransferred) {
		Preconditions.checkArgument(position >= 0);
		Preconditions.checkArgument(count > 0);
		Preconditions.checkArgument(totalTransferred >= 0);
		int n = (int) Math.min(BUFFER_SIZE, count); // int-cast: n <= BUFFER_SIZE
		try {
			byte[] bytes = src.readNBytes(n);
			if (bytes.length == 0) { // EOF
				return CompletableFuture.completedFuture(totalTransferred);
			}
			return this.writeAll(ByteBuffer.wrap(bytes), position).thenCompose(written -> {
				assert bytes.length == written;
				if (written == count // DONE, wrote requested number of bytes
						|| bytes.length < n) { // EOF
					return CompletableFuture.completedFuture(totalTransferred + written);
				} else { // CONTINUE, further bytes to be transferred
					assert written < count;
					assert written == n;
					return this.transferFrom(src, position + written, count - written, totalTransferred + written);
				}
			});
		} catch (IOException e) {
			return CompletableFuture.failedFuture(e);
		}
	}

	public CompletableFuture<Integer> read(ByteBuffer dst, long position) {
		CompletableFuture<Integer> future = new CompletableFuture<>();
		fc.read(dst, position, future, new FutureCompleter());
		return future;
	}

	/**
	 * Other than {@link #write(ByteBuffer, long)}, this method keeps writing until all available bytes in
	 * <code>src</code> have been written. Upon completion, <code>src</code>'s position will have proceeded to its
	 * limit and therefore no more bytes are available.
	 * <p>
	 * Use {@link ByteBuffer#asReadOnlyBuffer()} to prevent unwanted concurrent modifications to <code>src</code>.
	 *
	 * @param src      The data to write
	 * @param position The position in the file channel
	 * @return The number of bytes written (i.e. the number of bytes available in <code>src</code>).
	 */
	public CompletableFuture<Integer> writeAll(ByteBuffer src, long position) {
		return writeAllInternal(src, position, 0);
	}

	private CompletableFuture<Integer> writeAllInternal(ByteBuffer src, long position, int totalWritten) {
		return write(src, position).thenCompose(written -> {
			if (src.hasRemaining()) {
				return writeAllInternal(src, position + written, totalWritten + written);
			} else {
				return CompletableFuture.completedFuture(totalWritten + written);
			}
		});
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
