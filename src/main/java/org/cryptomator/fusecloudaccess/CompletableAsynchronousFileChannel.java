package org.cryptomator.fusecloudaccess;

import com.google.common.base.Preconditions;
import jnr.ffi.Pointer;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.CompletableFuture;

class CompletableAsynchronousFileChannel implements Closeable {

	private static final int BUFFER_SIZE = 4 * 1024 * 1024; // 4 MiB

	private final AsynchronousFileChannel fc;

	public CompletableAsynchronousFileChannel(AsynchronousFileChannel fc) {
		this.fc = fc;
	}

	/**
	 * Reads <code>count</code> bytes beginning from <code>position</code> into <code>ptr</code>.
	 *
	 * @param ptr      The buffer where to put read bytes
	 * @param position The position in the file channel
	 * @param count    The number of bytes to read
	 * @return The total number of bytes read, which is <code>count</code> unless reaching EOF.
	 */
	public CompletableFuture<Integer> readToPointer(Pointer ptr, long position, long count) {
		Preconditions.checkArgument(position >= 0);
		Preconditions.checkArgument(count > 0);
		return readToPointer(ptr, position, count, 0);
	}

	private CompletableFuture<Integer> readToPointer(Pointer ptr, long position, long remaining, int totalRead) {
		assert position >= 0;
		assert remaining > 0;
		assert totalRead >= 0;
		int n = (int) Math.min(BUFFER_SIZE, remaining); // int-cast: n <= BUFFER_SIZE
		ByteBuffer buffer = ByteBuffer.allocate(n);
		return this.read(buffer, position).thenCompose(read -> {
			assert read <= n;
			if (read == -1) { // EOF
				return CompletableFuture.completedFuture(totalRead);
			}
			buffer.flip();
			ptr.put(totalRead, buffer.array(), buffer.position(), buffer.limit());
			if (read == remaining // DONE, read requested number of bytes
					|| read < n) { // EOF
				return CompletableFuture.completedFuture(totalRead + read);
			} else { // CONTINUE, further bytes to be read
				assert read < remaining;
				assert read == n;
				return this.readToPointer(ptr, position + read, remaining - read, totalRead + read);
			}
		});
	}

	/**
	 * Writes <code>count</code> bytes from <code>ptr</code> into the file channel, starting at <code>position</code>.
	 *
	 * @param ptr      The buffer where to get bytes to be written
	 * @param position The position in the file channel
	 * @param count    The number of bytes to write
	 * @return The total number of bytes written which should always be <code>count</code>.
	 */
	public CompletableFuture<Integer> writeFromPointer(Pointer ptr, long position, long count) {
		Preconditions.checkArgument(position >= 0);
		Preconditions.checkArgument(count > 0);
		return writeFromPointer(ptr, position, count, 0);
	}

	private CompletableFuture<Integer> writeFromPointer(Pointer ptr, long position, long remaining, int totalWritten) {
		assert position >= 0;
		assert remaining > 0;
		assert totalWritten >= 0;
		int n = (int) Math.min(BUFFER_SIZE, remaining); // int-cast: n <= BUFFER_SIZE
		byte[] buffer = new byte[n];
		ptr.get(position, buffer, totalWritten, n);
		return this.writeAll(ByteBuffer.wrap(buffer), position).thenCompose(written -> {
			assert written == n;
			if (written == remaining) { // DONE, wrote requested number of bytes
				return CompletableFuture.completedFuture(totalWritten + written);
			} else { // CONTINUE, further bytes to be written
				assert written < remaining;
				return writeFromPointer(ptr, position + written, remaining - written, totalWritten + written);
			}
		});
	}

	/**
	 * Transfers up to <code>count</code> bytes from <code>in</code> to this file channel starting at <code>position</code>.
	 *
	 * @param src      The source to read from
	 * @param position The position in the file channel
	 * @param count    The number of bytes to transfer
	 * @return The total number of bytes transferred, which is <code>count</code> unless reaching EOF.
	 */
	public CompletableFuture<Long> transferFrom(InputStream src, long position, long count) {
		Preconditions.checkArgument(position >= 0);
		Preconditions.checkArgument(count > 0);
		return transferFrom(src, position, count, 0l);
	}

	private CompletableFuture<Long> transferFrom(InputStream src, long position, long remaining, long totalTransferred) {
		assert position >= 0;
		assert remaining > 0;
		assert totalTransferred >= 0;
		int n = (int) Math.min(BUFFER_SIZE, remaining); // int-cast: n <= BUFFER_SIZE
		try {
			byte[] bytes = src.readNBytes(n);
			if (bytes.length == 0) { // EOF
				return CompletableFuture.completedFuture(totalTransferred);
			}
			return this.writeAll(ByteBuffer.wrap(bytes), position).thenCompose(written -> {
				assert bytes.length == written;
				if (written == remaining // DONE, transferred requested number of bytes
						|| bytes.length < n) { // EOF
					return CompletableFuture.completedFuture(totalTransferred + written);
				} else { // CONTINUE, further bytes to be transferred
					assert written < remaining;
					assert written == n;
					return this.transferFrom(src, position + written, remaining - written, totalTransferred + written);
				}
			});
		} catch (IOException e) {
			return CompletableFuture.failedFuture(e);
		}
	}

	/**
	 * Transfers up to <code>count</code> bytes from this file to <code>in</code> starting at <code>position</code>.
	 *
	 * @param position The position in the file channel
	 * @param count    The number of bytes to transfer
	 * @param dst      The target to write to
	 * @return The total number of bytes transferred, which is <code>count</code> unless reaching EOF.
	 */
	public CompletableFuture<Long> transferTo(long position, long count, WritableByteChannel dst) {
		Preconditions.checkArgument(position >= 0);
		Preconditions.checkArgument(count > 0);
		return transferTo(position, count, dst, 0l);
	}

	private CompletableFuture<Long> transferTo(long position, long remaining, WritableByteChannel dst, long totalTransferred) {
		assert position >= 0;
		assert remaining > 0;
		assert totalTransferred >= 0;
		ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
		return read(buffer, position).thenCompose(read -> {
			if (read == -1) {
				return CompletableFuture.completedFuture(totalTransferred);
			}
			buffer.flip();
			int written = 0;
			try {
				while (buffer.hasRemaining()) {
					written += dst.write(buffer);
				}
			} catch (IOException e) {
				return CompletableFuture.failedFuture(e);
			}
			assert written == read;
			if (written == remaining // DONE, transferred requested number of bytes
					|| read < buffer.capacity()) { // EOF
				return CompletableFuture.completedFuture(totalTransferred + written);
			} else { // CONTINUE, further bytes to be transferred
				assert written < remaining;
				assert read == buffer.capacity();
				return this.transferTo(position + written, remaining - written, dst, totalTransferred + written);
			}
		});
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

	public boolean isOpen() {
		return fc.isOpen();
	}

	public long size() throws IOException {
		return fc.size();
	}

	public void truncate(long size) throws IOException {
		fc.truncate(size);
	}

	public void force(boolean metaData) throws IOException {
		fc.force(metaData);
	}

	@Override
	public void close() throws IOException {
		fc.close();
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
