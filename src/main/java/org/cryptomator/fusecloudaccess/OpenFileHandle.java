package org.cryptomator.fusecloudaccess;

import jnr.ffi.Pointer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

class OpenFileHandle {

	private static final int BUFFER_SIZE = 1024;

	private final OpenFile openFile;
	private final long id;

	public OpenFileHandle(OpenFile openFile, long id) {
		this.openFile = openFile;
		this.id = id;
	}

	public OpenFile getFile() {
		return openFile;
	}

	long getId() {
		return id;
	}

	/**
	 * Reads up to {@code size} bytes beginning at {@code offset} into {@code buf}.
	 *
	 * @param buf    Buffer
	 * @param offset Position of first byte to read
	 * @param size   Number of bytes to read
	 * @return A CompletionStage either containing the actual number of bytes read (can be less than {@code size} if reached EOF)
	 * or failing with an {@link IOException}
	 */
	public CompletionStage<Integer> read(Pointer buf, long offset, long size) {
		return openFile.load(offset, size).thenCompose(fc -> {
			try {
				long pos = offset;
				while (pos < offset + size) {
					int n = (int) Math.min(BUFFER_SIZE, size - (pos - offset)); // int-cast: n <= BUFFER_SIZE
					var out = new ByteArrayOutputStream();
					long transferred = fc.transferTo(pos, n, Channels.newChannel(out));
					assert transferred == out.size();
					buf.put(pos - offset, out.toByteArray(), 0, out.size());
					pos += transferred;
					if (transferred < n) {
						break; // EOF
					}
				}
				int totalRead = (int) (pos - offset); // TODO: can we return long?
				return CompletableFuture.completedFuture(totalRead);
			} catch (IOException e) {
				return CompletableFuture.failedFuture(e);
			}
		});
	}

	/**
	 * Writes up to {@code size} bytes beginning at {@code offset} from {@code buf} to this file.
	 *
	 * @param buf    Buffer
	 * @param offset Position of first byte to write
	 * @param size   Number of bytes to write
	 * @return A CompletionStage either containing the actual number of bytes written or failing with an {@link IOException}
	 */
	public CompletionStage<Integer> write(Pointer buf, long offset, long size) {
		openFile.markDirty();
		return openFile.load(0, Long.MAX_VALUE).thenCompose(fc -> {
			try {
				long pos = offset;
				while (pos < offset + size) {
					int n = (int) Math.min(BUFFER_SIZE, size - (pos - offset)); // int-cast: n <= BUFFER_SIZE
					byte[] tmp = new byte[n];
					buf.get(pos - offset, tmp, 0, n);
					var in = new ByteArrayInputStream(tmp);
					pos += fc.transferFrom(Channels.newChannel(in), pos, n);
				}
				int totalRead = (int) (pos - offset); // TODO: can we return long?
				return CompletableFuture.completedFuture(totalRead);
			} catch (IOException e) {
				return CompletableFuture.failedFuture(e);
			}
		});
	}

	CompletionStage<Void> truncate(long size) {
		try {
			openFile.truncate(size);
			return CompletableFuture.completedFuture(null);
		} catch (IOException e) {
			return CompletableFuture.failedFuture(e);
		}
	}
}
