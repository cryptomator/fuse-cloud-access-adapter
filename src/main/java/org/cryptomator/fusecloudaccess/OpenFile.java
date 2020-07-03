package org.cryptomator.fusecloudaccess;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.base.MoreObjects;
import jnr.constants.platform.OpenFlags;
import jnr.ffi.Pointer;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.ProgressListener;

class OpenFile {

	private final CloudProvider provider;
	private final Path path;
	private final Set<OpenFlags> flags;
	private final int timeoutMillis;

	public OpenFile(CloudProvider provider, Path path, Set<OpenFlags> flags, int timeoutMillis) {
		this.provider = provider;
		this.path = path;
		this.flags = flags;
		this.timeoutMillis = timeoutMillis;
	}

	/**
	 * Reads up to {@code num} bytes beginning at {@code offset} into {@code buf}
	 *
	 * @param buf Buffer
	 * @param offset Position of first byte to read
	 * @param size Number of bytes to read
	 * @return Actual number of bytes read (can be less than {@code size} if reached EOF).
	 * @throws IOException If an exception occurs during read.
	 * @throws InterruptedException If the operation is interrupted while waiting for an input stream
	 * @throws ExecutionException If opening an input stream to read from failed (see cause for details)
	 * @throws TimeoutException If opening an input stream took too long
	 */
	public int read(Pointer buf, long offset, long size) throws IOException, InterruptedException, ExecutionException, TimeoutException {
		try (var in = provider.read(path, offset, size, ProgressListener.NO_PROGRESS_AWARE).toCompletableFuture().get(100, TimeUnit.MILLISECONDS)) {
			byte[] tmp = new byte[1024];
			long pos = offset;
			while (pos < offset + size) {
				int read = in.read(tmp);
				if (read == -1) {
					break;
				}
				buf.put(pos - offset, tmp, 0, read);
				pos += read;
			}
			return (int) (pos - offset);
		}
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(OpenFile.class) //
				.add("path", path) //
				.toString();
	}

}
