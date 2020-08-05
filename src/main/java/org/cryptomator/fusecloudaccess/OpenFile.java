package org.cryptomator.fusecloudaccess;

import com.google.common.base.MoreObjects;
import jnr.constants.platform.OpenFlags;
import jnr.ffi.Pointer;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.ProgressListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

class OpenFile {

	private static final Logger LOG = LoggerFactory.getLogger(OpenFile.class);

	private final CloudProvider provider;
	private final Path path;
	private final Set<OpenFlags> flags;
	private final CompletionStage<Path> cachePopulated;
	private boolean dirty;

	public OpenFile(CloudProvider provider, Path path, Set<OpenFlags> flags, CompletionStage<Path> cachePopulated) {
		this.provider = provider;
		this.path = path;
		this.flags = flags;
		this.cachePopulated = cachePopulated;
	}

	// visible for testing
	void setDirty() {
		this.dirty = true;
	}

	// visible for testing
	boolean isDirty() {
		return dirty;
	}

	/**
	 * Reads up to {@code size} bytes beginning at {@code offset} into {@code buf}.
	 *
	 * @param buf Buffer
	 * @param offset Position of first byte to read
	 * @param size Number of bytes to read
	 * @return A CompletionStage either containing the actual number of bytes read (can be less than {@code size} if reached EOF)
	 * or failing with an {@link IOException}
	 */
	public CompletionStage<Integer> read(Pointer buf, long offset, long size) {
		return provider.read(path, offset, size, ProgressListener.NO_PROGRESS_AWARE).thenCompose(inputStream -> {
			try (var in = inputStream) {
				byte[] tmp = new byte[1024];
				long pos = offset;
				while (pos < offset + size) {
					int read = in.read(tmp);
					if (read == -1) {
						break;
					}
					int n = (int) Math.min(offset + size - pos, read); //result know to return <= 1024
					buf.put(pos - offset, tmp, 0, n);
					pos += n;
				}
				int totalRead = (int) (pos - offset);
				return CompletableFuture.completedFuture(totalRead);
			} catch (IOException e) {
				return CompletableFuture.failedFuture(e);
			}
		});
	}

	/**
	 * Writes up to {@code size} bytes beginning at {@code offset} from {@code buf} to this file.
	 *
	 * @param buf Buffer
	 * @param offset Position of first byte to write
	 * @param size Number of bytes to write
	 * @return A CompletionStage either containing the actual number of bytes written or failing with an {@link IOException}
	 */
	public CompletionStage<Integer> write(Pointer buf, long offset, long size) {
		return cachePopulated.thenCompose(cacheFile -> {
			setDirty();
			try (var ch = FileChannel.open(cacheFile, StandardOpenOption.WRITE)) {
				byte[] tmp = new byte[1024];
				ch.position(offset);
				while (ch.position() < offset + size) {
					int n = (int) Math.min(offset + size - ch.position(), tmp.length); // result know to return <= 1024
					buf.get(ch.position() - offset, tmp, 0, n);
					ch.write(ByteBuffer.wrap(tmp, 0, n));
				}
				int totalRead = (int) (ch.position() - offset);
				return CompletableFuture.completedFuture(totalRead);
			} catch (IOException e) {
				return CompletableFuture.failedFuture(e);
			}
		});
	}

	/**
	 * {@link CloudProvider#write(Path, boolean, InputStream, ProgressListener) Writes} any cached data.
	 * @return A CompletionStage succeeding after all data has been written or failing with an IOException.
	 */
	public CompletionStage<Void> flush() {
		if (!dirty) {
			return CompletableFuture.completedFuture(null);
		}
		return cachePopulated.thenCompose(cacheFile -> {
			try {
				var in = Files.newInputStream(cacheFile, StandardOpenOption.READ);
				return provider.write(path, true, in, ProgressListener.NO_PROGRESS_AWARE).thenRun(() -> this.closeSilently(in));
			} catch (IOException e) {
				return CompletableFuture.failedFuture(e);
			}
		});
	}

	public CompletionStage<Void> truncate(long size) {
		return cachePopulated.thenCompose(cacheFile -> {
			setDirty();
			try (var f = FileChannel.open(cacheFile, StandardOpenOption.WRITE)) {
				f.truncate(size);
				return CompletableFuture.completedFuture(null);
			} catch (IOException e) {
				return CompletableFuture.failedFuture(e);
			}
		});
	}

	private void closeSilently(Closeable closeable) {
		try {
			closeable.close();
		} catch (IOException e) {
			LOG.warn("Failed to close {}", closeable);
		}
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(OpenFile.class) //
				.add("path", path) //
				.toString();
	}

}
