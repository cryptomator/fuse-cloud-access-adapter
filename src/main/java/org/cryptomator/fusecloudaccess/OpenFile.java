package org.cryptomator.fusecloudaccess;

import com.google.common.base.MoreObjects;
import jnr.constants.platform.OpenFlags;
import jnr.ffi.Pointer;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.ProgressListener;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

class OpenFile {

	private final CloudProvider provider;
	private final Path path;
	private final Set<OpenFlags> flags;
	private CompletionStage<Path> cachePopulated;

	public OpenFile(CloudProvider provider, Path path, Set<OpenFlags> flags, CompletionStage<Path> cachePopulated) {
		this.provider = provider;
		this.path = path;
		this.flags = flags;
		this.cachePopulated = cachePopulated;
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

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(OpenFile.class) //
				.add("path", path) //
				.toString();
	}

}
