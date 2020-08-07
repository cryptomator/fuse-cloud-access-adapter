package org.cryptomator.fusecloudaccess;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.ProgressListener;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static java.nio.file.StandardOpenOption.*;

public class CachedFile implements Closeable {

	private final Path path;
	private final FileChannel fc;
	private final CloudProvider provider;
	private final RangeSet<Long> populatedRanges;

	CachedFile(Path path, FileChannel fc, CloudProvider provider, RangeSet<Long> populatedRanges) {
		this.path = path;
		this.fc = fc;
		this.provider = provider;
		this.populatedRanges = populatedRanges;
	}

	public static CachedFile create(Path path, Path tmpFilePath, CloudProvider provider, long initialSize) throws IOException {
		var fc = FileChannel.open(tmpFilePath, READ, WRITE, CREATE_NEW, SPARSE);
		fc.write(ByteBuffer.allocateDirect(1), initialSize - 1); // grow file to initialSize
		return new CachedFile(path, fc, provider, TreeRangeSet.create());
	}

	@Override
	public void close() throws IOException {
		fc.close();
	}

	/**
	 * Loads content into the cache file (if necessary) and provides access to the file channel that will then contain
	 * the requested content, so it can be consumed via {@link FileChannel#transferTo(long, long, WritableByteChannel)}.
	 *
	 * @param offset First byte to read (inclusive), which must not exceed the file's size
	 * @param count  Number of bytes to load
	 * @return A CompletionStage that completes as soon as the requested range is available or fails either due to I/O errors or if requesting a range beyond EOF
	 */
	public CompletionStage<FileChannel> load(long offset, long count) {
		Preconditions.checkArgument(offset >= 0);
		Preconditions.checkArgument(count > 0);
		try {
			if (offset > fc.size()) {
				throw new EOFException("Requested range beyond EOF");
			}
		} catch (IOException e) {
			return CompletableFuture.failedFuture(e);
		}
		var range = Range.closedOpen(offset, offset + count);
		if (populatedRanges.encloses(range)) {
			return CompletableFuture.completedFuture(fc);
		} else {
			var missingRanges = ImmutableRangeSet.of(range).difference(populatedRanges);
			return CompletableFuture.allOf(missingRanges.asRanges().stream().map(this::loadMissing).toArray(CompletableFuture[]::new)).thenApply(ignored -> fc);
		}
	}

	private CompletionStage<Void> loadMissing(Range<Long> range) {
		assert !populatedRanges.intersects(range);
		long offset = range.lowerEndpoint();
		long size = range.upperEndpoint() - range.lowerEndpoint();
		return provider.read(path, offset, size, ProgressListener.NO_PROGRESS_AWARE).thenCompose(in -> {
			try (var ch = Channels.newChannel(in)) {
				ByteBuffer buf = ByteBuffer.allocateDirect(1024);
				long pos = offset;
				while (pos < offset + size) {
					buf.clear();
					int read = ch.read(buf);
					if (read == -1) {
						break;
					} else {
						buf.flip();
						fc.write(buf, pos);
						pos += read;
					}
				}
				var transferredRange = Range.closedOpen(offset, pos);
				populatedRanges.add(transferredRange);
				return CompletableFuture.completedFuture(null);
			} catch (IOException e) {
				return CompletableFuture.failedFuture(e);
			}
		});
	}

}
