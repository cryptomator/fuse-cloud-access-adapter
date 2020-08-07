package org.cryptomator.fusecloudaccess;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.SPARSE;
import static java.nio.file.StandardOpenOption.WRITE;

public class CachedFile implements Closeable {

	private final RangeSet<Long> populatedRanges = TreeRangeSet.create();
	private final FileChannel fc;
	private final BiFunction<Long, Long, CompletionStage<InputStream>> loader;

	private CachedFile(FileChannel fc, BiFunction<Long, Long, CompletionStage<InputStream>> loader) {
		this.fc = fc;
		this.loader = loader;
	}

	public static CachedFile create(Path cacheFilePath, BiFunction<Long, Long, CompletionStage<InputStream>> loader) throws IOException {
		var fc = FileChannel.open(cacheFilePath, READ, WRITE, CREATE_NEW, SPARSE);
		return new CachedFile(fc, loader);
	}

	@Override
	public void close() throws IOException {
		fc.close();
	}

	/**
	 * Loads content into the cache file (if necessary) and provides access to the file channel that will then contain
	 * the requested content, so it can be consumed via {@link FileChannel#transferTo(long, long, WritableByteChannel)}.
	 *
	 * @param offset First byte to read (inclusive)
	 * @param size   Number of bytes to load
	 * @return A CompletionStage that completes as soon as the requested range is available
	 */
	public CompletionStage<FileChannel> load(long offset, long size) {
		Preconditions.checkArgument(offset >= 0);
		Preconditions.checkArgument(size > 0);
		var range = Range.closedOpen(offset, offset + size);
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
		return loader.apply(offset, size).thenCompose(in -> {
			try (var ch = Channels.newChannel(in)) {
				ByteBuffer buf = ByteBuffer.allocateDirect(1024);
				long pos = offset;
				while (pos < offset + size) {
					buf.clear();
					int read = ch.read(buf);
					if (read == -1) {
						break;
					}
					buf.flip();
					fc.write(buf, pos);
					pos += read;
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
