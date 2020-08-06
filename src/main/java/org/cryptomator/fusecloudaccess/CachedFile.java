package org.cryptomator.fusecloudaccess;

import com.google.common.base.Preconditions;
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

import static java.nio.file.StandardOpenOption.*;

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
			return loader.apply(offset, size).thenCompose(in -> {
				try (var ch = Channels.newChannel(in)) {
					fc.position(offset).write(ByteBuffer.allocateDirect(1)); // grow file if needed
					var transferred = fc.transferFrom(ch, offset, size); // write contents to file
					var transferredRange = Range.closedOpen(offset, offset + transferred);
					populatedRanges.add(transferredRange);
					return CompletableFuture.completedFuture(fc);
				} catch (IOException e) {
					return CompletableFuture.failedFuture(e);
				}
			});
		}
	}

}
