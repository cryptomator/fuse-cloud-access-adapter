package org.cryptomator.fusecloudaccess;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import jnr.constants.platform.OpenFlags;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.ProgressListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.SPARSE;
import static java.nio.file.StandardOpenOption.WRITE;

public class CachedFile implements Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(CachedFile.class);

	private final Path path;
	private final FileChannel fc;
	private final CloudProvider provider;
	private final RangeSet<Long> populatedRanges;
	private final AtomicLong fileHandleGen;
	private final ConcurrentMap<Long, CachedFileHandle> handles;
	private boolean dirty;

	CachedFile(Path path, FileChannel fc, CloudProvider provider, RangeSet<Long> populatedRanges) {
		this.path = path;
		this.fc = fc;
		this.provider = provider;
		this.populatedRanges = populatedRanges;
		this.fileHandleGen = new AtomicLong();
		this.handles = new ConcurrentHashMap<>();
	}

	public static CachedFile create(Path path, Path tmpFilePath, CloudProvider provider, long initialSize) throws IOException {
		var fc = FileChannel.open(tmpFilePath, READ, WRITE, CREATE_NEW, SPARSE);
		if (initialSize > 0) {
			fc.write(ByteBuffer.allocateDirect(1), initialSize - 1); // grow file to initialSize
		}
		return new CachedFile(path, fc, provider, TreeRangeSet.create());
	}

	@Override
	public void close() throws IOException {
		fc.close();
	}

	public CachedFileHandle openFileHandle() {
		var handleId = fileHandleGen.incrementAndGet();
		var handle = new CachedFileHandle(this, handleId);
		handles.put(handleId, handle);
		return handle;
	}

	public CompletionStage<Void> releaseFileHandle(long fileHandle) {
		handles.remove(fileHandle);
		if (handles.isEmpty() && dirty) {
			try {
				LOG.debug("uploading {}", path);
				fc.position(0);
				// TODO Performance: schedule upload on background task, return immediately, save file contents in "lost+found" dir on error
				return provider.write(path, true, Channels.newInputStream(fc), ProgressListener.NO_PROGRESS_AWARE).thenRun(() -> {
					LOG.debug("uploaded {}", path);
				});
			} catch (IOException e) {
				return CompletableFuture.failedFuture(e);
			}
		} else {
			return CompletableFuture.completedFuture(null);
		}
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
		Preconditions.checkArgument(count >= 0);
		try {
			if (offset > fc.size()) {
				throw new EOFException("Requested range beyond EOF");
			}
		} catch (IOException e) {
			return CompletableFuture.failedFuture(e);
		}
		var range = Range.closedOpen(offset, offset + count);
		if (range.isEmpty() || populatedRanges.encloses(range)) {
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
				long transferred = fc.transferFrom(ch, offset, size);
				var transferredRange = Range.closedOpen(offset, offset + transferred);
				populatedRanges.add(transferredRange);
				return CompletableFuture.completedFuture(null);
			} catch (IOException e) {
				return CompletableFuture.failedFuture(e);
			}
		});
	}

	void truncate(long size) throws IOException {
		fc.truncate(size);
		markDirty();
	}

	void markDirty() {
		this.dirty = true;
	}

	boolean isDirty() {
		return dirty;
	}
}
