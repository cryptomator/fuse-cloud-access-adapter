package org.cryptomator.fusecloudaccess;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeMap;
import com.google.common.collect.TreeRangeSet;
import jnr.ffi.Pointer;
import org.cryptomator.cloudaccess.api.CloudPath;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.ProgressListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.file.StandardOpenOption.*;

class OpenFile implements Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(OpenFile.class);

	private final CompletableAsynchronousFileChannel fc;
	private final CloudProvider provider;
	private final RangeSet<Long> populatedRanges;
	private final RangeMap<Long, CompletionStage<Void>> activeRequests;
	private final AtomicInteger openFileHandleCount;
	private final AtomicReference<OpenFile.State> state;
	private final int readAheadBytes;
	private volatile CloudPath path;
	private Instant lastModified;

	public enum State {UNMODIFIED, NEEDS_UPLOAD, UPLOADING, NEEDS_REUPLOAD}

	// visible for testing
	OpenFile(CloudPath path, CompletableAsynchronousFileChannel fc, CloudProvider provider, RangeSet<Long> populatedRanges, RangeMap<Long, CompletionStage<Void>> activeRequests, Instant initialLastModified, int readAheadBytes) {
		this.path = path;
		this.fc = fc;
		this.provider = provider;
		this.populatedRanges = populatedRanges;
		this.activeRequests = activeRequests;
		this.openFileHandleCount = new AtomicInteger();
		this.state = new AtomicReference<>(State.UNMODIFIED);
		this.lastModified = initialLastModified;
		this.readAheadBytes = readAheadBytes;
	}

	/**
	 * Creates a cached representation of a file. File contents are loaded on demand from the provided cloud provider.
	 *
	 * @param path        The path of this file in the cloud
	 * @param tmpFilePath Where to store the volatile cache
	 * @param provider    The cloud provider used to load and persist file contents
	 * @param initialSize Must be 0 for newly created files. (Use {@link #truncate(long)} if you want to grow it)
	 * @return The created file
	 * @throws IOException I/O errors during creation of the cache file located at <code>tmpFilePath</code>
	 */
	public static OpenFile create(CloudPath path, Path tmpFilePath, CloudProvider provider, long initialSize, int readAheadBytes) throws IOException {
		var fc = AsynchronousFileChannel.open(tmpFilePath, READ, WRITE, CREATE_NEW, SPARSE, DELETE_ON_CLOSE);
		if (initialSize > 0) {
			try {
				fc.write(ByteBuffer.allocateDirect(1), initialSize - 1).get(); // grow file to initialSize
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new InterruptedIOException();
			} catch (ExecutionException e) {
				throw new IOException("Failed to create file", e);
			}
		}
		return new OpenFile(path, new CompletableAsynchronousFileChannel(fc), provider, TreeRangeSet.create(), TreeRangeMap.create(), Instant.now(), readAheadBytes);
	}

	public AtomicInteger getOpenFileHandleCount() {
		return openFileHandleCount;
	}

	public State getState() {
		return state.get();
	}

	public boolean transitionToUploading() {
		return state.compareAndSet(State.NEEDS_UPLOAD, State.UPLOADING);
	}

	public boolean transitionToUnmodified() {
		return state.compareAndSet(State.UPLOADING, State.UNMODIFIED);
	}

	public boolean transitionToReuploading() {
		return state.compareAndSet(State.NEEDS_REUPLOAD, State.UPLOADING);
	}

	public CloudPath getPath() {
		return path;
	}

	private void markDirty() {
		state.updateAndGet(currentState -> {
			switch (currentState) {
				case UNMODIFIED:
				case NEEDS_UPLOAD:
					return State.NEEDS_UPLOAD;
				case UPLOADING:
				case NEEDS_REUPLOAD:
					return State.NEEDS_REUPLOAD;
				default:
					throw new IllegalStateException("Unsupported state");
			}
		});
	}

	// must only be modified when having a path lock for "newPath"
	public void setPath(CloudPath newPath) {
		this.path = newPath;
	}

	/**
	 * Gets the total size of this file.
	 * The size is set during creation of the file and only modified by {@link #truncate(long)} and {@link #write(Pointer, long, long)}.
	 *
	 * @return The current size of the cached file.
	 */
	public long getSize() {
		Preconditions.checkState(fc.isOpen(), "fc not open for " + path);
		try {
			return fc.size();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public Instant getLastModified() {
		return lastModified;
	}

	public void setLastModified(Instant newLastModified) {
		this.lastModified = newLastModified;
	}

	@Override
	public synchronized void close() {
		try {
			LOG.trace("Closing {}", path);
			fc.close();
		} catch (IOException e) {
			LOG.error("Failed to close tmp file.", e);
		}
	}

	/**
	 * Reads up to {@code size} bytes beginning at {@code offset} into {@code buf}.
	 *
	 * @param buf    Buffer
	 * @param offset Position of first byte to read
	 * @param count  Number of bytes to read
	 * @return A CompletionStage either containing the actual number of bytes read (can be less than {@code size} if reached EOF)
	 * or failing with an {@link IOException}
	 */
	public CompletionStage<Integer> read(Pointer buf, long offset, long count) {
		Preconditions.checkState(fc.isOpen());
		if (offset >= getSize()) {
			// reads starting beyond EOF are no-op
			return CompletableFuture.completedFuture(0);
		}
		return load(offset, count).thenCompose(ignored -> fc.readToPointer(buf, offset, count));
	}

	/**
	 * Writes up to {@code size} bytes beginning at {@code offset} from {@code buf} to this file.
	 *
	 * @param buf    Buffer
	 * @param offset Position of first byte to write
	 * @param count  Number of bytes to write
	 * @return A CompletionStage either containing the actual number of bytes written or failing with an {@link IOException}
	 */
	public CompletableFuture<Integer> write(Pointer buf, long offset, long count) {
		Preconditions.checkState(fc.isOpen());
		markDirty();
		setLastModified(Instant.now().truncatedTo(ChronoUnit.SECONDS));
		markPopulatedIfGrowing(offset);
		return fc.writeFromPointer(buf, offset, count).thenApply(written -> {
			synchronized (this) {
				populatedRanges.add(Range.closedOpen(offset, offset + written));
			}
			return written;
		});
	}

	/**
	 * Loads content into the cache file (if necessary) and provides access to the file channel that will then contain
	 * the requested content, so it can be consumed via {@link FileChannel#transferTo(long, long, WritableByteChannel)}.
	 *
	 * @param offset First byte to read (inclusive), which must not exceed the file's size
	 * @param count  Number of bytes to load
	 */
	// visible for testing
	CompletionStage<Void> load(long offset, long count) {
		Preconditions.checkArgument(offset >= 0);
		Preconditions.checkArgument(count >= 0);
		Preconditions.checkState(fc.isOpen());
		if (count == 0) { // nothing to load
			return CompletableFuture.completedFuture(null);
		}
		try {
			var size = fc.size();
			if (offset >= size) {
				throw new IllegalArgumentException("offset beyond EOF");
			}
			var requiredLastByte = Math.min(size, offset + count); // reads not behind eof (lastByte is exclusive!)
			var requiredRange = Range.closedOpen(offset, requiredLastByte);
			synchronized (this) {
				if (requiredRange.isEmpty() || populatedRanges.encloses(requiredRange)) {
					return CompletableFuture.completedFuture(null);
				} else {
					var desiredCount = Math.max(count, readAheadBytes); // reads at least the readahead
					var desiredLastByte = Math.min(size, offset + desiredCount); // reads not behind eof (lastByte is exclusive!)
					var desiredRange = Range.closedOpen(offset, desiredLastByte);

					var activeRanges = ImmutableRangeSet.copyOf(activeRequests.asMapOfRanges().keySet());
					var missingRanges = ImmutableRangeSet.of(desiredRange).difference(populatedRanges).difference(activeRanges);

					var relevantRequests = new HashSet<>(activeRequests.subRangeMap(desiredRange).asMapOfRanges().values());

					for (var range : missingRanges.asRanges()) {
						var request = loadMissing(range);
						relevantRequests.add(request);
					}

					return CompletableFuture.allOf(relevantRequests.stream().map(CompletionStage::toCompletableFuture).toArray(CompletableFuture[]::new));
				}
			}
		} catch (IOException e) {
			return CompletableFuture.failedFuture(e);
		}
	}

	private CompletionStage<Void> loadMissing(Range<Long> requestedRange) {
		assert !populatedRanges.intersects(requestedRange); // synchronized by caller
		assert activeRequests.subRangeMap(requestedRange).asMapOfRanges().isEmpty(); // synchronized by caller
		long offset = requestedRange.lowerEndpoint();
		long size = requestedRange.upperEndpoint() - requestedRange.lowerEndpoint();

		var read = provider.read(path, offset, size, ProgressListener.NO_PROGRESS_AWARE).thenCompose(in -> {
			var mergeTask = mergeData(requestedRange, in);
			return mergeTask.whenComplete((result, exception) -> closeQuietly(in));
		});

		activeRequests.put(requestedRange, read);

		read.whenComplete((result, error) -> completedRequest(requestedRange, read));

		return read;
	}

	// visible for testing
	synchronized void completedRequest(Range<Long> requestedRange, CompletionStage<Void> request) {
		var entry = activeRequests.getEntry(requestedRange.lowerEndpoint());
		// only remove active request if it hasn't been replaced by a broader request
		if (entry.getKey().equals(requestedRange) && entry.getValue().equals(request)) {
			activeRequests.remove(requestedRange);
		}
	}

	/**
	 * Writes data within the given <code>range</code> from <code>source</code> to
	 * this file's FileChannel. Skips already populated ranges.
	 * <p>
	 * After merging, the file channel is fully populated within the given range,
	 * unless hitting EOF on <code>source</code>.
	 *
	 * @param range  Where to place the data within the file channel
	 * @param source An input stream beginning at the first byte of the requested range
	 * @return
	 */
	// visible for testing
	synchronized CompletableFuture<Void> mergeData(Range<Long> range, InputStream source) {
		var missingRanges = ImmutableRangeSet.of(range).difference(populatedRanges).asRanges().iterator();
		return mergeDataInternal(missingRanges, source, range.lowerEndpoint());
	}

	private CompletableFuture<Void> mergeDataInternal(Iterator<Range<Long>> missingRanges, InputStream source, final long sourceOffset) {
		if (!missingRanges.hasNext()) {
			return CompletableFuture.completedFuture(null);
		}
		var range = missingRanges.next();
		Preconditions.checkArgument(sourceOffset <= range.lowerEndpoint());

		// inputstream may contain regions that aren't "missing".
		// therefore we need to "skip" till the begin of our range:
		try {
			long p = sourceOffset;
			while (p < range.lowerEndpoint()) {
				var skipped = source.skip(range.lowerEndpoint() - p);
				if (skipped == 0) {
					throw new EOFException("failed to skip to begin of desired range");
				}
				p += skipped;
			}
			assert p == range.lowerEndpoint();
		} catch (IOException e) {
			return CompletableFuture.failedFuture(e);
		}

		// now transfer contents from inputstream to our file. repeat process for next range, when finished
		long position = range.lowerEndpoint();
		var count = range.upperEndpoint() - range.lowerEndpoint();
		return fc.transferFrom(source, position, count).thenCompose(transferred -> {
			synchronized (this) {
				var populatedRange = Range.closedOpen(position, position + transferred);
				populatedRanges.add(populatedRange);
			}
			return mergeDataInternal(missingRanges, source, position + transferred);
		});
	}

	/**
	 * Grows _or_ shrinks the file to the requested size.
	 *
	 * @param size
	 * @throws IOException
	 */
	public void truncate(long size) throws IOException {
		Preconditions.checkState(fc.isOpen());
		if (size < fc.size()) {
			fc.truncate(size);
			markDirty();
			setLastModified(Instant.now().truncatedTo(ChronoUnit.SECONDS));
		} else if (size > fc.size()) {
			assert size > 0;
			markPopulatedIfGrowing(size);
			fc.write(ByteBuffer.allocateDirect(1), size - 1);
			markDirty();
			setLastModified(Instant.now().truncatedTo(ChronoUnit.SECONDS));
		} else {
			assert size == fc.size();
			// no-op
		}
	}

	/**
	 * When growing a file beyond its current size, mark the grown region as populated.
	 * This prevents unnecessary calls to {@link CloudProvider#read(CloudPath, long, long, ProgressListener) provider.read(...)}.
	 *
	 * @param newSize
	 */
	private void markPopulatedIfGrowing(long newSize) {
		long oldSize = getSize();
		if (newSize > oldSize) {
			synchronized (this) {
				populatedRanges.add(Range.closedOpen(oldSize, newSize));
			}
		}
	}

	/**
	 * Saves a copy of the data contained in this open file to the specified destination path and resets the dirty flag.
	 * If there are any uncached ranges within this file, they'll get loaded before.
	 *
	 * @param destination A path of a non-existing file in an existing directory.
	 * @return A CompletionStage completed as soon as all data is written.
	 */
	public synchronized CompletionStage<Void> persistTo(Path destination) {
		Preconditions.checkState(fc.isOpen());
		long size = getSize();
		return load(0, size).thenCompose(ignored -> {
			WritableByteChannel dst;
			try {
				dst = Files.newByteChannel(destination, CREATE_NEW, WRITE);
			} catch (IOException e) {
				return CompletableFuture.failedFuture(e);
			}
			var transferTask = fc.transferTo(0, size, dst).<Void>thenApply(transferred -> null);
			return transferTask.whenComplete((result, exception) -> closeQuietly(dst));
		});
	}

	private void closeQuietly(Closeable closeable) {
		try {
			closeable.close();
		} catch (IOException e) {
			LOG.error("Failed to close stream", e);
		}
	}
}
