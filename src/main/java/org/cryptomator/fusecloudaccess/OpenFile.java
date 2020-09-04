package org.cryptomator.fusecloudaccess;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import jnr.ffi.Pointer;
import org.cryptomator.cloudaccess.api.CloudPath;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.ProgressListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.file.StandardOpenOption.*;

class OpenFile implements Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(OpenFile.class);
	private static final int BUFFER_SIZE = 1024; // 1 kiB
	private static final int READAHEAD_SIZE = 1024 * 1024; // 1 MiB

	private final FileChannel fc;
	private final CloudProvider provider;
	private final RangeSet<Long> populatedRanges;
	private final AtomicInteger openFileHandleCount;
	private CloudPath path;
	private Instant lastModified;
	private boolean dirty;

	// visible for testing
	OpenFile(CloudPath path, FileChannel fc, CloudProvider provider, RangeSet<Long> populatedRanges, Instant initialLastModified) {
		this.path = path;
		this.fc = fc;
		this.provider = provider;
		this.populatedRanges = populatedRanges;
		this.openFileHandleCount = new AtomicInteger();
		this.lastModified = initialLastModified;
	}

	/**
	 * Creates a cached representation of a file. File contents are loaded on demand from the provided cloud provider.
	 *
	 * @param path                The path of this file in the cloud
	 * @param tmpFilePath         Where to store the volatile cache
	 * @param provider            The cloud provider used to load and persist file contents
	 * @param initialSize         Must be 0 for newly created files. (Use {@link #truncate(long)} if you want to grow it)
	 * @param initialLastModified The initial modification date to report until further writes happen
	 * @return The created file
	 * @throws IOException I/O errors during creation of the cache file located at <code>tmpFilePath</code>
	 */
	public static OpenFile create(CloudPath path, Path tmpFilePath, CloudProvider provider, long initialSize, Instant initialLastModified) throws IOException {
		var fc = FileChannel.open(tmpFilePath, READ, WRITE, CREATE_NEW, SPARSE, DELETE_ON_CLOSE);
		if (initialSize > 0) {
			fc.write(ByteBuffer.allocateDirect(1), initialSize - 1); // grow file to initialSize
		}
		return new OpenFile(path, fc, provider, TreeRangeSet.create(), initialLastModified);
	}

	public CloudPath getPath() {
		return path;
	}

	AtomicInteger getOpenFileHandleCount() {
		return openFileHandleCount;
	}

	public void updatePath(CloudPath newPath) {
		this.path = newPath;
	}

	/**
	 * Gets the total size of this file.
	 * The size is set during creation of the file and only modified by {@link #truncate(long)} and {@link #write(Pointer, long, long)}.
	 *
	 * @return The current size of the cached file.
	 */
	public long getSize() {
		Preconditions.checkState(fc.isOpen());
		try {
			return fc.size();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	void setDirty(boolean dirty) {
		this.dirty = dirty;
		if (dirty) {
			this.lastModified = Instant.now();
		}
	}

	boolean isDirty() {
		return dirty && fc.isOpen();
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
	 * @param size   Number of bytes to read
	 * @return A CompletionStage either containing the actual number of bytes read (can be less than {@code size} if reached EOF)
	 * or failing with an {@link IOException}
	 */
	public CompletionStage<Integer> read(Pointer buf, long offset, long size) {
		Preconditions.checkState(fc.isOpen());
		if (offset >= getSize()) {
			// reads starting beyond EOF are no-op
			return CompletableFuture.completedFuture(0);
		}
		return load(offset, size).thenCompose(ignored -> {
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
	public int write(Pointer buf, long offset, long size) throws IOException {
		Preconditions.checkState(fc.isOpen());
		assert size < Integer.MAX_VALUE; // technically an unsigned integer in the c header file
		setDirty(true);
		markPopulatedIfGrowing(offset);
		long pos = offset;
		while (pos < offset + size) {
			int n = (int) Math.min(BUFFER_SIZE, size - (pos - offset)); // int-cast: n <= BUFFER_SIZE
			byte[] tmp = new byte[n];
			buf.get(pos - offset, tmp, 0, n);
			pos += fc.write(ByteBuffer.wrap(tmp), pos);
		}
		int written = (int) (pos - offset); // int-cast: result <= size
		synchronized (populatedRanges) {
			populatedRanges.add(Range.closedOpen(offset, offset + written));
		}
		return written;
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
		try {
			var size = fc.size();
			if (offset >= size) {
				throw new EOFException("Requested range beyond EOF");
			}
			var requiredLastByte = Math.min(size, offset + count); // reads not behind eof (lastByte is exclusive!)
			var requiredRange = Range.closedOpen(offset, requiredLastByte);
			synchronized (populatedRanges) {
				if (requiredRange.isEmpty() || populatedRanges.encloses(requiredRange)) {
					return CompletableFuture.completedFuture(null);
				} else {
					var desiredCount = Math.max(count, READAHEAD_SIZE); // reads at least the readahead
					var desiredLastByte = Math.min(size, offset + desiredCount); // reads not behind eof (lastByte is exclusive!)
					var desiredRange = Range.closedOpen(offset, desiredLastByte);
					var missingRanges = ImmutableRangeSet.of(desiredRange).difference(populatedRanges);
					return CompletableFuture.allOf(missingRanges.asRanges().stream().map(this::loadMissing).toArray(CompletableFuture[]::new));
				}
			}
		} catch (IOException e) {
			return CompletableFuture.failedFuture(e);
		}
	}

	private CompletionStage<Void> loadMissing(Range<Long> requestedRange) {
		assert !populatedRanges.intersects(requestedRange); // synchronized by caller
		long offset = requestedRange.lowerEndpoint();
		long size = requestedRange.upperEndpoint() - requestedRange.lowerEndpoint();
		return provider.read(path, offset, size, ProgressListener.NO_PROGRESS_AWARE).thenCompose(inputStream -> {
			try (var in = inputStream) {
				mergeData(requestedRange, in);
				return CompletableFuture.completedFuture(null);
			} catch (IOException e) {
				return CompletableFuture.failedFuture(e);
			}
		});
	}

	/**
	 * Writes data within the given <code>range</code> from <code>source</code> to
	 * this file's FileChannel. Skips already populated ranges.
	 * <p>
	 * After merging, the file channel is fully populated within the given range,
	 * unless hitting EOF on <code>source</code>.
	 *
	 * @param range  Where to place the data within the file channel
	 * @param source The data source
	 * @throws IOException
	 */
	// visible for testing
	void mergeData(Range<Long> range, InputStream source) throws IOException {
		synchronized (populatedRanges) {
			var missingRanges = ImmutableRangeSet.of(range).difference(populatedRanges);
			long idx = range.lowerEndpoint();
			for (var r : missingRanges.asRanges()) { // known to be sorted ascending
				long offset = r.lowerEndpoint();
				long size = r.upperEndpoint() - r.lowerEndpoint();
				source.skip(offset - idx); // skip bytes between missing ranges (i.e. already populated)
				long transferred = fc.transferFrom(Channels.newChannel(source), offset, size);
				var transferredRange = Range.closedOpen(offset, offset + transferred);
				populatedRanges.add(transferredRange);
				idx = r.upperEndpoint();
			}
		}
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
			setDirty(true);
		} else if (size > fc.size()) {
			assert size > 0;
			markPopulatedIfGrowing(size);
			fc.write(ByteBuffer.allocateDirect(1), size - 1);
			setDirty(true);
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
			synchronized (populatedRanges) {
				populatedRanges.add(Range.closedOpen(oldSize, newSize));
			}
		}
	}

	/**
	 * Saves a copy of the data contained in this open file to the specified destination path.
	 * If there are any uncached ranges within this file, they'll get loaded before.
	 *
	 * @param destination A path of a non-existing file in an existing directory.
	 * @return A CompletionStage completed as soon as all data is written.
	 */
	public synchronized CompletionStage<Void> persistTo(Path destination) {
		Preconditions.checkState(fc.isOpen());
		return load(0, getSize()).thenCompose(ignored -> {
			try (WritableByteChannel dst = Files.newByteChannel(destination, CREATE_NEW, WRITE)) {
				fc.transferTo(0, fc.size(), dst);
				return CompletableFuture.completedFuture(null);
			} catch (IOException e) {
				return CompletableFuture.failedFuture(e);
			}
		});
	}
}
