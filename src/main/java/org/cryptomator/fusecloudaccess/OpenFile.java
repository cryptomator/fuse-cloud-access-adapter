package org.cryptomator.fusecloudaccess;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import jnr.ffi.Pointer;
import org.cryptomator.cloudaccess.api.CloudItemMetadata;
import org.cryptomator.cloudaccess.api.CloudItemType;
import org.cryptomator.cloudaccess.api.CloudPath;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.ProgressListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
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
	private final AtomicInteger openFileHandles;
	private CloudPath path;
	private Instant lastModified;
	private boolean dirty;

	// visible for testing
	OpenFile(CloudPath path, FileChannel fc, CloudProvider provider, RangeSet<Long> populatedRanges, Instant initialLastModified) {
		this.path = path;
		this.fc = fc;
		this.provider = provider;
		this.populatedRanges = populatedRanges;
		this.openFileHandles = new AtomicInteger();
		this.lastModified = initialLastModified;
	}

	public static OpenFile create(CloudPath path, Path tmpFilePath, CloudProvider provider, long initialSize, Instant initialLastModified) throws IOException {
		var fc = FileChannel.open(tmpFilePath, READ, WRITE, CREATE_NEW, SPARSE, DELETE_ON_CLOSE);
		if (initialSize > 0) {
			fc.write(ByteBuffer.allocateDirect(1), initialSize - 1); // grow file to initialSize
		}
		return new OpenFile(path, fc, provider, TreeRangeSet.create(), initialLastModified);
	}

	CloudPath getPath() {
		return path;
	}

	public long getSize() throws IOException {
		Preconditions.checkState(fc.isOpen());
		return fc.size();
	}

	int opened() {
		return openFileHandles.incrementAndGet();
	}

	int released() {
		return openFileHandles.decrementAndGet();
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

	void updatePath(CloudPath newPath) {
		this.path = newPath;
	}

	void updateLastModified(Instant newLastModified) {
		this.lastModified = newLastModified;
	}

	CloudItemMetadata getMetadata() {
		Preconditions.checkState(fc.isOpen());
		try {
			return new CloudItemMetadata(path.getFileName().toString(), path, CloudItemType.FILE, Optional.of(lastModified), Optional.of(fc.size()));
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
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
	CompletionStage<Void> load(long offset, long count) {
		Preconditions.checkArgument(offset >= 0);
		Preconditions.checkArgument(count >= 0);
		Preconditions.checkState(fc.isOpen());
		try {
			var size = fc.size();
			if (offset > size) {
				throw new EOFException("Requested range beyond EOF");
			}
			var upper = Math.min(size, offset + count);
			var range = Range.closedOpen(offset, upper);
			synchronized (populatedRanges) {
				if (range.isEmpty() || populatedRanges.encloses(range)) {
					return CompletableFuture.completedFuture(null);
				} else {
					var missingRanges = ImmutableRangeSet.of(range).difference(populatedRanges);
					return CompletableFuture.allOf(missingRanges.asRanges().stream().map(this::loadMissing).toArray(CompletableFuture[]::new));
				}
			}
		} catch (IOException e) {
			return CompletableFuture.failedFuture(e);
		}
	}

	private CompletionStage<Void> loadMissing(Range<Long> range) {
		assert !populatedRanges.intersects(range); // synchronized by caller
		long offset = range.lowerEndpoint();
		long size = range.upperEndpoint() - range.lowerEndpoint();
		return provider.read(path, offset, size, ProgressListener.NO_PROGRESS_AWARE).thenCompose(in -> {
			try (var ch = Channels.newChannel(in)) {
				long transferred = fc.transferFrom(ch, offset, size);
				var transferredRange = Range.closedOpen(offset, offset + transferred);
				synchronized (populatedRanges) {
					populatedRanges.add(transferredRange);
				}
				return CompletableFuture.completedFuture(null);
			} catch (IOException e) {
				return CompletableFuture.failedFuture(e);
			}
		});
	}

	void truncate(long size) throws IOException {
		Preconditions.checkState(fc.isOpen());
		fc.truncate(size);
		setDirty(true);
	}

	/**
	 * Saves a copy of the data contained in this open file to the specified destination path.
	 *
	 * @param destination A path of a non-existing file in an existing directory.
	 * @throws IOException
	 */
	public synchronized void persistTo(Path destination) throws IOException {
		try (WritableByteChannel dst = Files.newByteChannel(destination, CREATE_NEW, WRITE)) {
			fc.transferTo(0, fc.size(), dst);
		}
	}
}
