package org.cryptomator.fusecloudaccess;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.cryptomator.cloudaccess.api.CloudItemMetadata;
import org.cryptomator.cloudaccess.api.CloudItemType;
import org.cryptomator.cloudaccess.api.CloudPath;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.ProgressListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.SPARSE;
import static java.nio.file.StandardOpenOption.WRITE;

class OpenFile implements Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(OpenFile.class);
	private static final AtomicLong FILE_HANDLE_GEN = new AtomicLong();

	private final Path tmpFilePath;
	private final FileChannel fc;
	private final CloudProvider provider;
	private final RangeSet<Long> populatedRanges;
	private final AtomicInteger openFileHandles;
	private final Set<Long> handles;
	private CloudPath path;
	private Instant lastModified;
	private boolean dirty;

	// visible for testing
	OpenFile(CloudPath path, Path tmpFilePath, FileChannel fc, CloudProvider provider, RangeSet<Long> populatedRanges, Instant initialLastModified) {
		this.path = path;
		this.tmpFilePath = tmpFilePath;
		this.fc = fc;
		this.provider = provider;
		this.populatedRanges = populatedRanges;
		this.openFileHandles = new AtomicInteger();
		this.lastModified = initialLastModified;
		this.handles = new HashSet<>();
	}

	CloudPath getPath() {
		return path;
	}

	Path getTmpFilePath() {
		return tmpFilePath;
	}

	int opened() {
		return openFileHandles.incrementAndGet();
	}

	int released() {
		return openFileHandles.decrementAndGet();
	}

	Set<Long> getHandles() {
		return handles;
	}

	public static OpenFile create(CloudPath path, Path tmpFilePath, CloudProvider provider, long initialSize, Instant initialLastModified) throws IOException {
		var fc = FileChannel.open(tmpFilePath, READ, WRITE, CREATE_NEW, SPARSE);
		if (initialSize > 0) {
			fc.write(ByteBuffer.allocateDirect(1), initialSize - 1); // grow file to initialSize
		}
		return new OpenFile(path, tmpFilePath, fc, provider, TreeRangeSet.create(), initialLastModified);
	}

	@Override
	public void close() {
		try {
			fc.close();
			Files.delete(tmpFilePath);
		} catch (IOException e) {
			LOG.error("Failed to close tmp file " + tmpFilePath, e);
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
		Preconditions.checkState(fc.isOpen());
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
		Preconditions.checkState(fc.isOpen());
		fc.truncate(size);
		setDirty(true);
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

	CloudItemMetadata getMetadata() {
		Preconditions.checkState(fc.isOpen());
		try {
			return new CloudItemMetadata(path.getFileName().toString(), path, CloudItemType.FILE, Optional.of(lastModified), Optional.of(fc.size()));
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public InputStream asPersistableStream() throws IOException {
		Preconditions.checkState(fc.isOpen());
		fc.position(0);
		return new UnclosableInputStream(Channels.newInputStream(fc));
	}
}
