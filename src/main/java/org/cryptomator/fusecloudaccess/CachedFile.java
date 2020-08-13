package org.cryptomator.fusecloudaccess;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.cryptomator.cloudaccess.api.CloudItemMetadata;
import org.cryptomator.cloudaccess.api.CloudItemType;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.ProgressListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.SPARSE;
import static java.nio.file.StandardOpenOption.WRITE;

public class CachedFile implements Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(CachedFile.class);
	private static final AtomicLong FILE_HANDLE_GEN = new AtomicLong();

	private final FileChannel fc;
	private final CloudProvider provider;
	private final RangeSet<Long> populatedRanges;
	private final Consumer<Path> onClose;
	private final ConcurrentMap<Long, CachedFileHandle> handles;
	private Path path;
	private Instant lastModified;
	private boolean dirty;
	private boolean deleted;

	// visible for testing
	CachedFile(Path path, FileChannel fc, CloudProvider provider, RangeSet<Long> populatedRanges, Instant initialLastModified, Consumer<Path> onClose) {
		this.fc = fc;
		this.provider = provider;
		this.populatedRanges = populatedRanges;
		this.onClose = onClose;
		this.handles = new ConcurrentHashMap<>();
		this.path = path;
		this.lastModified = initialLastModified;
	}

	public static CachedFile create(Path path, Path tmpFilePath, CloudProvider provider, long initialSize, Instant initialLastModified, Consumer<Path> onClose) throws IOException {
		var fc = FileChannel.open(tmpFilePath, READ, WRITE, CREATE_NEW, SPARSE);
		if (initialSize > 0) {
			fc.write(ByteBuffer.allocateDirect(1), initialSize - 1); // grow file to initialSize
		}
		return new CachedFile(path, fc, provider, TreeRangeSet.create(), initialLastModified, onClose);
	}

	@Override
	public void close() {
		try {
			fc.close();
		} catch (IOException e) {
			LOG.error("Failed to close tmp file channel.", e);
		} finally {
			onClose.accept(path);
		}
	}

	public CachedFileHandle openFileHandle() {
		var handleId = FILE_HANDLE_GEN.incrementAndGet();
		var handle = new CachedFileHandle(this, handleId);
		handles.put(handleId, handle);
		return handle;
	}

	public CompletionStage<Void> releaseFileHandle(long fileHandle) {
		handles.remove(fileHandle);
		if (handles.isEmpty()) {
			if (deleted || !dirty) {
				close();
				return CompletableFuture.completedFuture(null);
			}
			try {
				assert dirty && !deleted;
				LOG.debug("uploading {}...", path);
				fc.position(0);
				// TODO Performance: schedule upload on background task, return immediately, save file contents in "lost+found" dir on error
				return provider.write(path, true, Channels.newInputStream(fc), ProgressListener.NO_PROGRESS_AWARE).thenRun(() -> {
					LOG.debug("uploaded {}", path);
					close();
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
		this.lastModified = Instant.now();
		this.dirty = true;
	}

	boolean isDirty() {
		return dirty;
	}

	/**
	 * Prevents the cached data from being persisted on {@link #releaseFileHandle(long)}.
	 */
	void markDeleted() {
		this.deleted = true;
	}

	void updatePath(Path newPath) {
		this.path = newPath;
	}

	CloudItemMetadata getMetadata() {
		try {
			return new CloudItemMetadata(path.getFileName().toString(), path, CloudItemType.FILE, Optional.of(lastModified), Optional.of(fc.size()));
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
