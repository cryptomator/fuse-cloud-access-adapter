package org.cryptomator.fusecloudaccess;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import org.cryptomator.cloudaccess.api.CloudPath;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.ProgressListener;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public class CachedFileTest {

	private CloudPath file;
	private CloudProvider provider;
	private FileChannel fileChannel;
	private CachedFile cachedFile;
	private RangeSet<Long> populatedRanges;
	private Consumer<CloudPath> onClose;

	@BeforeEach
	public void setup() throws IOException {
		this.file = Mockito.mock(CloudPath.class, "/path/to/file");
		this.provider = Mockito.mock(CloudProvider.class);
		this.fileChannel = Mockito.mock(FileChannel.class);
		this.populatedRanges = Mockito.mock(RangeSet.class);
		this.onClose = Mockito.mock(Consumer.class);
		this.cachedFile = new CachedFile(file, fileChannel, provider, populatedRanges, Instant.EPOCH, onClose);
		Mockito.when(fileChannel.size()).thenReturn(100l);
	}

	@DisplayName("create new cached file")
	@ParameterizedTest(name = "with initial size={0}")
	@ValueSource(longs = {0l, 1l, 42l})
	public void testCreate(long size, @TempDir Path tmpDir) throws IOException {
		Path tmpFile = tmpDir.resolve("cache.file");
		try (var cachedFile = CachedFile.create(file, tmpFile, provider, size, Instant.EPOCH, onClose)) {
			Assertions.assertNotNull(cachedFile);
			Assertions.assertEquals(size, Files.size(tmpFile));
		}
	}

	@Test
	@DisplayName("load region [101, 110] which is behind EOF")
	public void testEof() {
		var futureResult = cachedFile.load(101, 9);
		Assertions.assertTrue(futureResult.toCompletableFuture().isCompletedExceptionally());
	}

	@Test
	@DisplayName("load region [50, 60] (miss)")
	public void testLoadNonExisting() throws IOException {
		Mockito.when(populatedRanges.encloses(Range.closedOpen(50l, 60l))).thenReturn(false);
		byte[] content = new byte[10];
		Arrays.fill(content, (byte) 0x33);
		Mockito.when(provider.read(file, 50, 10, ProgressListener.NO_PROGRESS_AWARE)).thenReturn(CompletableFuture.completedFuture(new ByteArrayInputStream(content)));
		Mockito.when(fileChannel.transferFrom(Mockito.any(), Mockito.eq(50l), Mockito.eq(10l))).thenReturn(10l);

		var futureResult = cachedFile.load(50, 10);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertEquals(result, fileChannel);
		Mockito.verify(fileChannel).transferFrom(Mockito.any(), Mockito.eq(50l), Mockito.eq(10l));
		Mockito.verify(populatedRanges).add(Range.closedOpen(50l, 60l));
	}

	@Test
	@DisplayName("load region [50, 60] (miss, hitting remote EOF)")
	public void testLoadNonExistingReachingEof() throws IOException {
		Mockito.when(populatedRanges.encloses(Range.closedOpen(50l, 60l))).thenReturn(false);
		byte[] content = new byte[9];
		Arrays.fill(content, (byte) 0x33);
		Mockito.when(provider.read(file, 50, 10, ProgressListener.NO_PROGRESS_AWARE)).thenReturn(CompletableFuture.completedFuture(new ByteArrayInputStream(content)));
		Mockito.when(fileChannel.transferFrom(Mockito.any(), Mockito.eq(50l), Mockito.eq(10l))).thenReturn(9l);

		var futureResult = cachedFile.load(50, 10);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertEquals(result, fileChannel);
		Mockito.verify(fileChannel).transferFrom(Mockito.any(), Mockito.eq(50l), Mockito.eq(10l));
		Mockito.verify(populatedRanges).add(Range.closedOpen(50l, 59l));
	}

	@Test
	@DisplayName("load region [50, 60] (fail exceptionally due to failed future)")
	public void testLoadFailsA() {
		var e = new IOException("fail.");
		Mockito.when(provider.read(file, 50, 10, ProgressListener.NO_PROGRESS_AWARE)).thenReturn(CompletableFuture.failedFuture(e));

		var futureResult = cachedFile.load(50, 10);
		var ee = Assertions.assertThrows(ExecutionException.class, () -> {
			Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());
		});

		Assertions.assertTrue(futureResult.toCompletableFuture().isCompletedExceptionally());
		Assertions.assertEquals(e, ee.getCause());
	}

	@Test
	@DisplayName("load region [50, 60] (fail exceptionally due to internal I/O error)")
	public void testLoadFailsB() throws IOException {
		var e = new IOException("fail.");
		var content = new byte[10];
		Mockito.when(provider.read(file, 50, 10, ProgressListener.NO_PROGRESS_AWARE)).thenReturn(CompletableFuture.completedFuture(new ByteArrayInputStream(content)));
		Mockito.when(fileChannel.transferFrom(Mockito.any(), Mockito.eq(50l), Mockito.eq(10l))).thenThrow(e);

		var futureResult = cachedFile.load(50, 10);
		var ee = Assertions.assertThrows(ExecutionException.class, () -> {
			Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());
		});

		Assertions.assertTrue(futureResult.toCompletableFuture().isCompletedExceptionally());
		Assertions.assertEquals(e, ee.getCause());
	}

	@Test
	@DisplayName("load region [50, 50] (empty range)")
	public void testLoadEmptyRange() {
		var futureResult = cachedFile.load(50, 0);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertEquals(result, fileChannel);
		Mockito.verify(provider, Mockito.never()).read(Mockito.any(), Mockito.anyLong(), Mockito.anyLong(), Mockito.any());
	}

	@Test
	@DisplayName("load region [50, 60] (hit)")
	public void testLoadCached() {
		Mockito.when(populatedRanges.encloses(Range.closedOpen(50l, 60l))).thenReturn(true);

		var futureResult = cachedFile.load(50, 10);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertEquals(result, fileChannel);
		Mockito.verify(provider, Mockito.never()).read(Mockito.any(), Mockito.anyLong(), Mockito.anyLong(), Mockito.any());
	}

	@Test
	@DisplayName("load region [50, 70] (partial hit)")
	public void testLoadPartiallyCached() throws IOException {
		Mockito.when(populatedRanges.encloses(Range.closedOpen(50l, 70l))).thenReturn(false);
		Mockito.when(populatedRanges.asRanges()).thenReturn(Set.of(Range.closedOpen(50l, 60l)));
		byte[] content = new byte[10];
		Arrays.fill(content, (byte) 0x55);
		Mockito.when(provider.read(file, 60, 10, ProgressListener.NO_PROGRESS_AWARE)).thenReturn(CompletableFuture.completedFuture(new ByteArrayInputStream(content)));
		Mockito.when(fileChannel.transferFrom(Mockito.any(), Mockito.eq(60l), Mockito.eq(10l))).thenReturn(10l);

		var futureResult = cachedFile.load(50, 20);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertEquals(result, fileChannel);
		Mockito.verify(fileChannel).transferFrom(Mockito.any(), Mockito.eq(60l), Mockito.eq(10l));
		Mockito.verify(populatedRanges).add(Range.closedOpen(60l, 70l));
	}

	@DisplayName("truncate(...)")
	@ParameterizedTest(name = "truncate({0})")
	@ValueSource(longs = {0l, 1l, 2l, 3l})
	public void testTruncate(long size) throws IOException {
		cachedFile.truncate(size);
		Mockito.verify(fileChannel).truncate(size);
	}

	@Test
	@DisplayName("release last open file handle (dirty)")
	public void testReleaseFileHandleDirty() throws IOException {
		var handle = cachedFile.openFileHandle();
		cachedFile.markDirty();
		Assumptions.assumeTrue(cachedFile.isDirty());
		Mockito.when(provider.write(Mockito.eq(file), Mockito.eq(true), Mockito.any(), Mockito.any())).thenReturn(CompletableFuture.completedFuture(null));
		Mockito.doNothing().when(fileChannel).close();

		var futureResult = cachedFile.releaseFileHandle(handle.getId());

		Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());
		Mockito.verify(provider).write(Mockito.eq(file), Mockito.eq(true), Mockito.any(), Mockito.any());
		Mockito.verify(fileChannel).close();
		Mockito.verify(onClose).accept(Mockito.eq(file));
	}

	@Test
	@DisplayName("release last open file handle (non-dirty)")
	public void testReleaseFileHandleNonDirty() throws IOException {
		var handle = cachedFile.openFileHandle();
		Assumptions.assumeFalse(cachedFile.isDirty());
		Mockito.doNothing().when(fileChannel).close();

		var futureResult = cachedFile.releaseFileHandle(handle.getId());

		Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());
		Mockito.verify(provider, Mockito.never()).write(Mockito.any(), Mockito.anyBoolean(), Mockito.any(), Mockito.any());
		Mockito.verify(fileChannel).close();
		Mockito.verify(onClose).accept(Mockito.eq(file));
	}

	@Test
	@DisplayName("release file handle (but not last)")
	public void testReleaseFileHandleNotLast() throws IOException {
		var handle1 = cachedFile.openFileHandle();
		var handle2 = cachedFile.openFileHandle();
		cachedFile.markDirty();
		Assumptions.assumeTrue(cachedFile.isDirty());
		Mockito.doNothing().when(fileChannel).close();

		var futureResult = cachedFile.releaseFileHandle(handle1.getId());

		Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());
		Mockito.verify(provider, Mockito.never()).write(Mockito.any(), Mockito.anyBoolean(), Mockito.any(), Mockito.any());
		Mockito.verify(fileChannel, Mockito.never()).close();
		Mockito.verify(onClose, Mockito.never()).accept(Mockito.any());
	}

}