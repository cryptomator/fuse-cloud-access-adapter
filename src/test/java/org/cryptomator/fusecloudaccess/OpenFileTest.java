package org.cryptomator.fusecloudaccess;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import jnr.ffi.Pointer;
import org.cryptomator.cloudaccess.api.CloudPath;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.ProgressListener;
import org.cryptomator.cloudaccess.api.exceptions.CloudProviderException;
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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.spi.FileSystemProvider;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public class OpenFileTest {

	private CloudPath file;
	private CloudProvider provider;
	private FileChannel fileChannel;
	private OpenFile openFile;
	private RangeSet<Long> populatedRanges;

	@BeforeEach
	public void setup() throws IOException {
		this.file = Mockito.mock(CloudPath.class, "/path/to/file");
		this.provider = Mockito.mock(CloudProvider.class);
		this.fileChannel = Mockito.mock(FileChannel.class);
		this.populatedRanges = Mockito.mock(RangeSet.class);
		this.openFile = new OpenFile(file, fileChannel, provider, populatedRanges, Instant.EPOCH);
		Mockito.when(fileChannel.size()).thenReturn(100l);
		Mockito.when(fileChannel.isOpen()).thenReturn(true);
	}

	@DisplayName("create new cached file")
	@ParameterizedTest(name = "with initial size={0}")
	@ValueSource(longs = {0l, 1l, 42l})
	public void testCreate(long size, @TempDir Path tmpDir) throws IOException {
		Path tmpFile = tmpDir.resolve("cache.file");
		try (var cachedFile = OpenFile.create(file, tmpFile, provider, size, Instant.EPOCH)) {
			Assertions.assertNotNull(cachedFile);
			Assertions.assertEquals(size, cachedFile.getSize());
		}
		Assertions.assertTrue(Files.notExists(tmpFile));
	}

	@Test
	@DisplayName("test perstistTo(...)")
	public void testPersist(@TempDir Path tmpDir) throws IOException {
		Path tmpFile = tmpDir.resolve("cache.file");
		Path persistentFile = tmpDir.resolve("persistent.file");

		try (var cachedFile = OpenFile.create(file, tmpFile, provider, 100, Instant.EPOCH)) {
			cachedFile.persistTo(persistentFile);
		}

		Assertions.assertTrue(Files.notExists(tmpFile));
		Assertions.assertTrue(Files.exists(persistentFile));
		Assertions.assertEquals(100, Files.size(persistentFile));
	}

	@DisplayName("test I/O error during write(...)")
	@Test
	public void testWriteFailsWithException() throws IOException {
		var e = new IOException("fail");
		var buf = Mockito.mock(Pointer.class);
		Mockito.when(fileChannel.write(Mockito.any(), Mockito.anyLong())).thenThrow(e);

		var thrown = Assertions.assertThrows(IOException.class, () -> {
			openFile.write(buf, 1000l, 42l);
		});

		Assertions.assertEquals(e, thrown);
	}

	@DisplayName("test write(...)")
	@ParameterizedTest(name = "write(buf, 1000, {0})")
	@ValueSource(ints = {0, 100, 1023, 1024, 1025, 10_000})
	public void testWrite(int n) throws IOException {
		var content = new byte[n];
		var written = new byte[n];
		var buf = Mockito.mock(Pointer.class);
		new Random(42l).nextBytes(content);
		Mockito.doAnswer(invocation -> {
			long offset = invocation.getArgument(0);
			byte[] dst = invocation.getArgument(1);
			int idx = invocation.getArgument(2);
			int len = invocation.getArgument(3);
			System.arraycopy(content, (int) offset, dst, idx, len);
			return null;
		}).when(buf).get(Mockito.anyLong(), Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());
		Mockito.doAnswer(invocation -> {
			ByteBuffer source = invocation.getArgument(0);
			long pos = invocation.getArgument(1);
			int count = source.capacity();
			source.get(written, (int) pos - 1000, count);
			return count;
		}).when(fileChannel).write(Mockito.any(), Mockito.anyLong());
		Assumptions.assumeFalse(openFile.isDirty());

		var result = openFile.write(buf, 1000l, n);

		Assertions.assertEquals(n, result);
		Assertions.assertArrayEquals(content, written);
		Assertions.assertTrue(openFile.isDirty());
		Mockito.verify(populatedRanges).add(Range.closedOpen(1000l, 1000l + n));
	}

	@DisplayName("test I/O error during read(...)")
	@Test
	public void testReadFailsWithException() throws IOException {
		var e = new IOException("fail");
		var buf = Mockito.mock(Pointer.class);
		var in = new ByteArrayInputStream(new byte[42]);
		Mockito.when(fileChannel.size()).thenReturn(1042l);
		Mockito.when(provider.read(file, 1000l, 42l, ProgressListener.NO_PROGRESS_AWARE)).thenReturn(CompletableFuture.completedFuture(in));
		Mockito.when(fileChannel.transferTo(Mockito.anyLong(), Mockito.anyLong(), Mockito.any())).thenThrow(e);

		var futureResult = openFile.read(buf, 1000l, 42l);
		var thrown = Assertions.assertThrows(CompletionException.class, () -> {
			Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().join());
		});

		Assertions.assertTrue(futureResult.toCompletableFuture().isCompletedExceptionally());
		Assertions.assertEquals(e, thrown.getCause());
	}

	@Test
	@DisplayName("read to EOF")
	public void testReadToEof() throws IOException {
		var buf = Mockito.mock(Pointer.class);
		var in = new ByteArrayInputStream(new byte[42]);
		Mockito.when(fileChannel.size()).thenReturn(1042l);
		Mockito.when(provider.read(file, 1000l, 42l, ProgressListener.NO_PROGRESS_AWARE)).thenReturn(CompletableFuture.completedFuture(in));
		Mockito.doAnswer(invocation -> {
			WritableByteChannel target = invocation.getArgument(2);
			target.write(ByteBuffer.allocate(50));
			return 50l;
		}).when(fileChannel).transferTo(Mockito.eq(1000l), Mockito.eq(100l), Mockito.any());

		var futureResult = openFile.read(buf, 1000l, 100l);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertEquals(50, result);
		Mockito.verify(buf, Mockito.times(1)).put(Mockito.eq(0l), Mockito.any(byte[].class), Mockito.eq(0), Mockito.eq(50));
	}

	@DisplayName("test read(...)")
	@ParameterizedTest(name = "read(buf, 1000, {0})")
	@ValueSource(ints = {0, 100, 1023, 1024, 1025, 10_000})
	public void testRead(int n) throws IOException {
		var content = new byte[n];
		var transferred = new byte[n];
		var buf = Mockito.mock(Pointer.class);
		var in = new ByteArrayInputStream(content);
		new Random(42l).nextBytes(content);
		Mockito.when(fileChannel.size()).thenReturn(1042l);
		Mockito.when(provider.read(file, 1000l, 42l, ProgressListener.NO_PROGRESS_AWARE)).thenReturn(CompletableFuture.completedFuture(in));
		Mockito.doAnswer(invocation -> {
			long pos = invocation.getArgument(0);
			long count = invocation.getArgument(1);
			WritableByteChannel target = invocation.getArgument(2);
			target.write(ByteBuffer.wrap(content, (int) pos - 1000, (int) count));
			return count;
		}).when(fileChannel).transferTo(Mockito.anyLong(), Mockito.anyLong(), Mockito.any());
		Mockito.doAnswer(invocation -> {
			long offset = invocation.getArgument(0);
			byte[] src = invocation.getArgument(1);
			int idx = invocation.getArgument(2);
			int len = invocation.getArgument(3);
			System.arraycopy(src, idx, transferred, (int) offset, len);
			return null;
		}).when(buf).put(Mockito.anyLong(), Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());

		var futureResult = openFile.read(buf, 1000l, n);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertEquals(n, result);
		Assertions.assertArrayEquals(content, transferred);
	}

	@Test
	@DisplayName("load region [101, 110] which is behind EOF")
	public void testEof() {
		var futureResult = openFile.load(101, 9);
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

		var futureResult = openFile.load(50, 10);
		Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

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

		var futureResult = openFile.load(50, 10);
		Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Mockito.verify(fileChannel).transferFrom(Mockito.any(), Mockito.eq(50l), Mockito.eq(10l));
		Mockito.verify(populatedRanges).add(Range.closedOpen(50l, 59l));
	}

	@Test
	@DisplayName("load region [50, 60] (fail exceptionally due to failed future)")
	public void testLoadFailsA() {
		var e = new CloudProviderException("fail.");
		Mockito.when(provider.read(file, 50, 10, ProgressListener.NO_PROGRESS_AWARE)).thenReturn(CompletableFuture.failedFuture(e));

		var futureResult = openFile.load(50, 10);
		var thrown = Assertions.assertThrows(CloudProviderException.class, () -> {
			Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().join());
		});

		Assertions.assertTrue(futureResult.toCompletableFuture().isCompletedExceptionally());
		Assertions.assertEquals(e, thrown);
	}

	@Test
	@DisplayName("load region [50, 60] (fail exceptionally due to internal I/O error)")
	public void testLoadFailsB() throws IOException {
		var e = new CloudProviderException("fail.");
		var content = new byte[10];
		Mockito.when(provider.read(file, 50, 10, ProgressListener.NO_PROGRESS_AWARE)).thenReturn(CompletableFuture.completedFuture(new ByteArrayInputStream(content)));
		Mockito.when(fileChannel.transferFrom(Mockito.any(), Mockito.eq(50l), Mockito.eq(10l))).thenThrow(e);

		var futureResult = openFile.load(50, 10);
		var thrown = Assertions.assertThrows(CloudProviderException.class, () -> {
			Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().join());
		});

		Assertions.assertTrue(futureResult.toCompletableFuture().isCompletedExceptionally());
		Assertions.assertEquals(e, thrown);
	}

	@Test
	@DisplayName("load region [50, 50] (empty range)")
	public void testLoadEmptyRange() {
		var futureResult = openFile.load(50, 0);
		Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Mockito.verify(provider, Mockito.never()).read(Mockito.any(), Mockito.anyLong(), Mockito.anyLong(), Mockito.any());
	}

	@Test
	@DisplayName("load region [50, 60] (hit)")
	public void testLoadCached() {
		Mockito.when(populatedRanges.encloses(Range.closedOpen(50l, 60l))).thenReturn(true);

		var futureResult = openFile.load(50, 10);
		Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

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

		var futureResult = openFile.load(50, 20);
		Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Mockito.verify(fileChannel).transferFrom(Mockito.any(), Mockito.eq(60l), Mockito.eq(10l));
		Mockito.verify(populatedRanges).add(Range.closedOpen(60l, 70l));
	}

	@DisplayName("truncate(...)")
	@ParameterizedTest(name = "truncate({0})")
	@ValueSource(longs = {0l, 1l, 2l, 3l})
	public void testTruncate(long size) throws IOException {
		openFile.truncate(size);
		Mockito.verify(fileChannel).truncate(size);
	}

}