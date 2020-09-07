package org.cryptomator.fusecloudaccess;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import jnr.ffi.Pointer;
import org.cryptomator.cloudaccess.api.CloudPath;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.exceptions.CloudProviderException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

public class OpenFileTest {

	private CloudPath file;
	private CloudProvider provider;
	private AsynchronousFileChannel fileChannel;
	private OpenFile openFile;
	private RangeSet<Long> populatedRanges;

	@BeforeEach
	public void setup() throws IOException {
		this.file = Mockito.mock(CloudPath.class, "/path/to/file");
		this.provider = Mockito.mock(CloudProvider.class);
		this.fileChannel = Mockito.mock(AsynchronousFileChannel.class);
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

		try (var cachedFile = OpenFile.create(file, tmpFile, provider, 0, Instant.EPOCH)) {
			cachedFile.truncate(100l);
			cachedFile.persistTo(persistentFile);
		}

		Assertions.assertTrue(Files.notExists(tmpFile));
		Assertions.assertTrue(Files.exists(persistentFile));
		Assertions.assertEquals(100, Files.size(persistentFile));
	}

	@Nested
	@DisplayName("write(...)")
	public class Write {

		@DisplayName("fail due to I/O error")
		@Test
		public void testWriteFailsWithException() throws IOException {
			var e = new IOException("fail");
			CompletableFuture<Integer> failedFuture = CompletableFuture.failedFuture(e);
			var buf = Mockito.mock(Pointer.class);
			Mockito.when(fileChannel.write(Mockito.any(), Mockito.anyLong())).thenReturn(failedFuture);

			var thrown = Assertions.assertThrows(IOException.class, () -> {
				openFile.write(buf, 1000l, 42l);
			});

			Assertions.assertEquals(e, thrown);
		}

		@DisplayName("successful write")
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
				return CompletableFuture.completedFuture(count);
			}).when(fileChannel).write(Mockito.any(), Mockito.anyLong());
			Assumptions.assumeFalse(openFile.isDirty());

			var result = openFile.write(buf, 1000l, n);

			Assertions.assertEquals(n, result);
			Assertions.assertArrayEquals(content, written);
			Assertions.assertTrue(openFile.isDirty());
			Mockito.verify(populatedRanges).add(Range.closedOpen(1000l, 1000l + n));
		}

	}

	@Nested
	@DisplayName("read(...)")
	public class Read {

		OpenFile fileSpy;

		@BeforeEach
		void setup() {
			fileSpy = Mockito.spy(openFile);
			Mockito.doReturn(CompletableFuture.completedFuture(null)).when(fileSpy).load(Mockito.anyLong(), Mockito.anyLong());
		}

		@DisplayName("no-op read (offset >= EOF)")
		@ParameterizedTest(name = "read(buf, {0}, 10)")
		@ValueSource(longs = {100l, 101l})
		public void testReadBeyondEof(long offset) throws IOException {
			Mockito.when(fileChannel.size()).thenReturn(100l);
			var buf = Mockito.mock(Pointer.class);

			var futureResult = fileSpy.read(buf, offset, 10);
			var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

			Assertions.assertEquals(0, result);
			Mockito.verify(fileSpy, Mockito.never()).load(Mockito.anyLong(), Mockito.anyLong());
		}

		@DisplayName("successful read")
		@ParameterizedTest(name = "read(buf, 100, {0})")
		@ValueSource(ints = {0, 100, 1023, 1024, 1025, 10_000})
		public void testReadSuccess(int readSize) throws IOException {
			Mockito.when(fileChannel.size()).thenReturn(100_000l);
			long readOffset = 100;
			var content = new byte[readSize];
			var transferred = new byte[readSize];
			var buf = Mockito.mock(Pointer.class);
			new Random(42l).nextBytes(content);
			var interestingByte = readSize !=0 ? content[readSize-1]: -1;
			Mockito.doAnswer(invocation -> {
				ByteBuffer out = invocation.getArgument(0);
				long position = invocation.getArgument(1);
				int bytesRead = out.limit();
				out.put(content,(int) (position-readOffset),bytesRead);
				return CompletableFuture.completedFuture(bytesRead);
			}).when(fileChannel).read(Mockito.any(), Mockito.anyLong());
			Mockito.doAnswer(invocation -> {
				long offset = invocation.getArgument(0);
				byte[] src = invocation.getArgument(1);
				int idx = invocation.getArgument(2);
				int len = invocation.getArgument(3);
				System.arraycopy(src, idx, transferred, (int) offset, len);
				return null;
			}).when(buf).put(Mockito.anyLong(), Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());

			var futureResult = fileSpy.read(buf, readOffset, readSize);
			var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

			Assertions.assertEquals(readSize, result);
			Assertions.assertFalse(fileSpy.isDirty());
			Assertions.assertArrayEquals(content, transferred);
			Mockito.verify(fileSpy).load(readOffset, readSize);
		}

		@Test
		@DisplayName("fail due to I/O error in tmp file channel")
		public void testReadFailure() throws IOException {
			var e = new IOException();
			CompletableFuture failure = CompletableFuture.failedFuture(e);
			Mockito.when(fileChannel.read(Mockito.any(), Mockito.anyLong() )).thenReturn(failure);

			var futureResult = fileSpy.read(Mockito.mock(Pointer.class), 42, 1024);
			var thrown = Assertions.assertThrows(ExecutionException.class, () -> {
				Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());
			});

			Assertions.assertEquals(e, thrown.getCause());
			Mockito.verify(fileSpy).load(42, 1024);
		}

		@Test
		@DisplayName("fail due to load() error")
		public void testLoadFailure() {
			var e = new Exception();
			Mockito.doReturn(CompletableFuture.failedFuture(e)).when(fileSpy).load(Mockito.anyLong(), Mockito.anyLong());

			var futureResult = fileSpy.read(Mockito.mock(Pointer.class), 42, 1024);
			var thrown = Assertions.assertThrows(ExecutionException.class, () -> {
				Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());
			});

			Assertions.assertEquals(e, thrown.getCause());
			Mockito.verify(fileSpy).load(42, 1024);
		}

	}

	@Nested
	@DisplayName("load(...)")
	public class Load {

		OpenFile fileSpy;

		@BeforeEach
		public void setup() throws IOException {
			this.fileSpy = Mockito.spy(openFile);
			Mockito.doNothing().when(fileSpy).mergeData(Mockito.any(), Mockito.any());
		}

		@DisplayName("load 0 bytes")
		@ParameterizedTest(name = "region [{0}, ...]")
		@ValueSource(longs = {0l, 1l, 99l, 100l, 101l})
		public void testLoadZero(long offset) throws IOException {
			Mockito.when(fileChannel.size()).thenReturn(100l);

			var futureResult = fileSpy.load(offset, 0);
			Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

			Mockito.verify(fileSpy, Mockito.never()).mergeData(Mockito.any(), Mockito.any());
		}

		@DisplayName("region behind at EOF (100)")
		@ParameterizedTest(name = "region [{0}, ...]")
		@ValueSource(longs = {100l, 101l})
		public void testLoadFailsAfterEof(long offset) throws IOException {
			Mockito.when(fileChannel.size()).thenReturn(100l);

			Assertions.assertThrows(IllegalArgumentException.class, () -> fileSpy.load(offset, 10));

			Mockito.verify(fileSpy, Mockito.never()).mergeData(Mockito.any(), Mockito.any());
		}

		@Test
		@DisplayName("region [90, 110] till EOF (100)")
		public void testLoadToEof() throws IOException {
			var inputStream = Mockito.mock(InputStream.class);
			Mockito.when(fileChannel.size()).thenReturn(100l);
			Mockito.when(populatedRanges.encloses(Range.closedOpen(90l, 100l))).thenReturn(false);
			Mockito.when(provider.read(Mockito.eq(file), Mockito.eq(90l), Mockito.anyLong(), Mockito.any())).thenReturn(CompletableFuture.completedFuture(inputStream));

			var futureResult = fileSpy.load(90l, 20l);
			Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

			Mockito.verify(fileSpy).mergeData(Mockito.argThat(r -> r.lowerEndpoint() == 90l && r.upperEndpoint() >= 100), Mockito.eq(inputStream));
		}

		@Test
		@DisplayName("region [50, 60] (miss)")
		public void testLoadNonExisting() throws IOException {
			var inputStream = Mockito.mock(InputStream.class);
			Mockito.when(populatedRanges.encloses(Range.closedOpen(50l, 60l))).thenReturn(false);
			Mockito.when(provider.read(Mockito.eq(file), Mockito.eq(50l), Mockito.anyLong(), Mockito.any())).thenReturn(CompletableFuture.completedFuture(inputStream));

			var futureResult = fileSpy.load(50l, 10l);
			Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

			Mockito.verify(fileSpy).mergeData(Mockito.argThat(r -> r.lowerEndpoint() == 50l && r.upperEndpoint() >= 60), Mockito.eq(inputStream));
		}

		@Test
		@DisplayName("region [50, 60] (fail exceptionally due to failed future)")
		public void testLoadFailsA() {
			var e = new CloudProviderException("fail.");
			Mockito.when(provider.read(Mockito.eq(file), Mockito.eq(50l), Mockito.anyLong(), Mockito.any())).thenReturn(CompletableFuture.failedFuture(e));

			var futureResult = fileSpy.load(50, 10);
			var thrown = Assertions.assertThrows(CloudProviderException.class, () -> {
				Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().join());
			});

			Assertions.assertTrue(futureResult.toCompletableFuture().isCompletedExceptionally());
			Assertions.assertEquals(e, thrown);
		}

		@Test
		@DisplayName("region [50, 60] (fail exceptionally due to failed merge)")
		public void testLoadFailsB() throws IOException {
			var e = new IOException("fail.");
			Mockito.when(provider.read(Mockito.eq(file), Mockito.eq(50l), Mockito.anyLong(), Mockito.any())).thenReturn(CompletableFuture.completedFuture(Mockito.mock(InputStream.class)));
			Mockito.doThrow(e).when(fileSpy).mergeData(Mockito.any(), Mockito.any());

			var futureResult = fileSpy.load(50, 10);
			var thrown = Assertions.assertThrows(ExecutionException.class, () -> {
				Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());
			});

			Assertions.assertTrue(futureResult.toCompletableFuture().isCompletedExceptionally());
			Assertions.assertEquals(e, thrown.getCause());
		}

		@Test
		@DisplayName("region [50, 50] (empty range)")
		public void testLoadEmptyRange() throws IOException {
			var futureResult = fileSpy.load(50, 0);
			Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

			Mockito.verify(fileSpy, Mockito.never()).mergeData(Mockito.any(), Mockito.any());
			Mockito.verify(provider, Mockito.never()).read(Mockito.any(), Mockito.anyLong(), Mockito.anyLong(), Mockito.any());
		}

		@Test
		@DisplayName("region [50, 60] (hit)")
		public void testLoadCached() throws IOException {
			Mockito.when(populatedRanges.encloses(Range.closedOpen(50l, 60l))).thenReturn(true);

			var futureResult = fileSpy.load(50, 10);
			Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

			Mockito.verify(fileSpy, Mockito.never()).mergeData(Mockito.any(), Mockito.any());
			Mockito.verify(provider, Mockito.never()).read(Mockito.any(), Mockito.anyLong(), Mockito.anyLong(), Mockito.any());
		}

		@Test
		@DisplayName("region [50, 70] (partial hit)")
		public void testLoadPartiallyCached() throws IOException {
			var inputStream = Mockito.mock(InputStream.class);
			Mockito.when(populatedRanges.encloses(Range.closedOpen(50l, 70l))).thenReturn(false);
			Mockito.when(populatedRanges.asRanges()).thenReturn(Set.of(Range.closedOpen(50l, 60l)));
			Mockito.when(provider.read(Mockito.eq(file), Mockito.eq(60l), Mockito.anyLong(), Mockito.any())).thenReturn(CompletableFuture.completedFuture(inputStream));

			var futureResult = fileSpy.load(50, 20);
			Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());
			Mockito.verify(fileSpy).mergeData(Mockito.argThat(r -> r.lowerEndpoint() == 60l && r.upperEndpoint() >= 70), Mockito.eq(inputStream));
		}

	}

	@Nested
	@DisplayName("truncate(...)")
	public class Truncate {

		@BeforeEach
		public void setup() {
			Assumptions.assumeTrue(openFile.getSize() == 100l);
			Assumptions.assumeFalse(openFile.isDirty());
		}

		@DisplayName("shrinking (new size < 100)")
		@ParameterizedTest(name = "truncate({0})")
		@ValueSource(longs = {0l, 1l, 99l})
		public void testShrink(long size) throws IOException {
			openFile.truncate(size);

			Mockito.verify(fileChannel).truncate(size);
			Mockito.verify(fileChannel, Mockito.never()).write(Mockito.any(), Mockito.anyLong());
			Assertions.assertTrue(openFile.isDirty());
		}

		@DisplayName("growing (new size > 100)")
		@ParameterizedTest(name = "truncate({0})")
		@ValueSource(longs = {101l, 150l})
		public void testGrow(long size) throws IOException {
			openFile.truncate(size);

			Mockito.verify(fileChannel, Mockito.never()).truncate(Mockito.anyLong());
			Mockito.verify(fileChannel).write(Mockito.argThat(b -> b.remaining() == 1), Mockito.eq(size - 1));
			Assertions.assertTrue(openFile.isDirty());
		}

		@Test
		@DisplayName("no-op (new size == 100)")
		public void testNoop() throws IOException {
			openFile.truncate(100l);

			Mockito.verify(fileChannel, Mockito.never()).truncate(Mockito.anyLong());
			Mockito.verify(fileChannel, Mockito.never()).write(Mockito.any(), Mockito.anyLong());
			Assertions.assertFalse(openFile.isDirty());
		}

	}

	@Nested
	@DisplayName("merge(...)")
	public class Merge {

		private InputStream in = Mockito.mock(InputStream.class);
		private OpenFile fileSpy;

		@BeforeEach
		public void setup() {
			var prePopulatedRanges = ImmutableRangeSet.of(Range.closedOpen(0l, 50l));
			populatedRanges = Mockito.spy(TreeRangeSet.create(prePopulatedRanges));
			openFile = new OpenFile(file, fileChannel, provider, populatedRanges, Instant.EPOCH);
			this.fileSpy = Mockito.spy(openFile);
		}

		@Test
		@DisplayName("not intersecting populated ranges")
		public void testMergeFullRange() throws IOException {
			var range = Range.closedOpen(100l, 120l);
			Assumptions.assumeFalse(populatedRanges.encloses(range));
			//Mockito.when(fileChannel.transferFrom(Mockito.any(), Mockito.eq(100l), Mockito.eq(20l))).thenReturn(20l);
			Mockito.doReturn(20l)
					.when(fileSpy).transferSingleMissingRange(Mockito.any(),Mockito.eq(100l), Mockito.eq(20l));

			fileSpy.mergeData(range, in);

			Mockito.verify(populatedRanges).add(Range.closedOpen(100l, 120l));
			Assertions.assertTrue(populatedRanges.encloses(range));
		}

		@Test
		@DisplayName("partially populated (in between)")
		public void testMergePartiallyPopulatedRange1() throws IOException {
			var range = Range.closedOpen(100l, 150l);
			populatedRanges.add(Range.closedOpen(110l, 120l));
			Assumptions.assumeFalse(populatedRanges.encloses(range));
			Mockito.doReturn(10l)
					.when(fileSpy).transferSingleMissingRange(Mockito.any(),Mockito.eq(100l), Mockito.eq(10l));
			Mockito.doReturn(30l)
					.when(fileSpy).transferSingleMissingRange(Mockito.any(),Mockito.eq(120l), Mockito.eq(30l));

			fileSpy.mergeData(range, in);

			Mockito.verify(populatedRanges).add(Range.closedOpen(100l, 110l));
			Mockito.verify(populatedRanges).add(Range.closedOpen(120l, 150l));
			Assertions.assertTrue(populatedRanges.encloses(range));
		}

		@Test
		@DisplayName("partially populated (at begin)")
		public void testMergePartiallyPopulatedRange2() throws IOException {
			var range = Range.closedOpen(0l, 100l);
			Assumptions.assumeFalse(populatedRanges.encloses(range));
			Mockito.doReturn(50l)
					.when(fileSpy).transferSingleMissingRange(Mockito.any(),Mockito.eq(50l), Mockito.eq(50l));

			fileSpy.mergeData(range, in);

			Mockito.verify(populatedRanges).add(Range.closedOpen(50l, 100l));
			Assertions.assertTrue(populatedRanges.encloses(range));
		}

		@Test
		@DisplayName("fully populated")
		public void testMergeFullyPopulatedRange() throws IOException {
			var range = Range.closedOpen(10l, 20l);
			Assumptions.assumeTrue(populatedRanges.encloses(range));

			fileSpy.mergeData(range, in);

			Mockito.verify(populatedRanges, Mockito.never()).add(Mockito.any());
			Assertions.assertTrue(populatedRanges.encloses(range));
		}

		@Test
		@DisplayName("reaching EOF")
		public void testMergeWithEOF() throws IOException {
			var range = Range.closedOpen(100l, 120l);
			Assumptions.assumeFalse(populatedRanges.encloses(range));
			Mockito.doReturn(10l)
					.when(fileSpy).transferSingleMissingRange(Mockito.any(),Mockito.eq(100l), Mockito.eq(20l));

			fileSpy.mergeData(range, in);

			Mockito.verify(populatedRanges).add(Range.closedOpen(100l, 110l));
			Assertions.assertFalse(populatedRanges.encloses(range));
			Assertions.assertTrue(populatedRanges.intersects(range));
		}

		@Test
		@DisplayName("empty range")
		public void testMergeEmptyRange() throws IOException {
			var range = Range.closedOpen(100l, 100l);
			Assumptions.assumeTrue(range.isEmpty());

			fileSpy.mergeData(Range.closedOpen(100l, 100l), in);

			Mockito.verify(populatedRanges, Mockito.never()).add(Mockito.any());
		}

	}
}