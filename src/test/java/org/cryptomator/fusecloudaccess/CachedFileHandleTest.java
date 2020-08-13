package org.cryptomator.fusecloudaccess;

import jnr.ffi.Pointer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

class CachedFileHandleTest {

	private CachedFile cachedFile;
	private FileChannel fileChannel;
	private Pointer buf;
	private CachedFileHandle handle;

	@BeforeEach
	public void setup() {
		this.cachedFile = Mockito.mock(CachedFile.class);
		this.fileChannel = Mockito.mock(FileChannel.class);
		this.buf = Mockito.mock(Pointer.class);
		this.handle = new CachedFileHandle(cachedFile, 42l);
	}

	@DisplayName("test I/O error during handle.read(...)")
	@Test
	public void testReadFailsWithException() throws IOException {
		var e = new IOException("fail");
		Mockito.when(cachedFile.load(1000l, 42l)).thenReturn(CompletableFuture.completedFuture(fileChannel));
		Mockito.when(fileChannel.transferTo(Mockito.anyLong(), Mockito.anyLong(), Mockito.any())).thenThrow(e);

		var futureResult = handle.read(buf, 1000l, 42l);
		var ee = Assertions.assertThrows(ExecutionException.class, () -> {
			Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());
		});

		Assertions.assertTrue(futureResult.toCompletableFuture().isCompletedExceptionally());
		Assertions.assertEquals(e, ee.getCause());
	}

	@Test
	public void testReadToEof() throws IOException {
		Mockito.when(cachedFile.load(1000l, 100l)).thenReturn(CompletableFuture.completedFuture(fileChannel));
		Mockito.doAnswer(invocation -> {
			WritableByteChannel target = invocation.getArgument(2);
			target.write(ByteBuffer.allocate(50));
			return 50l;
		}).when(fileChannel).transferTo(Mockito.eq(1000l), Mockito.eq(100l), Mockito.any());

		var futureResult = handle.read(buf, 1000l, 100l);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertEquals(50, result);
		Mockito.verify(buf, Mockito.times(1)).put(Mockito.eq(0l), Mockito.any(byte[].class), Mockito.eq(0), Mockito.eq(50));
	}

	@DisplayName("test handle.read(...)")
	@ParameterizedTest(name = "read(buf, 1000, {0})")
	@ValueSource(ints = {0, 100, 1023, 1024, 1025, 10_000})
	public void testRead(int n) throws IOException {
		var content = new byte[n];
		var transferred = new byte[n];
		new Random(42l).nextBytes(content);
		Mockito.when(cachedFile.load(1000l, n)).thenReturn(CompletableFuture.completedFuture(fileChannel));
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

		var futureResult = handle.read(buf, 1000l, n);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertEquals(n, result);
		Assertions.assertArrayEquals(content, transferred);
	}

	@DisplayName("test I/O error during handle.write(...)")
	@Test
	public void testWriteFailsWithException() throws IOException {
		var e = new IOException("fail");
		Mockito.when(cachedFile.load(0, Long.MAX_VALUE)).thenReturn(CompletableFuture.completedFuture(fileChannel));
		Mockito.when(fileChannel.transferFrom(Mockito.any(), Mockito.anyLong(), Mockito.anyLong())).thenThrow(e);

		var futureResult = handle.write(buf, 1000l, 42l);
		var ee = Assertions.assertThrows(ExecutionException.class, () -> {
			Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());
		});

		Assertions.assertTrue(futureResult.toCompletableFuture().isCompletedExceptionally());
		Assertions.assertEquals(e, ee.getCause());
	}

	@DisplayName("test handle.write(...)")
	@ParameterizedTest(name = "write(buf, 1000, {0})")
	@ValueSource(ints = {0, 100, 1023, 1024, 1025, 10_000})
	public void testWrite(int n) throws IOException {
		var content = new byte[n];
		var transferred = new byte[n];
		new Random(42l).nextBytes(content);
		Mockito.when(cachedFile.load(0, Long.MAX_VALUE)).thenReturn(CompletableFuture.completedFuture(fileChannel));
		Mockito.doAnswer(invocation -> {
			long offset = invocation.getArgument(0);
			byte[] dst = invocation.getArgument(1);
			int idx = invocation.getArgument(2);
			int len = invocation.getArgument(3);
			System.arraycopy(content, (int) offset, dst, idx, len);
			return null;
		}).when(buf).get(Mockito.anyLong(), Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());
		Mockito.doAnswer(invocation -> {
			ReadableByteChannel source = invocation.getArgument(0);
			long pos = invocation.getArgument(1);
			long count = invocation.getArgument(2);
			var tmp = ByteBuffer.allocate((int) count);
			source.read(tmp);
			tmp.flip();
			tmp.get(transferred, (int) pos - 1000, (int) count);
			return count;
		}).when(fileChannel).transferFrom(Mockito.any(), Mockito.anyLong(), Mockito.anyLong());

		var futureResult = handle.write(buf, 1000l, n);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertEquals(n, result);
		Assertions.assertArrayEquals(content, transferred);
	}

}