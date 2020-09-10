package org.cryptomator.fusecloudaccess;

import jnr.ffi.Pointer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.WritableByteChannel;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class CompletableAsynchronousFileChannelTest {
	
	private static final int MIB = 1024 * 1024;

	private AsynchronousFileChannel fc;
	private CompletableAsynchronousFileChannel completableFc;

	@BeforeEach
	public void setup() {
		this.fc = Mockito.mock(AsynchronousFileChannel.class);
		this.completableFc = new CompletableAsynchronousFileChannel(fc);
	}

	@Nested
	@DisplayName("readToPointer(...)")
	public class ReadToPointer {

		private Pointer ptr;

		@BeforeEach
		public void setup() {
			completableFc = Mockito.spy(completableFc);
			this.ptr = Mockito.mock(Pointer.class);
		}

		@Test
		@DisplayName("exception during read(...)")
		public void readToPointerFails() {
			var e = new IOException("fail");
			Mockito.doReturn(CompletableFuture.failedFuture(e)).when(completableFc).read(Mockito.any(), Mockito.anyLong());

			var futureResult = completableFc.readToPointer(ptr, 42l, 100l);
			var thrown = Assertions.assertThrows(ExecutionException.class, () -> {
				Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.get());
			});

			Assertions.assertEquals(e, thrown.getCause());
			Mockito.verifyNoMoreInteractions(ptr);
		}

		@Test
		@DisplayName("successful single-pass read")
		public void readToPointerSucceedsInSingleIteration() {
			Mockito.doAnswer(invocation -> {
				ByteBuffer buf = invocation.getArgument(0);
				buf.put(new byte[100]);
				return CompletableFuture.completedFuture(100);
			}).when(completableFc).read(Mockito.any(), Mockito.eq(42l));

			var futureResult = completableFc.readToPointer(ptr, 42l, 100l);
			var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.get());

			Assertions.assertEquals(100, result);
			Mockito.verify(ptr).put(Mockito.eq(0l), Mockito.any(byte[].class), Mockito.eq(0), Mockito.eq(100));
			Mockito.verifyNoMoreInteractions(ptr);
		}

		@Test
		@DisplayName("successful multi-pass read")
		public void readToPointerSucceedsInTwoIteration() {
			Mockito.doAnswer(invocation -> {
				ByteBuffer buf = invocation.getArgument(0);
				buf.put(new byte[4 * MIB]);
				return CompletableFuture.completedFuture(4 * MIB);
			}).when(completableFc).read(Mockito.any(), Mockito.eq(0l));
			Mockito.doAnswer(invocation -> {
				ByteBuffer buf = invocation.getArgument(0);
				buf.put(new byte[2 * MIB]);
				return CompletableFuture.completedFuture(2 * MIB);
			}).when(completableFc).read(Mockito.any(), Mockito.eq(4l * MIB));

			var futureResult = completableFc.readToPointer(ptr, 0l, 6 * MIB);
			var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.get());

			Assertions.assertEquals(6 * MIB, result);
			Mockito.verify(ptr).put(Mockito.eq(0l * MIB), Mockito.any(byte[].class), Mockito.eq(0), Mockito.eq(4 * MIB));
			Mockito.verify(ptr).put(Mockito.eq(4l * MIB), Mockito.any(byte[].class), Mockito.eq(0), Mockito.eq(2 * MIB));
			Mockito.verifyNoMoreInteractions(ptr);
		}

		@Test
		@DisplayName("successful multi-pass till EOF 1")
		public void readToPointerSucceedsInTwoIterationAtEOF1() {
			Mockito.doAnswer(invocation -> {
				ByteBuffer buf = invocation.getArgument(0);
				buf.put(new byte[4 * MIB]);
				return CompletableFuture.completedFuture(4 * MIB);
			}).when(completableFc).read(Mockito.any(), Mockito.eq(0l));
			Mockito.doAnswer(invocation -> {
				ByteBuffer buf = invocation.getArgument(0);
				buf.put(new byte[1 * MIB]);
				return CompletableFuture.completedFuture(1 * MIB);
			}).when(completableFc).read(Mockito.any(), Mockito.eq(4l * MIB));

			var futureResult = completableFc.readToPointer(ptr, 0l, 6 * MIB);
			var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.get());

			Assertions.assertEquals(5 * MIB, result);
			Mockito.verify(ptr).put(Mockito.eq(0l * MIB), Mockito.any(byte[].class), Mockito.eq(0), Mockito.eq(4 * MIB));
			Mockito.verify(ptr).put(Mockito.eq(4l * MIB), Mockito.any(byte[].class), Mockito.eq(0), Mockito.eq(1 * MIB));
			Mockito.verifyNoMoreInteractions(ptr);
		}

		@Test
		@DisplayName("successful multi-pass till EOF 2")
		public void readToPointerSucceedsInTwoIterationAtEOF2() {
			Mockito.doAnswer(invocation -> {
				ByteBuffer buf = invocation.getArgument(0);
				buf.put(new byte[4 * MIB]);
				return CompletableFuture.completedFuture(4 * MIB);
			}).when(completableFc).read(Mockito.any(), Mockito.eq(0l));
			Mockito.doAnswer(invocation -> {
				return CompletableFuture.completedFuture(-1);
			}).when(completableFc).read(Mockito.any(), Mockito.eq(4l * MIB));

			var futureResult = completableFc.readToPointer(ptr, 0l, 6 * MIB);
			var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.get());

			Assertions.assertEquals(4 * MIB, result);
			Mockito.verify(ptr).put(Mockito.eq(0l * MIB), Mockito.any(byte[].class), Mockito.eq(0), Mockito.eq(4 * MIB));
			Mockito.verifyNoMoreInteractions(ptr);
		}

	}

	@Nested
	@DisplayName("transferFrom(...)")
	public class TransferFrom {

		private InputStream in;

		@BeforeEach
		public void setup() {
			completableFc = Mockito.spy(completableFc);
			this.in = Mockito.mock(InputStream.class);
		}

		@Test
		@DisplayName("exception during src.read(...)")
		public void transferFromFails1() throws IOException {
			var e = new IOException("fail");
			Mockito.doThrow(e).when(in).readNBytes(Mockito.anyInt());

			var futureResult = completableFc.transferFrom(in, 42l, 100l);
			var thrown = Assertions.assertThrows(ExecutionException.class, () -> {
				Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.get());
			});

			Assertions.assertEquals(e, thrown.getCause());
		}

		@Test
		@DisplayName("exception during channel.write(...)")
		public void transferFromFails2() throws IOException {
			var e = new IOException("fail");
			Mockito.doReturn(new byte[10]).when(in).readNBytes(100);
			Mockito.doReturn(CompletableFuture.failedFuture(e)).when(completableFc).writeAll(Mockito.any(), Mockito.anyLong());


			var futureResult = completableFc.transferFrom(in, 42l, 100l);
			var thrown = Assertions.assertThrows(ExecutionException.class, () -> {
				Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.get());
			});

			Assertions.assertEquals(e, thrown.getCause());
		}

		@Test
		@DisplayName("instant EOF")
		public void transferFromEOF1() throws IOException {
			Mockito.doReturn(new byte[0]).when(in).readNBytes(100);

			var futureResult = completableFc.transferFrom(in, 42l, 100l);
			var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.get());

			Assertions.assertEquals(0, result);
		}

		@Test
		@DisplayName("EOF on first iteration")
		public void transferFromEOF2() throws IOException {
			Mockito.doReturn(new byte[80]).when(in).readNBytes(100);
			Mockito.doReturn(CompletableFuture.completedFuture(80)).when(completableFc).writeAll(Mockito.any(), Mockito.eq(42l));

			var futureResult = completableFc.transferFrom(in, 42l, 100l);
			var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.get());

			Assertions.assertEquals(80, result);
		}

		@Test
		@DisplayName("EOF in second iteration")
		public void transferFromEOF3() throws IOException {
			Mockito.doReturn(new byte[4 * MIB]).when(in).readNBytes(4 * MIB);
			Mockito.doReturn(new byte[1 * MIB]).when(in).readNBytes(2 * MIB);
			Mockito.doReturn(CompletableFuture.completedFuture(4 * MIB)).when(completableFc).writeAll(Mockito.any(), Mockito.eq(0l * MIB));
			Mockito.doReturn(CompletableFuture.completedFuture(1 * MIB)).when(completableFc).writeAll(Mockito.any(), Mockito.eq(4l * MIB));

			var futureResult = completableFc.transferFrom(in, 0l, 6l * MIB);
			var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.get());

			Assertions.assertEquals(5 * MIB, result);
		}

		@Test
		@DisplayName("transfer requested number of bytes in second iteration")
		public void transferFrom() throws IOException {
			Mockito.doReturn(new byte[4 * MIB]).when(in).readNBytes(4 * MIB);
			Mockito.doReturn(new byte[2 * MIB]).when(in).readNBytes(2 * MIB);
			Mockito.doReturn(CompletableFuture.completedFuture(4 * MIB)).when(completableFc).writeAll(Mockito.any(), Mockito.eq(0l * MIB));
			Mockito.doReturn(CompletableFuture.completedFuture(2 * MIB)).when(completableFc).writeAll(Mockito.any(), Mockito.eq(4l * MIB));

			var futureResult = completableFc.transferFrom(in, 0l, 6l * MIB);
			var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.get());

			Assertions.assertEquals(6 * MIB, result);
		}

	}

	@Nested
	@DisplayName("transferTo(...)")
	public class TransferTo {

		private WritableByteChannel dst;

		@BeforeEach
		public void setup() throws IOException {
			completableFc = Mockito.spy(completableFc);
			this.dst = Mockito.mock(WritableByteChannel.class);
			Mockito.doAnswer(invocation -> {
				ByteBuffer buffer = invocation.getArgument(0);
				int n = buffer.remaining();
				buffer.position(buffer.limit());
				return n;
			}).when(dst).write(Mockito.any());
		}

		@Test
		@DisplayName("exception during read(...)")
		public void transferToFails1() {
			var e = new IOException("fail");
			Mockito.doReturn(CompletableFuture.failedFuture(e)).when(completableFc).read(Mockito.any(), Mockito.anyLong());

			var futureResult = completableFc.transferTo(42l, 100l, dst);
			var thrown = Assertions.assertThrows(ExecutionException.class, () -> {
				Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.get());
			});

			Assertions.assertEquals(e, thrown.getCause());
		}

		@Test
		@DisplayName("exception during dst.write(...)")
		public void transferToFails2() throws IOException {
			var e = new IOException("fail");
			Mockito.doAnswer(invocation -> {
				ByteBuffer buffer = invocation.getArgument(0);
				buffer.put(new byte[100]);
				return CompletableFuture.completedFuture(100);
			}).when(completableFc).read(Mockito.any(), Mockito.eq(42l));
			Mockito.doThrow(e).when(dst).write(Mockito.any());


			var futureResult = completableFc.transferTo(42l, 100l, dst);
			var thrown = Assertions.assertThrows(ExecutionException.class, () -> {
				Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.get());
			});

			Assertions.assertEquals(e, thrown.getCause());
		}

		@Test
		@DisplayName("instant EOF")
		public void transferToEOF1() {
			Mockito.doReturn(CompletableFuture.completedFuture(-1)).when(completableFc).read(Mockito.any(), Mockito.anyLong());

			var futureResult = completableFc.transferTo(42l, 100l, dst);
			var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.get());

			Assertions.assertEquals(0, result);
		}

		@Test
		@DisplayName("EOF on first iteration")
		public void transferToEOF2() {
			Mockito.doAnswer(invocation -> {
				ByteBuffer buffer = invocation.getArgument(0);
				buffer.put(new byte[80]);
				return CompletableFuture.completedFuture(80);
			}).when(completableFc).read(Mockito.any(), Mockito.eq(42l));

			var futureResult = completableFc.transferTo(42l, 100l, dst);
			var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.get());

			Assertions.assertEquals(80, result);
		}

		@Test
		@DisplayName("EOF in second iteration")
		public void transferFromEOF3() {
			Mockito.doAnswer(invocation -> {
				ByteBuffer buffer = invocation.getArgument(0);
				buffer.put(new byte[4 * MIB]);
				return CompletableFuture.completedFuture(4 * MIB);
			}).when(completableFc).read(Mockito.any(), Mockito.eq(0l));
			Mockito.doAnswer(invocation -> {
				ByteBuffer buffer = invocation.getArgument(0);
				buffer.put(new byte[1 * MIB]);
				return CompletableFuture.completedFuture(1 * MIB);
			}).when(completableFc).read(Mockito.any(), Mockito.eq(4l * MIB));

			var futureResult = completableFc.transferTo(0l, 6l * MIB, dst);
			var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.get());

			Assertions.assertEquals(5 * MIB, result);
		}

		@Test
		@DisplayName("transfer requested number of bytes in second iteration")
		public void transferFrom() throws IOException {
			Mockito.doAnswer(invocation -> {
				ByteBuffer buffer = invocation.getArgument(0);
				buffer.put(new byte[4 * MIB]);
				return CompletableFuture.completedFuture(4 * MIB);
			}).when(completableFc).read(Mockito.any(), Mockito.eq(0l));
			Mockito.doAnswer(invocation -> {
				ByteBuffer buffer = invocation.getArgument(0);
				buffer.put(new byte[2 * MIB]);
				return CompletableFuture.completedFuture(2 * MIB);
			}).when(completableFc).read(Mockito.any(), Mockito.eq(4l * MIB));

			var futureResult = completableFc.transferTo(0l, 6l * MIB, dst);
			var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.get());

			Assertions.assertEquals(6 * MIB, result);
		}

	}

	@Test
	@DisplayName("read(...) fails with IOException")
	public void testReadFailsExceptionally() {
		var buf = Mockito.mock(ByteBuffer.class);
		var e = new IOException("fail.");
		Mockito.doAnswer(invocation -> {
			CompletableFuture<Integer> attachment = invocation.getArgument(2);
			CompletionHandler<Integer, CompletableFuture<Integer>> completionHandler = invocation.getArgument(3);
			completionHandler.failed(e, attachment);
			return null;
		}).when(fc).read(Mockito.any(), Mockito.eq(42l), Mockito.any(), Mockito.any());

		var futureResult = completableFc.read(buf, 42l);
		var thrown = Assertions.assertThrows(ExecutionException.class, () -> {
			Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.get());
		});

		Assertions.assertEquals(e, thrown.getCause());
	}

	@Test
	@DisplayName("read(...) succeeds")
	public void testRead13Bytes() {
		var buf = Mockito.mock(ByteBuffer.class);
		Mockito.doAnswer(invocation -> {
			CompletableFuture<Integer> attachment = invocation.getArgument(2);
			CompletionHandler<Integer, CompletableFuture<Integer>> completionHandler = invocation.getArgument(3);
			completionHandler.completed(13, attachment);
			return null;
		}).when(fc).read(Mockito.any(), Mockito.eq(42l), Mockito.any(), Mockito.any());

		var futureResult = completableFc.read(buf, 42l);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.get());

		Assertions.assertEquals(13, result);
	}

	@Test
	@DisplayName("write(...) fails with IOException")
	public void testWriteFailsExceptionally() {
		var buf = Mockito.mock(ByteBuffer.class);
		var e = new IOException("fail.");
		Mockito.doAnswer(invocation -> {
			CompletableFuture<Integer> attachment = invocation.getArgument(2);
			CompletionHandler<Integer, CompletableFuture<Integer>> completionHandler = invocation.getArgument(3);
			completionHandler.failed(e, attachment);
			return null;
		}).when(fc).write(Mockito.any(), Mockito.eq(42l), Mockito.any(), Mockito.any());

		var futureResult = completableFc.write(buf, 42l);
		var thrown = Assertions.assertThrows(ExecutionException.class, () -> {
			Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.get());
		});

		Assertions.assertEquals(e, thrown.getCause());
	}

	@Test
	@DisplayName("write(...) succeeds")
	public void testWrite13Bytes() {
		var buf = Mockito.mock(ByteBuffer.class);
		Mockito.doAnswer(invocation -> {
			CompletableFuture<Integer> attachment = invocation.getArgument(2);
			CompletionHandler<Integer, CompletableFuture<Integer>> completionHandler = invocation.getArgument(3);
			completionHandler.completed(13, attachment);
			return null;
		}).when(fc).write(Mockito.any(), Mockito.eq(42l), Mockito.any(), Mockito.any());

		var futureResult = completableFc.write(buf, 42l);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.get());

		Assertions.assertEquals(13, result);
	}

	@Test
	@DisplayName("writeAll(...) keeps on writing until no more bytes remaining")
	public void testWriteAll() {
		Mockito.doAnswer(invocation -> {
			ByteBuffer buffer = invocation.getArgument(0);
			buffer.position(buffer.position() + 50);
			CompletableFuture<Integer> attachment = invocation.getArgument(2);
			CompletionHandler<Integer, CompletableFuture<Integer>> completionHandler = invocation.getArgument(3);
			completionHandler.completed(50, attachment);
			return null;
		}).when(fc).write(Mockito.any(), Mockito.eq(0l), Mockito.any(), Mockito.any());
		Mockito.doAnswer(invocation -> {
			ByteBuffer buffer = invocation.getArgument(0);
			buffer.position(buffer.position() + 30);
			CompletableFuture<Integer> attachment = invocation.getArgument(2);
			CompletionHandler<Integer, CompletableFuture<Integer>> completionHandler = invocation.getArgument(3);
			completionHandler.completed(30, attachment);
			return null;
		}).when(fc).write(Mockito.any(), Mockito.eq(50l), Mockito.any(), Mockito.any());
		var buffer = ByteBuffer.allocate(80);
		Assumptions.assumeTrue(buffer.hasRemaining());

		var futureResult = completableFc.writeAll(buffer, 0l);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.get());

		Assertions.assertEquals(80, result);
		Assertions.assertFalse(buffer.hasRemaining());
	}

}