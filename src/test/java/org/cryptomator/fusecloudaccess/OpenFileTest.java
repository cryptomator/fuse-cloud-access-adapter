package org.cryptomator.fusecloudaccess;

import jnr.constants.platform.OpenFlags;
import jnr.ffi.Pointer;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

public class OpenFileTest {

	private static final Path PATH = Path.of("this/is/a/path");
	private static final byte[] CONTENT = ("Lorem ipsum dolor sit amet, consectetur adipiscing elit," +
			" sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam," +
			" quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute" +
			" irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur." +
			" Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit" +
			" anim id est laborum.")
			.getBytes();

	private final CloudProvider provider = Mockito.mock(CloudProvider.class);

	@Nested
	@DisplayName("opened for reading")
	public class OpenedForReading {

		private CompletionStage<Path> cacheReady = CompletableFuture.failedFuture(new IllegalStateException("opened for reading"));
		private OpenFile file;

		@BeforeEach
		public void setup() {
			file = new OpenFile(provider, PATH, EnumSet.of(OpenFlags.O_RDONLY), cacheReady);
		}


		@DisplayName("test read(...) to return correct number of bytes read and write the correct bytes to output buffer.")
		@Test
		public void testSuccessfulRead() throws IOException {
			long offset = 400;
			int numToRead = 80;
			var expectedNumOfRead = (int) Math.min(numToRead, CONTENT.length - offset);
			assert expectedNumOfRead > 0;

			InputStream source = new ByteArrayInputStream(CONTENT);
			source.skip(offset);
			Mockito.when(provider.read(Mockito.any(), Mockito.anyLong(), Mockito.anyLong(), Mockito.any()))
					.thenReturn(CompletableFuture.completedFuture(source));

			Pointer buf = Mockito.mock(Pointer.class);
			var target = ByteBuffer.allocate(numToRead);
			Mockito.doAnswer(invocation -> {
				long off = invocation.getArgument(0);
				byte[] src = invocation.getArgument(1);
				int idx = invocation.getArgument(2);
				int len = invocation.getArgument(3);
				target.put(Arrays.copyOfRange(src, idx, src.length), (int) off, len);
				return null;
			}).when(buf).put(Mockito.anyLong(), (byte[]) Mockito.any(), Mockito.anyInt(), Mockito.anyInt());

			var futureResult = file.read(buf, offset, numToRead);
			var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

			Assertions.assertEquals(expectedNumOfRead, result);
			Assertions.assertArrayEquals(Arrays.copyOfRange(CONTENT, (int) offset, (int) offset + numToRead), target.array());
		}

		@DisplayName("test read(...) returns failed stage if IOException occurs")
		@Test
		public void testIOFailureRead() throws IOException {
			long offset = 0;
			int expectedReadBytes = 30;

			InputStream source = InputStream.nullInputStream();
			source.close();

			Mockito.when(provider.read(Mockito.any(), Mockito.anyLong(), Mockito.anyLong(), Mockito.any()))
					.thenReturn(CompletableFuture.completedFuture(source));

			Pointer buf = Mockito.mock(Pointer.class);
			var result = file.read(buf, offset, expectedReadBytes).toCompletableFuture();

			Assertions.assertTrue(result.isCompletedExceptionally());
		}

		@DisplayName("test read(...) returns failed stage if provider.read(...) fails")
		@Test
		public void testCloudFailureRead() {
			long offset = 0;
			int expectedReadBytes = 30;

			Mockito.when(provider.read(Mockito.any(), Mockito.anyLong(), Mockito.anyLong(), Mockito.any()))
					.thenReturn(CompletableFuture.failedFuture(new RuntimeException()));

			Pointer buf = Mockito.mock(Pointer.class);
			var result = file.read(buf, offset, expectedReadBytes).toCompletableFuture();

			Assertions.assertTrue(result.isCompletedExceptionally());
		}

		@DisplayName("test write(...) returns failed stage")
		@Test
		public void testWrite() {
			Pointer buf = Mockito.mock(Pointer.class);

			var futureResult = file.write(buf, 0l, 0l);

			Assertions.assertTrue(futureResult.toCompletableFuture().isCompletedExceptionally());
		}

	}

	@Nested
	@DisplayName("opened for writing")
	public class OpenedForWriting {

		private Path cacheFile;
		private CompletionStage<Path> cacheReady;
		private OpenFile file;

		@BeforeEach
		public void setup(@TempDir Path tmpDir) throws IOException {
			cacheFile = tmpDir.resolve("cache.file");
			Files.createFile(cacheFile);
			cacheReady = CompletableFuture.completedFuture(cacheFile);
			file = new OpenFile(provider, PATH, EnumSet.of(OpenFlags.O_RDWR), cacheReady);
		}

		@DisplayName("write(...) returns correct number of bytes written and write the correct bytes to the cache.")
		@Test
		public void testWrite() throws IOException {
			byte[] bufferContent = new byte[5000];
			new Random(42l).nextBytes(bufferContent);
			Pointer buf = Mockito.mock(Pointer.class);
			Mockito.doAnswer(invocation -> {
				long off = invocation.getArgument(0);
				byte[] dst = invocation.getArgument(1);
				int idx = invocation.getArgument(2);
				int len = invocation.getArgument(3);
				System.arraycopy(bufferContent, (int) off, dst, idx, len);
				return null;
			}).when(buf).get(Mockito.anyLong(), (byte[]) Mockito.any(), Mockito.anyInt(), Mockito.anyInt());

			var futureResult = file.write(buf, 100l, bufferContent.length);
			var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

			Assertions.assertEquals(bufferContent.length, result);
			byte[] fileContent = Files.readAllBytes(cacheFile);
			Assertions.assertArrayEquals(new byte[100], Arrays.copyOf(fileContent, 100));
			Assertions.assertArrayEquals(bufferContent, Arrays.copyOfRange(fileContent, 100, fileContent.length));
		}

		@DisplayName("flush() writes cached contents to provider")
		@Test
		public void testFlush() throws IOException {
			Files.write(cacheFile, CONTENT);
			var written = new AtomicReference<byte[]>();
			Mockito.when(provider.write(Mockito.eq(PATH), Mockito.eq(true), Mockito.any(), Mockito.any())).thenAnswer(invocation -> {
				InputStream in = invocation.getArgument(2);
				written.set(in.readAllBytes());
				return CompletableFuture.completedFuture(null);
			});
			file.setDirty();

			var futureResult = file.flush();
			var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100000), () -> futureResult.toCompletableFuture().get());

			Assertions.assertNull(result);
			Assertions.assertArrayEquals(CONTENT, written.get());
		}

	}

}
