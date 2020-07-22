package org.cryptomator.fusecloudaccess;

import jnr.constants.platform.OpenFlags;
import jnr.ffi.Pointer;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class OpenFileTest {

	private static final Path PATH = Path.of("this/is/a/path");
	private static final Set<OpenFlags> OPEN_FLAGS = Set.of(OpenFlags.O_RDWR);
	private static final byte[] content = ("Lorem ipsum dolor sit amet, consectetur adipiscing elit," +
			" sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam," +
			" quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute" +
			" irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur." +
			" Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit" +
			" anim id est laborum.")
			.getBytes();

	private final CloudProvider provider = Mockito.mock(CloudProvider.class);
	private OpenFile file;

	@BeforeEach
	public void setup() {
		file = new OpenFile(provider, PATH, OPEN_FLAGS);
	}

	@DisplayName("test read(...) to return correct number of bytes read and write the correct bytes to output buffer.")
	@Test
	public void testSuccessfulRead() throws IOException {
		long offset = 400;
		int numToRead = 80;
		var expectedNumOfRead = (int) Math.min(numToRead, content.length - offset);
		assert expectedNumOfRead > 0;

		InputStream source = new ByteArrayInputStream(content);
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
		Assertions.assertArrayEquals(Arrays.copyOfRange(content, (int) offset, (int) offset + numToRead), target.array());
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
}
