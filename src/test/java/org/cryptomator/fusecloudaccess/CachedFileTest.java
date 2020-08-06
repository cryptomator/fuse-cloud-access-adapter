package org.cryptomator.fusecloudaccess;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CachedFileTest {

	private Path file;
	private BiFunction<Long, Long, CompletionStage<InputStream>> loader;
	private CachedFile cachedFile;

	@BeforeAll
	public void setup(@TempDir Path tmpDir) throws IOException {
		this.file = tmpDir.resolve("cache.file");
		this.loader = Mockito.mock(BiFunction.class);
		this.cachedFile = CachedFile.create(file, loader);
	}

	@Test
	@Order(1)
	@DisplayName("load region [8, 12] with 0x33")
	public void testLoad1() {
		byte[] content = new byte[4];
		Arrays.fill(content, (byte) 0x33);
		Mockito.when(loader.apply(8l, 4l)).thenReturn(CompletableFuture.completedFuture(new ByteArrayInputStream(content)));

		var futureResult = cachedFile.load(8, 4);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertNotNull(result);
	}

	@Test
	@Order(2)
	@DisplayName("load region [15, 20] with 0x77")
	public void testLoad2() {
		byte[] content = new byte[5];
		Arrays.fill(content, (byte) 0x77);
		Mockito.when(loader.apply(15l, 5l)).thenReturn(CompletableFuture.completedFuture(new ByteArrayInputStream(content)));

		var futureResult = cachedFile.load(15, 5);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertNotNull(result);
	}

	@Test
	@Order(3)
	@DisplayName("load region [10, 16] with 0x55")
	public void testLoad3() {
		byte[] content = new byte[6];
		Arrays.fill(content, (byte) 0x55);
		Mockito.when(loader.apply(10l, 6l)).thenReturn(CompletableFuture.completedFuture(new ByteArrayInputStream(content)));

		var futureResult = cachedFile.load(10, 6);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertNotNull(result);
	}

	@Test
	@Order(4)
	@DisplayName("load region [999, 1000] which is behind EOF")
	public void testLoad4() {
		Mockito.when(loader.apply(999l, 1l)).thenReturn(CompletableFuture.completedFuture(new ByteArrayInputStream(new byte[0])));

		var futureResult = cachedFile.load(999, 1);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertNotNull(result);
	}

	@Test
	@Order(5)
	@DisplayName("load region [9, 19] from cache")
	public void testLoad5() throws IOException {
		var futureResult = cachedFile.load(9, 10);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertNotNull(result);
		var baos = new ByteArrayOutputStream();
		result.transferTo(9, 10, Channels.newChannel(baos));
		var expected = new byte[] {0x33, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x77, 0x77, 0x77};
		Assertions.assertArrayEquals(expected, baos.toByteArray());
		Mockito.verify(loader, Mockito.never()).apply(9l, 10l);
	}

}