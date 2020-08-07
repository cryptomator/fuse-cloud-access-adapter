package org.cryptomator.fusecloudaccess;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.ProgressListener;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class CachedFileTest {

	private Path file;
	private CloudProvider provider;
	private FileChannel fileChannel;
	private CachedFile cachedFile;
	private RangeSet<Long> populatedRanges;

	@BeforeEach
	public void setup() throws IOException {
		this.file = Mockito.mock(Path.class, "/path/to/file");
		this.provider = Mockito.mock(CloudProvider.class);
		this.fileChannel = Mockito.mock(FileChannel.class);
		this.populatedRanges = Mockito.mock(RangeSet.class);
		this.cachedFile = new CachedFile(file, fileChannel, provider, populatedRanges);
		Mockito.when(fileChannel.size()).thenReturn(100l);
	}

	@Test
	@DisplayName("create new cached file")
	public void testCreate(@TempDir Path tmpDir) throws IOException {
		Path tmpFile = tmpDir.resolve("cache.file");
		try (var cachedFile = CachedFile.create(file, tmpFile, provider, 42l)) {
			Assertions.assertNotNull(cachedFile);
			Assertions.assertEquals(42l, Files.size(tmpFile));
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

		var futureResult = cachedFile.load(50, 10);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertEquals(result, fileChannel);
		var buf = ArgumentCaptor.forClass(ByteBuffer.class);
		Mockito.verify(fileChannel).write(buf.capture(), Mockito.eq(50l));
		Mockito.verify(populatedRanges).add(Range.closedOpen(50l, 60l));
		Assertions.assertEquals(ByteBuffer.wrap(content), buf.getValue());
	}

	@Test
	@DisplayName("load region [50, 60] (miss, hitting remote EOF)")
	public void testLoadNonExistingReachingEof() throws IOException {
		Mockito.when(populatedRanges.encloses(Range.closedOpen(50l, 60l))).thenReturn(false);
		byte[] content = new byte[9];
		Arrays.fill(content, (byte) 0x33);
		Mockito.when(provider.read(file, 50, 10, ProgressListener.NO_PROGRESS_AWARE)).thenReturn(CompletableFuture.completedFuture(new ByteArrayInputStream(content)));

		var futureResult = cachedFile.load(50, 10);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertEquals(result, fileChannel);
		var buf = ArgumentCaptor.forClass(ByteBuffer.class);
		Mockito.verify(fileChannel).write(buf.capture(), Mockito.eq(50l));
		Mockito.verify(populatedRanges).add(Range.closedOpen(50l, 59l));
		Assertions.assertEquals(ByteBuffer.wrap(content), buf.getValue().asReadOnlyBuffer().position(0).limit(content.length));
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
		Mockito.when(fileChannel.write(Mockito.any(), Mockito.anyLong())).thenThrow(e);

		var futureResult = cachedFile.load(50, 10);
		var ee = Assertions.assertThrows(ExecutionException.class, () -> {
			Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());
		});

		Assertions.assertTrue(futureResult.toCompletableFuture().isCompletedExceptionally());
		Assertions.assertEquals(e, ee.getCause());
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


		var futureResult = cachedFile.load(50, 20);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertEquals(result, fileChannel);
		var buf = ArgumentCaptor.forClass(ByteBuffer.class);
		Mockito.verify(fileChannel).write(buf.capture(), Mockito.eq(60l));
		Mockito.verify(populatedRanges).add(Range.closedOpen(60l, 70l));
		Assertions.assertEquals(ByteBuffer.wrap(content), buf.getValue());
	}

}