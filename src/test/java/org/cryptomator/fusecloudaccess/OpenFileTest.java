package org.cryptomator.fusecloudaccess;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import org.cryptomator.cloudaccess.api.CloudPath;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.ProgressListener;
import org.cryptomator.cloudaccess.api.exceptions.CloudProviderException;
import org.junit.jupiter.api.Assertions;
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
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.spi.FileSystemProvider;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public class OpenFileTest {

	private CloudPath file;
	private Path tmpFile;
	private FileSystem tmpFileSystem;
	private FileSystemProvider tmpFileSystemProvider;
	private CloudProvider provider;
	private FileChannel fileChannel;
	private OpenFile openFile;
	private RangeSet<Long> populatedRanges;
	private Consumer<CloudPath> onRelease;

	@BeforeEach
	public void setup() throws IOException {
		this.file = Mockito.mock(CloudPath.class, "/path/to/file");
		this.tmpFile = Mockito.mock(Path.class, "/tmp/cache.file");
		this.tmpFileSystem = Mockito.mock(FileSystem.class);
		this.tmpFileSystemProvider = Mockito.mock(FileSystemProvider.class);
		this.provider = Mockito.mock(CloudProvider.class);
		this.fileChannel = Mockito.mock(FileChannel.class);
		this.populatedRanges = Mockito.mock(RangeSet.class);
		this.onRelease = Mockito.mock(Consumer.class);
		this.openFile = new OpenFile(file, tmpFile, fileChannel, provider, populatedRanges, Instant.EPOCH);
		Mockito.when(fileChannel.size()).thenReturn(100l);
		Mockito.when(fileChannel.isOpen()).thenReturn(true);
		Mockito.when(tmpFile.getFileSystem()).thenReturn(tmpFileSystem);
		Mockito.when(tmpFileSystem.provider()).thenReturn(tmpFileSystemProvider);
	}

	@DisplayName("create new cached file")
	@ParameterizedTest(name = "with initial size={0}")
	@ValueSource(longs = {0l, 1l, 42l})
	public void testCreate(long size, @TempDir Path tmpDir) throws IOException {
		Path tmpFile = tmpDir.resolve("cache.file");
		try (var cachedFile = OpenFile.create(file, tmpFile, provider, size, Instant.EPOCH)) {
			Assertions.assertNotNull(cachedFile);
			Assertions.assertEquals(size, Files.size(tmpFile));
		}
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

		var futureResult = openFile.load(50, 10);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertEquals(result, fileChannel);
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
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertEquals(result, fileChannel);
		Mockito.verify(provider, Mockito.never()).read(Mockito.any(), Mockito.anyLong(), Mockito.anyLong(), Mockito.any());
	}

	@Test
	@DisplayName("load region [50, 60] (hit)")
	public void testLoadCached() {
		Mockito.when(populatedRanges.encloses(Range.closedOpen(50l, 60l))).thenReturn(true);

		var futureResult = openFile.load(50, 10);
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

		var futureResult = openFile.load(50, 20);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertEquals(result, fileChannel);
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