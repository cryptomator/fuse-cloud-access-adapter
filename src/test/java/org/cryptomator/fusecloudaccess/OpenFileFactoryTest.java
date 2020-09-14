package org.cryptomator.fusecloudaccess;

import jnr.constants.platform.OpenFlags;
import org.cryptomator.cloudaccess.api.CloudPath;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class OpenFileFactoryTest {

	private static final CloudPath PATH = CloudPath.of("this/is/a/path");
	private static final Set<OpenFlags> OPEN_FLAGS = Set.of(OpenFlags.O_RDONLY);

	private ConcurrentMap<CloudPath, OpenFile> activeFiles = new ConcurrentHashMap<>();
	private CloudProvider provider = Mockito.mock(CloudProvider.class);
	private OpenFileUploader uploader = Mockito.mock(OpenFileUploader.class);
	private ScheduledExecutorService scheduler = Mockito.mock(ScheduledExecutorService.class);
	private OpenFileFactory openFileFactory;
	private OpenFile openFile;

	@BeforeEach
	public void setup(@TempDir Path tmpDir) {
		openFileFactory = new OpenFileFactory(activeFiles, provider, uploader, tmpDir, scheduler);
		openFile = Mockito.mock(OpenFile.class);
		activeFiles.put(PATH, openFile);
		Mockito.when(openFile.getOpenFileHandleCount()).thenReturn(new AtomicInteger(0));
	}

	@Test
	@DisplayName("can get(...) file handle after opening a file")
	public void testAfterOpenTheOpenFileIsPresent() throws IOException {
		var handle = openFileFactory.open(PATH, OPEN_FLAGS, 42l, Instant.EPOCH);

		var file = openFileFactory.get(handle);

		Assertions.assertTrue(file.isPresent());
	}

	@Test
	@DisplayName("closing non-last file handle removes it without upload")
	public void testClosingReleasesHandleNotLast() throws IOException {
		var handle = openFileFactory.open(PATH, OPEN_FLAGS, 42l, Instant.EPOCH);
		Assumptions.assumeTrue(openFileFactory.get(handle).isPresent());
		Mockito.when(openFile.transitionToUploading()).thenReturn(false);
		Mockito.when(openFile.getPath()).thenReturn(PATH);
		Mockito.when(openFile.getOpenFileHandleCount()).thenReturn(new AtomicInteger(3));

		openFileFactory.close(handle);

		Assertions.assertFalse(openFileFactory.get(handle).isPresent());
		Assertions.assertEquals(2, openFile.getOpenFileHandleCount().get());
		Assertions.assertTrue(activeFiles.containsKey(PATH));
		Mockito.verify(uploader, Mockito.never()).scheduleUpload(Mockito.any(), Mockito.any());
	}

	@Test
	@DisplayName("closing last file handle removes it without upload if unmodified")
	public void testClosingReleasesHandleUnmodified() throws IOException {
		var handle = openFileFactory.open(PATH, OPEN_FLAGS, 42l, Instant.EPOCH);
		Assumptions.assumeTrue(openFile.equals(openFileFactory.get(handle).get()));
		Mockito.when(openFile.transitionToUploading()).thenReturn(false);
		Mockito.when(openFile.getPath()).thenReturn(PATH);
		Mockito.when(openFile.getOpenFileHandleCount()).thenReturn(new AtomicInteger(1));

		openFileFactory.close(handle);

		Assertions.assertEquals(0, openFile.getOpenFileHandleCount().get());
		Assertions.assertTrue(activeFiles.containsKey(PATH));
		Mockito.verify(uploader, Mockito.never()).scheduleUpload(Mockito.any(), Mockito.any());
	}

	@Test
	@DisplayName("closing last file handle triggers upload if modified")
	public void testClosingLastHandleTriggersUpload() throws IOException {
		var handle = openFileFactory.open(PATH, OPEN_FLAGS, 42l, Instant.EPOCH);
		Assumptions.assumeTrue(openFile.equals(openFileFactory.get(handle).get()));
		Mockito.when(openFile.transitionToUploading()).thenReturn(true);
		Mockito.when(openFile.getPath()).thenReturn(PATH);
		Mockito.when(openFile.getOpenFileHandleCount()).thenReturn(new AtomicInteger(1));

		openFileFactory.close(handle);

		Assertions.assertEquals(0, openFile.getOpenFileHandleCount().get());
		Assertions.assertTrue(activeFiles.containsKey(PATH));
		Mockito.verify(uploader).scheduleUpload(Mockito.eq(openFile), Mockito.any());
	}

	@Test
	@DisplayName("closing invalid handle is no-op")
	public void testClosingNonExisting() {
		Assumptions.assumeFalse(openFileFactory.get(1337l).isPresent());

		openFileFactory.close(1337l);
	}

	@Test
	@DisplayName("opening the same file twice leads to distinct file handles")
	public void testFileHandlesAreDistinct() throws IOException {
		var handle1 = openFileFactory.open(PATH, OPEN_FLAGS, 42l, Instant.EPOCH);
		var handle2 = openFileFactory.open(PATH, OPEN_FLAGS, 42l, Instant.EPOCH);

		Assertions.assertNotEquals(handle1, handle2);
		Assertions.assertEquals(2, openFile.getOpenFileHandleCount().get());
	}


	@DisplayName("getCachedMetadata()")
	@Test
	public void testGetCachedMetadata() {
		Mockito.when(openFile.getLastModified()).thenReturn(Instant.EPOCH);
		Mockito.when(openFile.getSize()).thenReturn(42l);

		var metadata = openFileFactory.getCachedMetadata(PATH);

		Assertions.assertEquals(PATH, metadata.get().getPath());
		Assertions.assertEquals(PATH.getFileName().toString(), metadata.get().getName());
		Assertions.assertEquals(Instant.EPOCH, metadata.get().getLastModifiedDate().get());
		Assertions.assertEquals(42l, metadata.get().getSize().get());
	}

}
