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
import java.util.concurrent.ExecutionException;

public class OpenFileFactoryTest {

	private static final CloudPath PATH = CloudPath.of("this/is/a/path");
	private static final Set<OpenFlags> OPEN_FLAGS = Set.of(OpenFlags.O_RDONLY);

	private CloudProvider provider = Mockito.mock(CloudProvider.class);
	private OpenFileUploader uploader = Mockito.mock(OpenFileUploader.class);
	private OpenFileFactory openFileFactory;

	@BeforeEach
	public void setup(@TempDir Path tmpDir) {
		openFileFactory = new OpenFileFactory(provider, uploader, tmpDir);
	}

	@Test
	@DisplayName("can get(...) file handle after opening a file")
	public void testAfterOpenTheOpenFileIsPresent() throws IOException {
		var handle = openFileFactory.open(PATH, OPEN_FLAGS, 42l, Instant.EPOCH);

		var sameHandle = openFileFactory.get(handle.getId());
		Assertions.assertSame(handle, sameHandle.get());
	}

	@Test
	@DisplayName("closing removes file handle")
	public void testClosingReleasesHandle() throws IOException {
		var handle = openFileFactory.open(PATH, OPEN_FLAGS, 42l, Instant.EPOCH);
		Assumptions.assumeTrue(openFileFactory.get(handle.getId()).isPresent());

		openFileFactory.close(handle.getId());

		Assertions.assertFalse(openFileFactory.get(handle.getId()).isPresent());
	}

	@Test
	@DisplayName("closing last file handle triggers upload")
	public void testClosingLastHandleTriggersUpload() throws IOException {
		var handle1 = openFileFactory.open(PATH, OPEN_FLAGS, 42l, Instant.EPOCH);
		var handle2 = openFileFactory.open(PATH, OPEN_FLAGS, 42l, Instant.EPOCH);
		Assertions.assertNotEquals(handle1, handle2);

		openFileFactory.close(handle1.getId());
		Mockito.verify(uploader, Mockito.never()).scheduleUpload(Mockito.any());

		openFileFactory.close(handle2.getId());
		Mockito.verify(uploader).scheduleUpload(handle2.getFile());
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
	}

}
