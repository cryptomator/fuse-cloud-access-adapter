package org.cryptomator.fusecloudaccess;

import jnr.constants.platform.OpenFlags;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.Set;

public class OpenFileFactoryTest {

	private static final Path PATH = Path.of("this/is/a/path");
	private static final Set<OpenFlags> OPEN_FLAGS = Set.of(OpenFlags.O_RDONLY);

	private CloudProvider provider = Mockito.mock(CloudProvider.class);
	private OpenFileFactory openFiles;

	@BeforeEach
	public void setup() {
		openFiles = new OpenFileFactory(provider);
	}

	@Test
	public void testAfterOpenTheOpenFileIsPresent() {
		long handle = openFiles.open(PATH, OPEN_FLAGS);

		var actualFile = openFiles.get(handle);

		Assertions.assertTrue(actualFile.isPresent());
	}

	@Test
	public void testAfterCloseTheOpenFileIsNotPresent() {
		long handle = openFiles.open(PATH, OPEN_FLAGS);
		assert openFiles.get(handle).isPresent();

		openFiles.close(handle);
		var actualFile = openFiles.get(handle);

		Assertions.assertTrue(actualFile.isEmpty());
	}

	@Test
	public void testSameParameterGetTwoHandlesAndOpenFiles() {
		//TODO: concurrent
		long handle1 = openFiles.open(PATH, OPEN_FLAGS);
		long handle2 = openFiles.open(PATH, OPEN_FLAGS);

		Assertions.assertNotEquals(handle1, handle2);

		var actualFile1 = openFiles.get(handle1);
		var actualFile2 = openFiles.get(handle2);

		Assertions.assertTrue(actualFile1.isPresent());
		Assertions.assertTrue(actualFile2.isPresent());
		Assertions.assertNotEquals(actualFile1.get(), actualFile2.get());
	}

	@Test
	public void testClosingOneHandleClosesCorrectOpenFile() {
		//TODO: Concurrent test!
		long handleClose = openFiles.open(PATH, OPEN_FLAGS);
		long handleOpen = openFiles.open(PATH, OPEN_FLAGS);
		assert openFiles.get(handleClose).isPresent() && openFiles.get(handleOpen).isPresent();

		openFiles.close(handleClose);
		var actualFileClosed = openFiles.get(handleClose);
		var actualFileOpen = openFiles.get(handleOpen);

		Assertions.assertTrue(actualFileClosed.isEmpty());
		Assertions.assertTrue(actualFileOpen.isPresent());
	}

}
