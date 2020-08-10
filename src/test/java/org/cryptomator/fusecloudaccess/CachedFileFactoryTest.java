package org.cryptomator.fusecloudaccess;

import jnr.constants.platform.OpenFlags;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class CachedFileFactoryTest {

	private static final Path PATH = Path.of("this/is/a/path");
	private static final Set<OpenFlags> OPEN_FLAGS = Set.of(OpenFlags.O_RDONLY);

	private CloudProvider provider = Mockito.mock(CloudProvider.class);
	private CachedFileFactory cachedFileFactory;

	@BeforeEach
	public void setup() {
		cachedFileFactory = new CachedFileFactory(provider);
	}

	@Test
	@DisplayName("can get(...) file handle after opening a file")
	public void testAfterOpenTheOpenFileIsPresent() throws IOException {
		var handle = cachedFileFactory.open(PATH, OPEN_FLAGS, 42l);

		var sameHandle = cachedFileFactory.get(handle.getId());
		Assertions.assertSame(handle, sameHandle.get());
	}

	@Test
	@DisplayName("closing removes file handle")
	public void testClosingReleasesHandle() throws IOException {
		var handle = cachedFileFactory.open(PATH, OPEN_FLAGS, 42l);
		Assumptions.assumeTrue(cachedFileFactory.get(handle.getId()).isPresent());
		Mockito.when(provider.write(Mockito.eq(PATH), Mockito.eq(true), Mockito.any(), Mockito.any())).thenReturn(CompletableFuture.completedFuture(null));

		cachedFileFactory.close(handle.getId());
		Mockito.verify(provider).write(Mockito.eq(PATH), Mockito.eq(true), Mockito.any(), Mockito.any());

		Assertions.assertFalse(cachedFileFactory.get(handle.getId()).isPresent());
	}

	@Test
	@DisplayName("opening the same file twice leads to distinct file handles")
	public void testFileHandlesAreDistinct() throws IOException {
		var handle1 = cachedFileFactory.open(PATH, OPEN_FLAGS, 42l);
		var handle2 = cachedFileFactory.open(PATH, OPEN_FLAGS, 42l);

		Assertions.assertNotEquals(handle1, handle2);
	}

	@Test
	@DisplayName("closing the last remaining handle writes cached contents")
	public void testClosingLastRemainingHandleFlushesCachedContents() throws IOException {
		var handle1 = cachedFileFactory.open(PATH, OPEN_FLAGS, 42l);
		var handle2 = cachedFileFactory.open(PATH, OPEN_FLAGS, 42l);
		Assumptions.assumeTrue(cachedFileFactory.get(handle1.getId()).isPresent());
		Assumptions.assumeTrue(cachedFileFactory.get(handle2.getId()).isPresent());
		Mockito.when(provider.write(Mockito.eq(PATH), Mockito.eq(true), Mockito.any(), Mockito.any())).thenReturn(CompletableFuture.completedFuture(null));

		cachedFileFactory.close(handle1.getId());
		Assertions.assertFalse(cachedFileFactory.get(handle1.getId()).isPresent());
		Assertions.assertTrue(cachedFileFactory.get(handle2.getId()).isPresent());
		Mockito.verify(provider, Mockito.never()).write(Mockito.eq(PATH), Mockito.anyBoolean(), Mockito.any(), Mockito.any());

		cachedFileFactory.close(handle2.getId());
		Assertions.assertFalse(cachedFileFactory.get(handle1.getId()).isPresent());
		Assertions.assertFalse(cachedFileFactory.get(handle2.getId()).isPresent());
		Mockito.verify(provider).write(Mockito.eq(PATH), Mockito.anyBoolean(), Mockito.any(), Mockito.any());
	}

}
