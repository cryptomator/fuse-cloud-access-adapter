package org.cryptomator.fusecloudaccess;

import jnr.constants.platform.OpenFlags;
import org.cryptomator.cloudaccess.api.CloudPath;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class CachedFileFactoryTest {

	private static final CloudPath PATH = CloudPath.of("this/is/a/path");
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
		var handle = cachedFileFactory.open(PATH, OPEN_FLAGS, 42l, Instant.EPOCH);

		var sameHandle = cachedFileFactory.get(handle.getId());
		Assertions.assertSame(handle, sameHandle.get());
	}

	@Test
	@DisplayName("closing removes file handle")
	public void testClosingReleasesHandle() throws IOException, ExecutionException, InterruptedException {
		var handle = cachedFileFactory.open(PATH, OPEN_FLAGS, 42l, Instant.EPOCH);
		Assumptions.assumeTrue(cachedFileFactory.get(handle.getId()).isPresent());

		var futureResult = cachedFileFactory.close(handle.getId());

		Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());
		Assertions.assertFalse(cachedFileFactory.get(handle.getId()).isPresent());
	}

	@Test
	@DisplayName("closing invalid handle is no-op")
	public void testClosingNonExisting() throws IOException {
		Assumptions.assumeFalse(cachedFileFactory.get(1337l).isPresent());

		var futureResult = cachedFileFactory.close(1337l);

		Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());
	}

	@Test
	@DisplayName("opening the same file twice leads to distinct file handles")
	public void testFileHandlesAreDistinct() throws IOException {
		var handle1 = cachedFileFactory.open(PATH, OPEN_FLAGS, 42l, Instant.EPOCH);
		var handle2 = cachedFileFactory.open(PATH, OPEN_FLAGS, 42l, Instant.EPOCH);

		Assertions.assertNotEquals(handle1, handle2);
	}

}
