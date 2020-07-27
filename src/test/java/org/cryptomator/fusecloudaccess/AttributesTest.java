package org.cryptomator.fusecloudaccess;

import jnr.ffi.Runtime;
import org.cryptomator.cloudaccess.api.CloudItemMetadata;
import org.cryptomator.cloudaccess.api.CloudItemType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import ru.serce.jnrfuse.struct.FileStat;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;


public class AttributesTest {

	private static final Path PATH = Path.of("path/to/resource");
	private static final Runtime RUNTIME = Runtime.getSystemRuntime();

	private CloudItemMetadata itemMetadata;
	private FileStat fileStat;

	@BeforeEach
	public void setup() {
		itemMetadata = Mockito.mock(CloudItemMetadata.class);
		Mockito.when(itemMetadata.getName()).thenReturn(PATH.getFileName().toString());
		Mockito.when(itemMetadata.getPath()).thenReturn(PATH);
		Mockito.when(itemMetadata.getItemType()).thenReturn(CloudItemType.UNKNOWN);
		fileStat = new FileStat(RUNTIME);
	}

	@ParameterizedTest
	@MethodSource("provideMetadataOfDifferentTypes")
	public void testFileTypesAreCorrect(CloudItemType type, int expectedBitsSet) {
		Mockito.when(itemMetadata.getItemType()).thenReturn(type);
		Attributes.copy(itemMetadata, fileStat);
		Assertions.assertEquals(expectedBitsSet, fileStat.st_mode.intValue() & expectedBitsSet);
	}

	private static Stream<Arguments> provideMetadataOfDifferentTypes() {
		return Stream.of(
				Arguments.of(CloudItemType.FILE, FileStat.S_IFREG),
				Arguments.of(CloudItemType.FOLDER, FileStat.S_IFDIR),
				Arguments.of(CloudItemType.UNKNOWN, 0)
		);
	}

	@ParameterizedTest
	@MethodSource("provideInstantOrNull")
	public void testMTimeSetCorrectly(Instant i) {
		var expectedInstant = Objects.isNull(i) ? Instant.EPOCH : i;
		Mockito.when(itemMetadata.getLastModifiedDate()).thenReturn(Optional.ofNullable(i));
		Attributes.copy(itemMetadata, fileStat);

		Assertions.assertEquals(expectedInstant.getEpochSecond(), fileStat.st_mtim.tv_sec.longValue());
		Assertions.assertEquals(expectedInstant.getNano(), fileStat.st_mtim.tv_nsec.longValue());
	}

	private static Stream<Arguments> provideInstantOrNull() {
		return Stream.of(
				Arguments.of(Instant.now()),
				Arguments.of((Instant) null)
		);
	}

	@ParameterizedTest
	@NullSource
	@ValueSource(longs = {123456L})
	public void testSizeSetCorrectly(Long size) {
		final long expectedSize = Objects.isNull(size) ? 0 : size;
		Mockito.when(itemMetadata.getSize()).thenReturn(Optional.ofNullable(size));
		Attributes.copy(itemMetadata, fileStat);
		Assertions.assertEquals(expectedSize, fileStat.st_size.longValue());

	}

}
