package org.cryptomator.fusecloudaccess;

import jnr.ffi.Runtime;
import org.cryptomator.cloudaccess.api.CloudItemMetadata;
import org.cryptomator.cloudaccess.api.CloudItemType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import ru.serce.jnrfuse.struct.FileStat;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;
import java.util.stream.Stream;


public class AttributesTest {

	private static final Runtime RUNTIME = Runtime.getSystemRuntime();

	private FileStat fileStat;

	@BeforeEach
	public void setup(){
		fileStat = new FileStat(RUNTIME);
	}

	@ParameterizedTest
	@MethodSource("provideMetadataOfDifferentTypes")
	public void testfileTypesAreCorrect(CloudItemType type, int expectedBitsSet){
		Attributes.copy(CloudItemMetadataProvider.ofType(type),fileStat);
		Assertions.assertEquals(expectedBitsSet, fileStat.st_mode.intValue() & expectedBitsSet );
	}

	private static Stream<Arguments> provideMetadataOfDifferentTypes(){
		return Stream.of(
				Arguments.of(CloudItemType.FILE, FileStat.S_IFREG),
				Arguments.of(CloudItemType.FOLDER, FileStat.S_IFDIR),
				Arguments.of(CloudItemType.UNKNOWN, 0)
		);
	}

	private static class CloudItemMetadataProvider {

		private static final String DEFAULT_NAME = "unknown.obj";
		private static final Path DEFAULT_PATH = Path.of("path/to/object");
		private static final CloudItemType DEFAULT_TYPE = CloudItemType.UNKNOWN;
		private static final Instant DEFAULT_MTIME = Instant.now();
		private static final long DEFAULT_SIZE = 1337L;

		public static CloudItemMetadata of(String name, Path p, CloudItemType type,  Instant mTime ,long size){
			return new CloudItemMetadata(name, p, type, Optional.of(mTime), Optional.of(size));
		}

		public static CloudItemMetadata ofName(String name){
			return CloudItemMetadataProvider.of(name, DEFAULT_PATH, DEFAULT_TYPE,DEFAULT_MTIME,DEFAULT_SIZE);
		}

		public static CloudItemMetadata ofPath(Path p){
			return CloudItemMetadataProvider.of(DEFAULT_NAME, p, DEFAULT_TYPE,DEFAULT_MTIME,DEFAULT_SIZE);
		}

		public static CloudItemMetadata ofType(CloudItemType type){
			return CloudItemMetadataProvider.of(DEFAULT_NAME, DEFAULT_PATH, type,DEFAULT_MTIME,DEFAULT_SIZE);
		}

		public static CloudItemMetadata ofMTime(Instant mTime){
			return CloudItemMetadataProvider.of(DEFAULT_NAME, DEFAULT_PATH, DEFAULT_TYPE,mTime,DEFAULT_SIZE);
		}

		public static CloudItemMetadata ofSize( long size){
			return CloudItemMetadataProvider.of(DEFAULT_NAME, DEFAULT_PATH, DEFAULT_TYPE,DEFAULT_MTIME,size);
		}
	}

}
