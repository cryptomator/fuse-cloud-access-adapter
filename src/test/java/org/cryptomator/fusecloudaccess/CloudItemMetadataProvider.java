package org.cryptomator.fusecloudaccess;

import org.cryptomator.cloudaccess.api.CloudItemMetadata;
import org.cryptomator.cloudaccess.api.CloudItemType;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;

public class CloudItemMetadataProvider {
	private static final String DEFAULT_NAME = "unknown.obj";
	private static final Path DEFAULT_PATH = Path.of("path/to/object");
	private static final CloudItemType DEFAULT_TYPE = CloudItemType.UNKNOWN;
	private static final Instant DEFAULT_MTIME = Instant.now();
	private static final long DEFAULT_SIZE = 1337L;

	public static CloudItemMetadata of(String name, Path p, CloudItemType type, Instant mTime, Long size) {
		return new CloudItemMetadata(name, p, type, Optional.ofNullable(mTime), Optional.ofNullable(size));
	}

	public static CloudItemMetadata ofName(String name) {
		return of(name, DEFAULT_PATH, DEFAULT_TYPE, DEFAULT_MTIME, DEFAULT_SIZE);
	}

	public static CloudItemMetadata ofPath(Path p) {
		return of(DEFAULT_NAME, p, DEFAULT_TYPE, DEFAULT_MTIME, DEFAULT_SIZE);
	}

	public static CloudItemMetadata ofType(CloudItemType type) {
		return of(DEFAULT_NAME, DEFAULT_PATH, type, DEFAULT_MTIME, DEFAULT_SIZE);
	}

	public static CloudItemMetadata ofMTime(Instant mTime) {
		return of(DEFAULT_NAME, DEFAULT_PATH, DEFAULT_TYPE, mTime, DEFAULT_SIZE);
	}

	public static CloudItemMetadata ofSize(Long size) {
		return of(DEFAULT_NAME, DEFAULT_PATH, DEFAULT_TYPE, DEFAULT_MTIME, size);
	}
}
