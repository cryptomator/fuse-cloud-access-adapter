package org.cryptomator.fusecloudaccess;

import org.cryptomator.cloudaccess.api.CloudProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

class OpenDirFactory {

	private static final Logger LOG = LoggerFactory.getLogger(OpenDirFactory.class);

	private final ConcurrentMap<Long, OpenDir> openDirs = new ConcurrentHashMap<>();
	private final AtomicLong fileHandleGen = new AtomicLong();
	private final CloudProvider provider;

	public OpenDirFactory(CloudProvider provider) {
		this.provider = provider;
	}

	/**
	 * @param path  path of the dir to open
	 * @return file handle used to identify and close open files.
	 */
	public long open(Path path) {
		long fileHandle = fileHandleGen.getAndIncrement();
		OpenDir dir = new OpenDir(provider, path);
		openDirs.put(fileHandle, dir);
		LOG.trace("Opening dir {} {}", fileHandle, dir);
		return fileHandle;
	}

	public Optional<OpenDir> get(long dirHandle) {
		return Optional.ofNullable(openDirs.get(dirHandle));
	}

	/**
	 * Closes the channel identified by the given fileHandle
	 *
	 * @param fileHandle file handle used to identify
	 */
	public void close(long fileHandle) {
		OpenDir dir = openDirs.remove(fileHandle);
		if (dir != null) {
			LOG.trace("Releasing dir {} {}", fileHandle, dir);
		}
	}

}
