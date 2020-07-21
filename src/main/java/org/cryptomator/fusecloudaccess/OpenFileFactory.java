package org.cryptomator.fusecloudaccess;

import jnr.constants.platform.OpenFlags;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

class OpenFileFactory {

	private static final Logger LOG = LoggerFactory.getLogger(OpenFileFactory.class);

	private final ConcurrentMap<Long, OpenFile> openFiles = new ConcurrentHashMap<>();
	private final AtomicLong fileHandleGen = new AtomicLong();
	private final CloudProvider provider;

	public OpenFileFactory(CloudProvider provider) {
		this.provider = provider;
	}

	/**
	 * @param path path of the file to open
	 * @param flags file open options
	 * @return file handle used to identify and close open files.
	 */
	public long open(Path path, Set<OpenFlags> flags) {
		long fileHandle = fileHandleGen.getAndIncrement();
		OpenFile file = new OpenFile(provider, path, flags);
		openFiles.put(fileHandle, file);
		LOG.trace("Opening {} {}", fileHandle, file);
		return fileHandle;
	}

	public Optional<OpenFile> get(long fileHandle) {
		return Optional.ofNullable(openFiles.get(fileHandle));
	}

	/**
	 * Closes the channel identified by the given fileHandle
	 *
	 * @param fileHandle file handle used to identify
	 */
	public void close(long fileHandle) {
		OpenFile file = openFiles.remove(fileHandle);
		if (file != null) {
			LOG.trace("Releasing {} {}", fileHandle, file);
		} else {
			LOG.trace("No open file with handle {} found.", fileHandle);
		}
	}

}
