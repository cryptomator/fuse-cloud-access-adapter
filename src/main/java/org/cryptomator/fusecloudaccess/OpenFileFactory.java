package org.cryptomator.fusecloudaccess;

import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import jnr.constants.platform.OpenFlags;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class OpenFileFactory {

	private static final Logger LOG = LoggerFactory.getLogger(OpenFileFactory.class);

	private final ConcurrentMap<Long, OpenFile> openFiles = new ConcurrentHashMap<>();
	private final AtomicLong fileHandleGen = new AtomicLong();
	private final CloudProvider provider;
	private final int timeoutMillis;

	public OpenFileFactory(CloudProvider provider, int timeoutMillis) {
		this.provider = provider;
		this.timeoutMillis = timeoutMillis;
	}

	/**
	 * @param path path of the file to open
	 * @param flags file open options
	 * @return file handle used to identify and close open files.
	 */
	public long open(Path path, Set<OpenFlags> flags) {
		long fileHandle = fileHandleGen.getAndIncrement();
		OpenFile file = new OpenFile(provider, path, flags, timeoutMillis);
		openFiles.put(fileHandle, file);
		LOG.trace("Opening {} {}", fileHandle, file);
		return fileHandle;
	}

	public OpenFile get(long fileHandle) {
		OpenFile result = openFiles.get(fileHandle);
		if (result == null) {
			throw new IllegalArgumentException("No open file for handle: " + fileHandle);
		}
		return result;
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
		}
	}

}
