package org.cryptomator.fusecloudaccess;

import org.cryptomator.cloudaccess.api.CloudPath;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

@FileSystemScoped
class OpenDirFactory {

	private static final Logger LOG = LoggerFactory.getLogger(OpenDirFactory.class);

	private final ConcurrentMap<Long, OpenDir> openDirs = new ConcurrentHashMap<>();
	private final AtomicLong fileHandleGen = new AtomicLong();
	private final CloudProvider provider;
	private final CloudPath uploadDir;

	@Inject
	public OpenDirFactory(CloudProvider provider, CloudPath uploadDir) {
		this.provider = provider;
		this.uploadDir = uploadDir;
	}

	/**
	 * @param path path of the dir to open
	 * @return file handle used to identify and close open files.
	 */
	public long open(CloudPath path) {
		long fileHandle = fileHandleGen.getAndIncrement();
		Predicate<String> listingFilter;
		if (path.equals(path.getRoot())) {
			listingFilter = s -> !uploadDir.getFileName().toString().equals(s);
		} else {
			listingFilter = s -> true;
		}
		OpenDir dir = new OpenDir(provider, listingFilter, path);

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
