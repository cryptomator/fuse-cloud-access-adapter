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
	private static final CloudPath ROOT_DIR = CloudPath.of("/");

	private final ConcurrentMap<Long, OpenDir> openDirs = new ConcurrentHashMap<>();
	private final AtomicLong fileHandleGen = new AtomicLong();
	private final CloudProvider provider;
	private final CloudPath uploadDir;

	@Inject
	public OpenDirFactory(CloudProvider provider, CloudAccessFSConfig config) {
		this.provider = provider;
		this.uploadDir = config.getUploadDir();
	}

	/**
	 * @param path path of the dir to open
	 * @return file handle used to identify and close open files.
	 */
	public long open(CloudPath path) {
		long fileHandle = fileHandleGen.getAndIncrement();
		String uploadDirName = Optional.ofNullable(uploadDir.getFileName()).map(CloudPath::toString).orElse(null);
		Predicate<String> listingFilter = ROOT_DIR.equals(path)
				? childName -> !childName.equals(uploadDirName) // exclude uploadDir from child list
				: childName -> true; // include all children
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
