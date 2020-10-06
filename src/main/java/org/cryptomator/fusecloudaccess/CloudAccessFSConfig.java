package org.cryptomator.fusecloudaccess;

import org.cryptomator.cloudaccess.api.CloudPath;

import javax.inject.Inject;
import java.nio.file.Path;

@FileSystemScoped
public class CloudAccessFSConfig {

	private static final int DEFAULT_PENDING_UPLOAD_TIMEOUT = 10;
	private static final long DEFAULT_TOTAL_QUOTA = 1_000_000_000; // 1 GB
	private static final long DEFAULT_AVAILABLE_QUOTA = 500_000_000; // 500 MB
	private static final int DEFAULT_IDLE_FILE_TIMEOUT = 20;
	private static final String DEFAULT_CACHE_DIR = System.getProperty("java.io.tmpdir") + "/fcaCache";
	private static final String DEFAULT_LOST_AND_FOUND_DIR = "lostAndFound";
	private static final String DEFAULT_UPLOAD_DIR = "/58a230a40ae05cee64dfc0680d920e1e";

	private final int pendingUploadTimeoutSeconds;
	private final long totalQuota;
	private final long availableQuota;
	private final int idleFileTimeoutSeconds;
	private final String cacheDir;
	private final String lostAndFoundDir;
	private final String uploadDir;

	@Inject
	CloudAccessFSConfig() {
		this.pendingUploadTimeoutSeconds = Integer.getInteger("org.cryptomator.fusecloudaccess.pendingUploadsTimeoutSeconds", DEFAULT_PENDING_UPLOAD_TIMEOUT);
		this.idleFileTimeoutSeconds = Integer.getInteger("org.cryptomator.fusecloudaccess.idleFileTimeoutSeconds", DEFAULT_IDLE_FILE_TIMEOUT);
		this.totalQuota = Long.getLong("org.cryptomator.fusecloudaccess.totalQuota", DEFAULT_TOTAL_QUOTA);
		this.availableQuota = Long.getLong("org.cryptomator.fusecloudaccess.availableQuota", DEFAULT_AVAILABLE_QUOTA);
		this.cacheDir = System.getProperty("org.cryptomator.fusecloudaccess.cacheDir", DEFAULT_CACHE_DIR);
		this.lostAndFoundDir = System.getProperty("org.cryptomator.fusecloudaccess.lostAndFoundDir", DEFAULT_LOST_AND_FOUND_DIR);
		this.uploadDir = System.getProperty("org.cryptomator.fusecloudaccess.uploadDir", DEFAULT_UPLOAD_DIR);
	}

	public int getPendingUploadTimeoutSeconds() {
		return pendingUploadTimeoutSeconds;
	}

	public int getIdleFileTimeoutSeconds() {
		return idleFileTimeoutSeconds;
	}

	public long getTotalQuota() {
		return totalQuota;
	}

	public long getAvailableQuota() {
		return availableQuota;
	}

	public Path getCacheDir() {
		return Path.of(cacheDir);
	}

	public Path getLostAndFoundDir() {
		return Path.of(lostAndFoundDir);
	}

	public CloudPath getUploadDir() {
		return CloudPath.of(uploadDir);
	}

}
