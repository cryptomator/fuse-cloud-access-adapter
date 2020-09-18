package org.cryptomator.fusecloudaccess;

import org.cryptomator.cloudaccess.api.CloudPath;

import javax.inject.Inject;
import java.nio.file.Path;

@FileSystemScoped
public class CloudAccessFSConfig {

	private static final int DEFAULT_RESPONSE_TIMEOUT = 10;
	private static final int DEFAULT_PENDING_UPLOAD_TIMEOUT = 10;
	private static final int DEFAULT_IDLE_FILE_TIMEOUT = 20;
	private static final String DEFAULT_CACHE_DIR = System.getProperty("java.io.tmpdir") + "/fcaCache";
	private static final String DEFAULT_LOST_AND_FOUND_DIR = "lostAndFound";
	private static final String DEFAULT_UPLOAD_DIR = "/58a230a40ae05cee64dfc0680d920e1e";

	private final int providerResponseTimeoutSeconds;
	private final int pendingUploadTimeoutSeconds;
	private final int idleFileTimeoutSeconds;
	private final String cacheDir;
	private final String lostAndFoundDir;
	private final String uploadDir;

	@Inject
	CloudAccessFSConfig() {
		this.providerResponseTimeoutSeconds = Integer.getInteger("org.cryptomator.fusecloudaccess.responseTimeoutSeconds", DEFAULT_RESPONSE_TIMEOUT);
		this.pendingUploadTimeoutSeconds = Integer.getInteger("org.cryptomator.fusecloudaccess.pendingUploadsTimeoutSeconds", DEFAULT_PENDING_UPLOAD_TIMEOUT);
		this.idleFileTimeoutSeconds = Integer.getInteger("org.cryptomator.fusecloudaccess.idleFileTimeoutSeconds", DEFAULT_IDLE_FILE_TIMEOUT);
		this.cacheDir = System.getProperty("org.cryptomator.fusecloudaccess.cacheDir", DEFAULT_CACHE_DIR);
		this.lostAndFoundDir = System.getProperty("org.cryptomator.fusecloudaccess.lostAndFoundDir", DEFAULT_LOST_AND_FOUND_DIR);
		this.uploadDir = System.getProperty("org.cryptomator.fusecloudaccess.uploadDir", DEFAULT_UPLOAD_DIR);
	}

	public int getProviderResponseTimeoutSeconds() {
		return providerResponseTimeoutSeconds;
	}

	public int getPendingUploadTimeoutSeconds() {
		return pendingUploadTimeoutSeconds;
	}

	public int getIdleFileTimeoutSeconds() {
		return idleFileTimeoutSeconds;
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
