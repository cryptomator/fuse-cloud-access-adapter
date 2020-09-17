package org.cryptomator.fusecloudaccess;

import org.cryptomator.cloudaccess.api.CloudPath;

import java.nio.file.Path;

public class CloudAccessFSConfig {

	private static final int DEFAULT_CACHE_TIMEOUT = 10;
	private static final int DEFAULT_RESPONSE_TIMEOUT = 1000;
	private static final int DEFAULT_PENDING_UPLOAD_TIMEOUT = 10;

	private final int providerResponseTimeoutMillis;
	private final int pendingUploadTimeoutSeconds;
	private final Path cacheDir;
	private final int cacheTimeoutSeconds;
	private final Path lostNFoundDir;
	private final CloudPath uploadDir;

	private CloudAccessFSConfig(int providerResponseTimeoutMillis, int pendingUploadTimeoutSeconds, Path cacheDir, int cacheTimeoutSeconds, Path lostNFoundDir, CloudPath uploadDir) {
		this.providerResponseTimeoutMillis = providerResponseTimeoutMillis;
		this.pendingUploadTimeoutSeconds = pendingUploadTimeoutSeconds;
		this.cacheDir = cacheDir;
		this.cacheTimeoutSeconds = cacheTimeoutSeconds;
		this.lostNFoundDir = lostNFoundDir;
		this.uploadDir = uploadDir;
	}

	public int getProviderResponseTimeoutMillis() {
		return providerResponseTimeoutMillis;
	}

	public int getPendingUploadTimeoutSeconds() {
		return pendingUploadTimeoutSeconds;
	}

	public Path getCacheDir() {
		return cacheDir;
	}

	public int getCacheTimeoutSeconds() {
		return cacheTimeoutSeconds;
	}

	public Path getLostNFoundDir() {
		return lostNFoundDir;
	}

	public CloudPath getUploadDir() {
		return uploadDir;
	}

	public Builder newConfigBuilder() {
		return new Builder(); //TODO: are there useful defaults for the Directories? If not, we should fix 'em right here by adding paramerters to sig
	}

	private static class Builder {

		private int providerResponseTimeoutMillis;
		private int pendingUploadTimeoutSeconds;
		private int cacheTimeoutSeconds;
		private Path cacheDir;
		private Path lostNFoundDir;
		private CloudPath uploadDir;

		private Builder() {
			this.providerResponseTimeoutMillis = DEFAULT_RESPONSE_TIMEOUT;
			this.pendingUploadTimeoutSeconds = DEFAULT_PENDING_UPLOAD_TIMEOUT;
			this.cacheTimeoutSeconds = DEFAULT_CACHE_TIMEOUT;
		}

		Builder setPendingUploadTimeoutSeconds(int pendingUploadTimeoutSeconds) {
			this.pendingUploadTimeoutSeconds = pendingUploadTimeoutSeconds;
			return this;
		}

		Builder setCacheDir(Path cacheDir) {
			this.cacheDir = cacheDir;
			return this;
		}

		Builder setCacheTimeoutSeconds(int cacheTimeoutSeconds) {
			this.cacheTimeoutSeconds = cacheTimeoutSeconds;
			return this;
		}

		Builder setLostNFoundDir(Path lostNFoundDir) {
			this.lostNFoundDir = lostNFoundDir;
			return this;
		}

		Builder setUploadDir(CloudPath uploadDir) {
			this.uploadDir = uploadDir;
			return this;
		}

		Builder setProviderResponseTimeoutMillis(int providerResponseTimeoutMillis) {
			this.providerResponseTimeoutMillis = providerResponseTimeoutMillis;
			return this;
		}

		public CloudAccessFSConfig build() {
			return new CloudAccessFSConfig(providerResponseTimeoutMillis, pendingUploadTimeoutSeconds, cacheDir, cacheTimeoutSeconds, lostNFoundDir, uploadDir);
		}
	}
}
