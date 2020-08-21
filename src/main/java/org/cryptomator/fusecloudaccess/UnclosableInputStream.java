package org.cryptomator.fusecloudaccess;

import java.io.FilterInputStream;
import java.io.InputStream;

/**
 * Used to prevent close of wrapped InputStream, whose life cycle is managed internally and must not be interfered by external invocations.
 */
class UnclosableInputStream extends FilterInputStream {

	public UnclosableInputStream(InputStream in) {
		super(in);
	}

	@Override
	public void close() {
		// no-op
	}

}
