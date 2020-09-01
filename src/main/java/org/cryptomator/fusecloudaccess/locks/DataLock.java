package org.cryptomator.fusecloudaccess.locks;

public interface DataLock extends AutoCloseable {

	@Override
	void close();
}
