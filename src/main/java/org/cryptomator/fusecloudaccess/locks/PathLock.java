package org.cryptomator.fusecloudaccess.locks;

public interface PathLock extends AutoCloseable {

	DataLock lockDataForReading();

	DataLock lockDataForWriting();

	@Override
	void close();

}
