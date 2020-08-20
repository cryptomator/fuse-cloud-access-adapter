package org.cryptomator.fusecloudaccess.locks;

public interface PathLockBuilder {

	PathLock forReading();

	PathLock forWriting();

}
