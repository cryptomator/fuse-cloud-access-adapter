package org.cryptomator.fusecloudaccess;

import jnr.ffi.Runtime;
import ru.serce.jnrfuse.struct.FuseFileInfo;

public class TestFileInfo extends FuseFileInfo {

	protected TestFileInfo(Runtime runtime) {
		super(runtime);
	}

	public static TestFileInfo create(){
		return new TestFileInfo(Runtime.getSystemRuntime());
	}

}
