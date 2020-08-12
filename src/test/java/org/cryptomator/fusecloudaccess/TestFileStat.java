package org.cryptomator.fusecloudaccess;

import jnr.ffi.Runtime;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

public class TestFileStat extends FileStat {

	protected TestFileStat(Runtime runtime) {
		super(runtime);
	}

	public static TestFileStat create(){
		return new TestFileStat(Runtime.getSystemRuntime());
	}

}
