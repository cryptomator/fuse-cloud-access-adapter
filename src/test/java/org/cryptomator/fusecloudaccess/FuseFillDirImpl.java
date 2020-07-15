package org.cryptomator.fusecloudaccess;

import jnr.ffi.Pointer;
import ru.serce.jnrfuse.FuseFillDir;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FuseFillDirImpl implements FuseFillDir {

	private static final FuseFillDirImpl empty = new FuseFillDirImpl(true);

	private final List<String> listing;

	private FuseFillDirImpl(boolean empty) {
		if (empty) {
			this.listing = Collections.emptyList();
		} else {
			this.listing = new ArrayList<>();
		}
	}

	@Override
	public int apply(Pointer pointer, ByteBuffer byteBuffer, Pointer pointer1, long l) {
		try {
			listing.add(new String(byteBuffer.array()));
			return 0;
		} catch (UnsupportedOperationException e) {
			return 1;
		}
	}

	public static FuseFillDirImpl getENOMEMFiller() {
		return empty;
	}

	public static FuseFillDirImpl getUnlimitedFiller() {
		return new FuseFillDirImpl(false);
	}

	public List<String> getListing() {
		return listing;
	}
}
