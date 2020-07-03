package org.cryptomator.fusecloudaccess;

import java.time.Instant;

import jnr.posix.util.Platform;
import org.cryptomator.cloudaccess.api.CloudItemMetadata;
import ru.serce.jnrfuse.struct.FileStat;

class Attributes {

	// uid/gid are overwritten by fuse mount options -ouid=...
	private static final int DUMMY_UID = 65534; // usually nobody
	private static final int DUMMY_GID = 65534; // usually nobody

	public static void copy(CloudItemMetadata metadata, FileStat stat) {
		switch (metadata.getItemType()) {
			case FILE:
				stat.st_mode.set(FileStat.S_IFREG | 0640);
				break;
			case FOLDER:
				stat.st_mode.set(FileStat.S_IFDIR | 0750);
				break;
			default:
				stat.st_mode.set(0);
				break;
		}
		stat.st_uid.set(DUMMY_UID);
		stat.st_gid.set(DUMMY_GID);
		stat.st_size.set(metadata.getSize().orElse(0l));
		var mTime = metadata.getLastModifiedDate().orElse(Instant.EPOCH);
		stat.st_mtim.tv_sec.set(mTime.getEpochSecond());
		stat.st_mtim.tv_nsec.set(mTime.getNano());
		if (Platform.IS_MAC || Platform.IS_WINDOWS) {
			assert stat.st_birthtime != null;
			stat.st_birthtime.tv_sec.set(mTime.getEpochSecond());
			stat.st_birthtime.tv_nsec.set(mTime.getNano());
		}
		stat.st_nlink.set(1);
		// make sure to nil certain fields known to contain garbage from uninitialized memory
		// fixes alleged permission bugs, see https://github.com/cryptomator/fuse-nio-adapter/issues/19
		if (Platform.IS_MAC) {
			stat.st_flags.set(0);
			stat.st_gen.set(0);
		}
	}
}
