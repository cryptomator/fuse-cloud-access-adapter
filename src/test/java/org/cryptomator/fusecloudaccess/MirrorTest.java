package org.cryptomator.fusecloudaccess;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.UUID;

import com.google.common.base.Preconditions;
import org.cryptomator.cloudaccess.api.CloudPath;
import org.cryptomator.cloudaccess.localfs.LocalFsCloudProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.SimpleLogger;

public class MirrorTest {

	static {
		System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "debug");
		System.setProperty(SimpleLogger.SHOW_DATE_TIME_KEY, "true");
		System.setProperty(SimpleLogger.DATE_TIME_FORMAT_KEY, "HH:mm:ss.SSS");
	}

	private static final Logger LOG = LoggerFactory.getLogger(MirrorTest.class);
	private static final String OS_NAME = System.getProperty("os.name").toLowerCase();
	private static final Path USER_HOME = Path.of(System.getProperty("user.home"));

	public static class MacMirror {

		public static void main(String args[]) {
			Preconditions.checkState(OS_NAME.contains("mac"), "Test designed to run on macOS.");

			try (Scanner scanner = new Scanner(System.in)) {
				System.out.println("Enter path to the directory you want to mirror:");
				Path p = Path.of(scanner.nextLine());
				Path m = Path.of("/Volumes/" + UUID.randomUUID().toString());
				Path c = Files.createTempDirectory("cache");
				var cloudAccessProvider = new LocalFsCloudProvider(p);
				var fs = CloudAccessFS.createNewFileSystem(cloudAccessProvider,1000, c, CloudPath.of("/tmpUploadDir")) ;
				var flags = new String[] {
						"-ouid=" + Files.getAttribute(USER_HOME, "unix:uid"),
						"-ogid=" + Files.getAttribute(USER_HOME, "unix:gid"),
						"-oatomic_o_trunc",
						"-oauto_xattr",
						"-oauto_cache",
						"-ovolname=CloudAccessMirror",
						"-omodules=iconv,from_code=UTF-8,to_code=UTF-8-MAC", // show files names in Unicode NFD encoding
						"-onoappledouble", // vastly impacts performance for some reason...
						"-odefault_permissions" // let the kernel assume permissions based on file attributes etc
				};
				LOG.info("Mounting FUSE file system at {}...", m);
				fs.mount(m, true, true, flags);
				LOG.info("Unmounted {}.", m);
			} catch (IOException e) {
				LOG.error("mount failed", e);
			}
		}

	}

	public static class WinMirror {

		public static void main(String args[]) {
			Preconditions.checkState(OS_NAME.contains("windows"), "Test designed to run on windows.");

			try (Scanner scanner = new Scanner(System.in)) {
				System.out.println("Enter path to the directory you want to mirror:");
				Path p = Paths.get(scanner.nextLine());
				System.out.println("Enter path to the not-existent directory you want to use as mountpoint :");
				Path m = Paths.get(scanner.nextLine());
				Path c = Files.createTempDirectory("cache");
				var cloudAccessProvider = new LocalFsCloudProvider(p);
				CloudAccessFSComponent.Builder builder;
				var fs = CloudAccessFS.createNewFileSystem(cloudAccessProvider,1000, c, CloudPath.of("/tmpUploadDir"));
				var flags = new String[] {
						"-ouid=-1",
						"-ogid=-1",
						"-oauto_unmount",
						"-oauto_cache",
						"-ovolname=CloudAccessMirror",
						"-odefault_permissions" // let the kernel assume permissions based on file attributes etc
				};
				//mount
				LOG.info("Mounting FUSE file system at {}...", m);
				fs.mount(m, false, true, flags);
				//wait
				LOG.info("Type anything and hit enter to stop.");
				scanner.nextLine();
				//unmount
				fs.umount(); //we are not blocking, therefore need to explicitly call umount
				LOG.info("Unmounted {}.", m);
			} catch (IOException e) {
				LOG.error("mount failed", e);
			}
		}
	}

}
