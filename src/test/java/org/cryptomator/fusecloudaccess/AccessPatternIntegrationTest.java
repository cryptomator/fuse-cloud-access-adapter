package org.cryptomator.fusecloudaccess;

import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import jnr.ffi.provider.jffi.ByteBufferMemoryIO;
import org.cryptomator.cloudaccess.CloudAccess;
import org.cryptomator.cloudaccess.api.CloudPath;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.impl.SimpleLogger;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;

import static java.nio.charset.StandardCharsets.US_ASCII;

public class AccessPatternIntegrationTest {

	static {
		System.setProperty("java.library.path", "/usr/local/lib/");
		System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "debug");
		System.setProperty(SimpleLogger.SHOW_DATE_TIME_KEY, "true");
		System.setProperty(SimpleLogger.DATE_TIME_FORMAT_KEY, "HH:mm:ss.SSS");
	}

	private Path mirrored;
	private Path cacheDir;
	private Path lostNFound;
	private CloudProvider provider;
	private CloudAccessFS fs;

	@BeforeEach
	void setup(@TempDir Path tmpDir) throws IOException {
		this.mirrored = tmpDir.resolve("mirrored");
		this.cacheDir = tmpDir.resolve("cache");
		this.lostNFound = tmpDir.resolve("lost+Found");
		Files.createDirectory(this.mirrored);
		Files.createDirectory(this.cacheDir);
		System.setProperty("org.cryptomator.fusecloudaccess.cacheDir", cacheDir.toString());
		System.setProperty("org.cryptomator.fusecloudaccess.lostAndFoundDir", lostNFound.toString());
		this.provider = CloudAccess.toLocalFileSystem(this.mirrored);
		this.fs = CloudAccessFS.createNewFileSystem(provider);

		fs.init(null);
	}

	@Test
	@Disabled // requires java.library.path to be set
	@DisplayName("simulate TextEdit.app's access pattern during save")
	void testAppleAutosaveAccessPattern() throws IOException, InterruptedException {
		// echo "asd" > foo.txt
		FuseFileInfo fi1 = TestFileInfo.create();
		fs.create("/foo.txt", 0644, fi1);
		fs.write("/foo.txt", mockPointer(US_ASCII.encode("asd")), 3, 0, fi1);
		Assertions.assertTrue(Files.exists(mirrored.resolve("foo.txt")));

		// mkdir foo.txt-temp3000
		fs.mkdir("/foo.txt-temp3000", 0755);

		// wait a bit (so that we can check if st_mtim updated)
		Thread.sleep(100);

		// echo "asdasd" > foo.txt-temp3000/foo.txt
		FuseFileInfo fi2 = TestFileInfo.create();
		fs.create("/foo.txt-temp3000/foo.txt", 0644, fi2);
		fs.write("/foo.txt-temp3000/foo.txt", mockPointer(US_ASCII.encode("asdasd")), 6, 0, fi2);
		Assertions.assertTrue(Files.exists(mirrored.resolve("foo.txt-temp3000/foo.txt")));

		// check updated metadata:
		TestFileStat stat1 = TestFileStat.create();
		TestFileStat stat2 = TestFileStat.create();
		fs.getattr("/foo.txt", stat1);
		fs.getattr("/foo.txt-temp3000/foo.txt", stat2);
		Instant modTime1 = Instant.ofEpochSecond(stat1.st_mtim.tv_sec.longValue(), stat1.st_mtim.tv_nsec.longValue());
		Instant modTime2 = Instant.ofEpochSecond(stat2.st_mtim.tv_sec.longValue(), stat2.st_mtim.tv_nsec.longValue());
		Assertions.assertEquals(3, stat1.st_size.intValue());
		Assertions.assertEquals(6, stat2.st_size.intValue());
		Assertions.assertTrue(modTime1.isBefore(modTime2), "modified date of stat1 is before stat2");

		// mv foo.txt foo.txt-temp3001
		fs.rename("/foo.txt", "/foo.txt-temp3001");

		// mv foo.txt-temp3000/foo.txt foo.txt
		fs.rename("/foo.txt-temp3000/foo.txt", "/foo.txt");
		fs.release("/foo.txt-temp3000/foo.txt", fi2);

		// rm -r foo.txt-temp3000
		fs.rmdir("/foo.txt-temp3000");
		Assertions.assertTrue(Files.notExists(mirrored.resolve("foo.txt-temp3000")));

		// rm foo.txt-temp3001
		fs.release("/foo.txt", fi1);
		fs.unlink("/foo.txt-temp3001");
		Assertions.assertTrue(Files.notExists(mirrored.resolve("foo.txt-temp3001")));

		// cat foo.txt == "asdasd"
		ByteBuffer buf = ByteBuffer.allocate(7);
		FuseFileInfo fi3 = TestFileInfo.create();
		fs.open("/foo.txt", fi3);
		int numRead = fs.read("/foo.txt", mockPointer(buf), 7, 0, fi3);
		fs.release("/foo.txt", fi3);
		Assertions.assertEquals(6, numRead);
		Assertions.assertArrayEquals("asdasd".getBytes(US_ASCII), Arrays.copyOf(buf.array(), numRead));
	}

	@Test
	@Disabled // requires java.library.path to be set
	@DisplayName("simulate Pages.app's access pattern during save")
	void testApplePagesSaveAccessPattern() throws InterruptedException {
		// echo "asd" > foo.txt
		FuseFileInfo fi1 = TestFileInfo.create();
		fs.create("/foo.txt", 0644, fi1);
		fs.write("/foo.txt", mockPointer(US_ASCII.encode("asd")), 3, 0, fi1);
		fs.release("/foo.txt", fi1);
		Assertions.assertTrue(Files.exists(mirrored.resolve("foo.txt")));

		// open for no reason:
		FuseFileInfo fi2 = TestFileInfo.create();
		fs.open("/foo.txt", fi2);

		// check metadata:
		TestFileStat stat1 = TestFileStat.create();
		fs.getattr("/foo.txt", stat1);
		Instant modTime1 = Instant.ofEpochSecond(stat1.st_mtim.tv_sec.longValue(), stat1.st_mtim.tv_nsec.longValue());
		Assertions.assertEquals(3, stat1.st_size.intValue());

		// mkdir foo.txt-temp3000
		fs.mkdir("/foo.txt-temp3000", 0755);

		// wait a bit (so that we can check if st_mtim updated)
		Thread.sleep(100);

		// echo "asdasd" > foo.txt-temp3000/foo.txt
		FuseFileInfo fi3 = TestFileInfo.create();
		fs.create("/foo.txt-temp3000/foo.txt", 0644, fi3);
		fs.write("/foo.txt-temp3000/foo.txt", mockPointer(US_ASCII.encode("asdasd")), 6, 0, fi3);
		fs.release("/foo.txt-temp3000/foo.txt", fi3);
		Assertions.assertTrue(Files.exists(mirrored.resolve("foo.txt-temp3000/foo.txt")));

		// check updated metadata:
		TestFileStat stat2 = TestFileStat.create();
		TestFileStat stat3 = TestFileStat.create();
		fs.getattr("/foo.txt", stat2);
		fs.getattr("/foo.txt-temp3000/foo.txt", stat3);
		Instant modTime2 = Instant.ofEpochSecond(stat2.st_mtim.tv_sec.longValue(), stat2.st_mtim.tv_nsec.longValue());
		Instant modTime3 = Instant.ofEpochSecond(stat3.st_mtim.tv_sec.longValue(), stat3.st_mtim.tv_nsec.longValue());
		Assertions.assertEquals(3, stat2.st_size.intValue());
		Assertions.assertEquals(6, stat3.st_size.intValue());
		Assertions.assertEquals(modTime1, modTime2);
		Assertions.assertTrue(modTime2.isBefore(modTime3), "modified date of stat2 is before stat3");

		// mv foo.txt foo.txt-temp3001
		fs.rename("/foo.txt", "/foo.txt-temp3001");

		// mv foo.txt-temp3000/foo.txt foo.txt
		fs.rename("/foo.txt-temp3000/foo.txt", "/foo.txt");

		// rm -r foo.txt-temp3000
		fs.rmdir("/foo.txt-temp3000");
		Assertions.assertTrue(Files.notExists(mirrored.resolve("foo.txt-temp3000")));

		// release useless handle:
		fs.release("/foo.txt-temp3001", fi2);

		// rm foo.txt-temp3001
		fs.unlink("/foo.txt-temp3001");
		Assertions.assertTrue(Files.notExists(mirrored.resolve("foo.txt-temp3001")));

		// cat foo.txt == "asdasd"
		ByteBuffer buf = ByteBuffer.allocate(7);
		FuseFileInfo fi4 = TestFileInfo.create();
		fs.open("/foo.txt", fi4);
		int numRead = fs.read("/foo.txt", mockPointer(buf), 7, 0, fi4);
		// do not release yet!
		Assertions.assertEquals(6, numRead);
		Assertions.assertArrayEquals("asdasd".getBytes(US_ASCII), Arrays.copyOf(buf.array(), numRead));

		// re-check metadata:
		TestFileStat stat4 = TestFileStat.create();
		fs.getattr("/foo.txt", stat4);
		Instant modTime4 = Instant.ofEpochSecond(stat4.st_mtim.tv_sec.longValue(), stat4.st_mtim.tv_nsec.longValue());
		Assertions.assertEquals(modTime3, modTime4);

		// now release:
		fs.release("/foo.txt", fi4);
	}

	@Test
	@Disabled // requires java.library.path to be set
	@DisplayName("simulates Notepad's access pattern during save of a new File")
	void testWindowsNotepadSavePatternForNewFiles() {
		FuseFileInfo fi1 = TestFileInfo.create();
		FileStat stat = TestFileStat.create();
		String p = "/foo.txt";
		int code = fs.getattr(p, stat);

		assert code == -ErrorCodes.ENOENT();

		fs.create(p, 0644, fi1);
		fs.release(p, fi1);

		FuseFileInfo fi2 = TestFileInfo.create();
		fs.open(p, fi2);
		fs.unlink(p);
		fs.release(p, fi2);

		FuseFileInfo fi3 = TestFileInfo.create();
		fs.create(p, 0644, fi3);
		Assertions.assertEquals(0, fs.getattr(p, stat));
	}

	@Test
	@Disabled // requires java.library.path to be set
	@DisplayName("simulates \"get attributes during write\" access pattern")
	void testGetAttributesDuringWrite() {
		provider = CloudAccess.vaultFormat8GCMCloudAccess(CloudAccess.toLocalFileSystem(this.mirrored), CloudPath.of("/"), Base64.getDecoder().decode("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=="));
		fs = CloudAccessFS.createNewFileSystem(provider);

		var path = "/foo.txt";

		// echo "asd" > foo.txt
		FuseFileInfo fi1 = TestFileInfo.create();
		fs.create(path, 0644, fi1);
		fs.write(path, mockPointer(US_ASCII.encode("asd")), 3, 0, fi1);

		TestFileStat stat1 = TestFileStat.create();
		fs.getattr(path, stat1);
		Assertions.assertEquals(3, stat1.st_size.intValue());

		fs.write(path, mockPointer(US_ASCII.encode("asd")), 3, 3, fi1);

		fs.getattr(path, stat1);
		Assertions.assertEquals(6, stat1.st_size.intValue());

		fs.release(path, fi1);
	}

	private Pointer mockPointer(ByteBuffer buf) {
		return new ByteBufferMemoryIO(Runtime.getSystemRuntime(), buf);
	}
}
