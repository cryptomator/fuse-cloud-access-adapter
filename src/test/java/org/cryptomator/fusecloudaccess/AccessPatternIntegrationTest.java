package org.cryptomator.fusecloudaccess;

import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import jnr.ffi.provider.jffi.ByteBufferMemoryIO;
import org.cryptomator.cloudaccess.CloudAccess;
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
import java.util.Arrays;

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
	private CloudProvider provider;
	private CloudAccessFS fs;

	@BeforeEach
	void setup(@TempDir Path tmpDir) throws IOException {
		this.mirrored = tmpDir.resolve("mirrored");
		this.cacheDir = tmpDir.resolve("cache");
		Files.createDirectory(this.mirrored);
		Files.createDirectory(this.cacheDir);
		this.provider = CloudAccess.toLocalFileSystem(this.mirrored);
		this.fs = new CloudAccessFS(provider, this.cacheDir, 1000);
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
		Assertions.assertEquals(3, stat1.st_size.intValue());
		Assertions.assertEquals(6, stat2.st_size.intValue());
		Assertions.assertTrue(stat1.st_mtim.tv_nsec.longValue() < stat2.st_mtim.tv_nsec.longValue(), "modified date of stat1 is before stat2");

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
	@DisplayName("simulates Notepad's access pattern during save of a new File")
	void testWindowsNotepadSavePatternForNewFiles() {
		FuseFileInfo fi1 = TestFileInfo.create();
		FileStat stat = TestFileStat.create();
		String p = "/foo.txt";
		int code = fs.getattr(p,stat);

		assert code == -ErrorCodes.ENOENT();

		fs.create(p,0644,fi1);
		fs.release(p,fi1);

		FuseFileInfo fi2 = TestFileInfo.create();
		fs.open(p,fi2);
		fs.unlink(p);
		fs.release(p,fi2);

		FuseFileInfo fi3 = TestFileInfo.create();
		fs.create(p,0644, fi3);
		Assertions.assertEquals(0, fs.getattr(p,stat));
	}

	private Pointer mockPointer(ByteBuffer buf) {
		return new ByteBufferMemoryIO(Runtime.getSystemRuntime(), buf);
	}
}
