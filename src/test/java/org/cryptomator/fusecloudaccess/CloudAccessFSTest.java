package org.cryptomator.fusecloudaccess;

import jnr.constants.platform.OpenFlags;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import org.cryptomator.cloudaccess.api.CloudItemMetadata;
import org.cryptomator.cloudaccess.api.CloudItemType;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.ProgressListener;
import org.cryptomator.cloudaccess.api.exceptions.AlreadyExistsException;
import org.cryptomator.cloudaccess.api.exceptions.CloudProviderException;
import org.cryptomator.cloudaccess.api.exceptions.NotFoundException;
import org.cryptomator.cloudaccess.api.exceptions.TypeMismatchException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class CloudAccessFSTest {

	private static final Runtime RUNTIME = Runtime.getSystemRuntime();
	private static final Path PATH = Path.of("some/path/to/resource");
	private static final int TIMEOUT = 100;

	private CloudAccessFS cloudFs;
	private CloudProvider provider;
	private OpenFileFactory fileFactory;
	private OpenDirFactory dirFactory;

	@BeforeAll
	public static void prepare() {
		if (OS.MAC.isCurrentOs()) {
			// otherwise dlopen("libfuse") fails
			System.setProperty("java.library.path", "/usr/local/lib");
		}
	}

	@BeforeEach
	public void setup() {
		provider = Mockito.mock(CloudProvider.class);
		fileFactory = Mockito.mock(OpenFileFactory.class);
		dirFactory = Mockito.mock(OpenDirFactory.class);
		cloudFs = new CloudAccessFS(provider, CloudAccessFSTest.TIMEOUT, fileFactory, dirFactory);
	}

	@DisplayName("test returnOrTimeout() returns expected result on regular execution")
	@Test
	public void testRegular() {
		int expectedResult = 1337;
		var future = CompletableFuture.completedFuture(expectedResult);
		Assertions.assertEquals(expectedResult, cloudFs.returnOrTimeout(future));
	}

	@DisplayName("test returnOrTimeout() returns EINTR on interrupt")
	@Test
	public void testInterrupt() throws InterruptedException {
		AtomicInteger actualResult = new AtomicInteger();
		Thread t = new Thread(() -> {
			actualResult.set(cloudFs.returnOrTimeout(new CompletableFuture<>()));
		});
		t.start();
		t.interrupt();
		t.join();
		Assertions.assertEquals(-ErrorCodes.EINTR(), actualResult.get());
	}

	@DisplayName("test returnOrTimeout() returns EIO on ExecutionException")
	@Test
	public void testExecution() {
		CompletableFuture future = CompletableFuture.failedFuture(new Exception());
		Assertions.assertEquals(-ErrorCodes.EIO(), cloudFs.returnOrTimeout(future));
	}

	@DisplayName("test returnOrTimeout() return ETIMEDOUT on timeout")
	@Test
	public void testTimeout() {
		Assertions.assertEquals(-ErrorCodes.ETIMEDOUT(), cloudFs.returnOrTimeout(new CompletableFuture<>()));
	}

	@Nested
	class GetAttrTests {

		private FileStat fileStat;

		@BeforeEach
		public void setup() {
			fileStat = new FileStat(RUNTIME);
		}

		@DisplayName("getattr() returns 0 on success")
		@Test
		public void testGetAttrSuccess() {
			CloudItemMetadata itemMetadata = Mockito.mock(CloudItemMetadata.class);
			Mockito.when(itemMetadata.getPath()).thenReturn(PATH);
			Mockito.when(itemMetadata.getItemType()).thenReturn(CloudItemType.FILE);
			Mockito.when(itemMetadata.getName()).thenReturn(PATH.getFileName().toString());
			Mockito.when(provider.itemMetadata(PATH)).thenReturn(CompletableFuture.completedFuture(itemMetadata));

			var result = cloudFs.getattr(PATH.toString(), fileStat);

			Assertions.assertEquals(0, result);
		}

		@DisplayName("getattr() returns ENOENT when resource is not found.")
		@Test
		public void testGetAttrReturnsENOENTIfNotFound() {
			Mockito.when(provider.itemMetadata(PATH)).thenReturn(CompletableFuture.failedFuture(new NotFoundException()));

			var result = cloudFs.getattr(PATH.toString(), fileStat);

			Assertions.assertEquals(-ErrorCodes.ENOENT(), result);
		}

		@ParameterizedTest(name = "getattr() returns EIO on any other exception (expected or not)")
		@ValueSource(classes = {CloudProviderException.class, RuntimeException.class})
		public void testGetAttrReturnsEIOOnException(Class<Exception> exceptionClass) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
			Exception e = exceptionClass.getDeclaredConstructor().newInstance();
			Mockito.when(provider.itemMetadata(PATH)).thenReturn(CompletableFuture.failedFuture(e));

			var result = cloudFs.getattr(PATH.toString(), fileStat);

			Assertions.assertEquals(-ErrorCodes.EIO(), result);
		}

	}

	@Nested
	class OpenDirTest {

		private FuseFileInfo fi;

		@BeforeEach
		public void setup() {
			fi = TestFileInfo.create();
		}

		@DisplayName("opendir() returns 0 on success and sets the file handle")
		@Test
		public void testSuccessReturnsZeroAndSetsHandle() {
			long expectedHandle = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
			fi.fh.set(0L);
			var itemMetadata = Mockito.mock(CloudItemMetadata.class);
			Mockito.when(itemMetadata.getItemType()).thenReturn(CloudItemType.FOLDER);
			Mockito.when(provider.itemMetadata(PATH))
					.thenReturn(CompletableFuture.completedFuture(itemMetadata));
			Mockito.when(dirFactory.open(PATH)).thenReturn(expectedHandle);

			var result = cloudFs.opendir(PATH.toString(), fi);

			Assertions.assertEquals(0, result);
			Assertions.assertEquals(expectedHandle, fi.fh.longValue());
		}

		@DisplayName("opendir() returns ENOENT when directory not found")
		@Test
		public void testNotFoundReturnsENOENT() {
			Mockito.when(provider.itemMetadata(PATH)).thenReturn(CompletableFuture.failedFuture(new NotFoundException()));

			var result = cloudFs.opendir(PATH.toString(), fi);

			Assertions.assertEquals(-ErrorCodes.ENOENT(), result);
		}

		@DisplayName("opendir() returns ENOTDIR when resource is not a directory")
		@Test
		public void testNotADirectoryReturnsENOTDIR() {
			var itemMetadata = Mockito.mock(CloudItemMetadata.class);
			Mockito.when(itemMetadata.getItemType()).thenReturn(CloudItemType.FILE);
			Mockito.when(provider.itemMetadata(PATH)).thenReturn(CompletableFuture.completedFuture(itemMetadata));

			var result = cloudFs.opendir(PATH.toString(), fi);

			Assertions.assertEquals(-ErrorCodes.ENOTDIR(), result);
		}

		@ParameterizedTest(name = "opendir() returns EIO on any other exception (expected or not)")
		@ValueSource(classes = {CloudProviderException.class, Exception.class})
		public void testGetAttrReturnsEIOOnException(Class<Exception> exceptionClass) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
			Exception e = exceptionClass.getDeclaredConstructor().newInstance();
			Mockito.when(provider.itemMetadata(PATH)).thenReturn(CompletableFuture.failedFuture(e));

			var result = cloudFs.opendir(PATH.toString(), fi);

			Assertions.assertEquals(-ErrorCodes.EIO(), result);
		}

	}

	@Nested
	class ReadDirTests {

		private Pointer buf;
		private FuseFileInfo fi;
		private OpenDir dir;

		@BeforeEach
		public void setup() {
			buf = Mockito.mock(Pointer.class);
			fi = TestFileInfo.create();
			dir = Mockito.mock(OpenDir.class);
		}

		@DisplayName("Successful readdir() returns 0")
		@Test
		public void testSuccess() {
			FuseFillDir filler = Mockito.mock(FuseFillDir.class);
			Mockito.when(dirFactory.get(Mockito.anyLong())).thenReturn(Optional.of(dir));
			Mockito.when(dir.list(buf, filler, 0)).thenReturn(CompletableFuture.completedFuture(0));

			var result = cloudFs.readdir(PATH.toString(), buf, filler, 0l, fi);

			Assertions.assertEquals(0, result);
			Mockito.verify(dir).list(buf, filler, 0);
		}

		@DisplayName("readdir() returns EOVERFLOW if offset is too large")
		@Test
		public void testOffsetExceedingIntegerRangeReturnsEOVERFLOW() {
			FuseFillDir filler = Mockito.mock(FuseFillDir.class);
			var result = cloudFs.readdir(PATH.toString(), buf, filler, Long.MAX_VALUE, fi);

			Assertions.assertEquals(-ErrorCodes.EOVERFLOW(), result);
		}

		@DisplayName("readdir() returns EBADF when directory not opened")
		@Test
		public void testNotOpenedReturnsEBADF() {
			Mockito.when(dirFactory.get(Mockito.anyLong())).thenReturn(Optional.empty());
			FuseFillDir filler = Mockito.mock(FuseFillDir.class);

			var result = cloudFs.readdir(PATH.toString(), buf, filler, 0l, fi);

			Assertions.assertEquals(-ErrorCodes.EBADF(), result);
		}

		@ParameterizedTest(name = "readdir() returns EIO on any other exception (expected or not)")
		@ValueSource(classes = {CloudProviderException.class, RuntimeException.class})
		public void testAnyExceptionReturnsEIO(Class<Exception> exceptionClass) throws ReflectiveOperationException {
			Exception e = exceptionClass.getDeclaredConstructor().newInstance();
			FuseFillDir filler = Mockito.mock(FuseFillDir.class);
			Mockito.when(dirFactory.get(Mockito.anyLong())).thenReturn(Optional.of(dir));
			Mockito.when(dir.list(buf, filler, 0)).thenReturn(CompletableFuture.failedFuture(e));

			var result = cloudFs.readdir(PATH.toString(), buf, filler, 0l, fi);

			Assertions.assertEquals(-ErrorCodes.EIO(), result);
		}

	}

	@Nested
	class OpenTest {

		private TestFileInfo fi;

		@BeforeEach
		public void setup() {
			fi = TestFileInfo.create();
		}

		@DisplayName("open() returns 0 in success and writes the handle to field FileInfo.fh")
		@Test
		public void testSuccessfulOpenReturnsZeroAndStoresHandle() {
			long expectedHandle = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
			fi.fh.set(0L);

			CloudItemMetadata itemMetadata = Mockito.mock(CloudItemMetadata.class);
			Mockito.when(itemMetadata.getItemType()).thenReturn(CloudItemType.FILE);

			Mockito.when(fileFactory.open(Mockito.any(Path.class), Mockito.anySet())).thenReturn(expectedHandle);
			Mockito.when(provider.itemMetadata(PATH)).thenReturn(CompletableFuture.completedFuture(itemMetadata));

			var result = cloudFs.open(PATH.toString(), fi);

			Assertions.assertEquals(0, result);
			Assertions.assertEquals(expectedHandle, fi.fh.get());
		}

		@DisplayName("open() returns EISDIR if the path points to a directory")
		@Test
		public void testFolderItemTypeReturnsEISDIR() {
			CloudItemMetadata itemMetadata = Mockito.mock(CloudItemMetadata.class);
			Mockito.when(itemMetadata.getItemType()).thenReturn(CloudItemType.FOLDER);

			Mockito.when(provider.itemMetadata(PATH)).thenReturn(CompletableFuture.completedFuture(itemMetadata));

			var result = cloudFs.open(PATH.toString(), fi);

			Assertions.assertEquals(-ErrorCodes.EISDIR(), result);
		}

		@DisplayName("open() returns EIO if the path points to an unknown resource")
		@Test
		public void testUnknownItemTypeReturnsEIO() {
			CloudItemMetadata itemMetadata = Mockito.mock(CloudItemMetadata.class);
			Mockito.when(itemMetadata.getItemType()).thenReturn(CloudItemType.UNKNOWN);

			Mockito.when(provider.itemMetadata(PATH)).thenReturn(CompletableFuture.completedFuture(itemMetadata));

			var result = cloudFs.open(PATH.toString(), fi);

			Assertions.assertEquals(-ErrorCodes.EIO(), result);
		}

		@DisplayName("open() returns ENOENT if the specified path is not found")
		@Test
		public void testNotFoundExceptionReturnsENOENT() {
			Mockito.when(provider.itemMetadata(PATH)).thenReturn(CompletableFuture.failedFuture(new NotFoundException()));

			var result = cloudFs.open(PATH.toString(), fi);

			Assertions.assertEquals(-ErrorCodes.ENOENT(), result);
		}

		@ParameterizedTest(name = "open() returns EIO on any other exception (expected or not)")
		@ValueSource(classes = {CloudProviderException.class, RuntimeException.class})
		public void testOpenReturnsEIOOnException(Class<Exception> exceptionClass) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
			Exception e = exceptionClass.getDeclaredConstructor().newInstance();
			Mockito.when(provider.itemMetadata(PATH)).thenReturn(CompletableFuture.failedFuture(e));

			var result = cloudFs.open(PATH.toString(), fi);

			Assertions.assertEquals(-ErrorCodes.EIO(), result);
		}
	}

	@Nested
	class ReadTest {

		private TestFileInfo fi;
		private OpenFile file;
		private Pointer buf;

		@BeforeEach
		public void setup() {
			fi = TestFileInfo.create();
			file = Mockito.mock(OpenFile.class);
			buf = Mockito.mock(Pointer.class);
		}

		@DisplayName("read() returns 0 on success")
		@Test
		public void testSuccessfulReadReturnsZero() {
			Mockito.when(fileFactory.get(Mockito.anyLong())).thenReturn(Optional.of(file));
			Mockito.when(file.read(buf, 1l, 2l)).thenReturn(CompletableFuture.completedFuture(0));

			var result = cloudFs.read(PATH.toString(), buf, 2l, 1l, fi);

			Assertions.assertEquals(0, result);
		}

		@DisplayName("read() returns ENOENT if resource is not found")
		@Test
		public void testNotFoundExceptionReturnsENOENT() {
			Mockito.when(fileFactory.get(Mockito.anyLong())).thenReturn(Optional.of(file));
			Mockito.when(file.read(buf, 1l, 2l)).thenReturn(CompletableFuture.failedFuture(new NotFoundException()));

			var result = cloudFs.read(PATH.toString(), buf, 2l, 1l, fi);

			Assertions.assertEquals(-ErrorCodes.ENOENT(), result);
		}

		@ParameterizedTest(name = "read() returns EIO on any other exception (expected or not)")
		@ValueSource(classes = {CloudProviderException.class, RuntimeException.class})
		public void testReadReturnsEIOOnAnyException(Class<Exception> exceptionClass) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
			Exception e = exceptionClass.getDeclaredConstructor().newInstance();
			Mockito.when(fileFactory.get(Mockito.anyLong())).thenReturn(Optional.of(file));
			Mockito.when(file.read(buf, 1l, 2l)).thenReturn(CompletableFuture.failedFuture(e));

			var result = cloudFs.read(PATH.toString(), buf, 2l, 1l, fi);

			Assertions.assertEquals(-ErrorCodes.EIO(), result);
		}

		@DisplayName("read() returns EBADF if file is not opened before")
		@Test
		public void testNotExistingHandleReturnsEBADF() {
			Mockito.when(fileFactory.get(Mockito.anyLong())).thenReturn(Optional.empty());

			var result = cloudFs.read(PATH.toString(), buf, 2l, 1l, fi);

			Assertions.assertEquals(-ErrorCodes.EBADF(), result);
		}

	}

	@Nested
	class RenameTest {

		private Path oldPath = Path.of("location/number/one");
		private Path newPath = Path.of("location/number/two");

		@DisplayName("rename(...) returns zero on success")
		@Test
		public void testSuccessReturnsZero() {
			Mockito.when(provider.move(oldPath, newPath, false))
					.thenReturn(CompletableFuture.completedFuture(newPath));
			Assertions.assertEquals(0, cloudFs.rename(oldPath.toString(), newPath.toString()));
		}

		@DisplayName("rename(...) returns ENOENT if cannot be found")
		@Test
		public void testNotFoundExceptionReturnsENOENT() {
			Mockito.when(provider.move(oldPath, newPath, false))
					.thenReturn(CompletableFuture.failedFuture(new NotFoundException()));

			var actualCode = cloudFs.rename(oldPath.toString(), newPath.toString());

			Assertions.assertEquals(-ErrorCodes.ENOENT(), actualCode);
		}

		@DisplayName("rename(...) never overwrites existing targets")
		@Test
		public void testAlreadyExistingPathsAreNeverReplaced() {
			Mockito.when(provider.move(Mockito.any(Path.class), Mockito.any(Path.class), Mockito.anyBoolean()))
					.thenReturn(CompletableFuture.completedFuture(newPath));

			cloudFs.rename(oldPath.toString(), newPath.toString());
			Mockito.verify(provider, Mockito.never()).move(oldPath, newPath, true);
		}

		@DisplayName("rename(...) returns EEXIST if target already exists")
		@Test
		public void tesAlreadyExistsExceptionReturnsEEXIST() {
			Mockito.when(provider.move(oldPath, newPath, false))
					.thenReturn(CompletableFuture.failedFuture(new AlreadyExistsException(newPath.toString())));

			var actualCode = cloudFs.rename(oldPath.toString(), newPath.toString());

			Assertions.assertEquals(-ErrorCodes.EEXIST(), actualCode);
		}

		@ParameterizedTest(name = "rename() returns EIO on any other exception (expected or not)")
		@ValueSource(classes = {CloudProviderException.class, Exception.class})
		public void testReadReturnsEIOOnAnyException(Class<Exception> exceptionClass) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
			Exception e = exceptionClass.getDeclaredConstructor().newInstance();
			Mockito.when(provider.move(oldPath, newPath, false))
					.thenReturn(CompletableFuture.failedFuture(e));

			var actualCode = cloudFs.rename(oldPath.toString(), newPath.toString());

			Assertions.assertEquals(-ErrorCodes.EIO(), actualCode);
		}
	}

	@Nested
	class MkdirTest {

		@DisplayName("mkdir(...) returns zero on success")
		@Test
		public void testSuccessReturnsZero() {
			Mockito.when(provider.createFolder(PATH))
					.thenReturn(CompletableFuture.completedFuture(PATH));

			var actualResult = cloudFs.mkdir(PATH.toString(), Mockito.anyLong());

			Assertions.assertEquals(0, actualResult);
		}

		@DisplayName("mkdir(...) returns EEXISTS if target already exists")
		@Test
		public void testAlreadyExistsExceptionReturnsEEXISTS() {
			Mockito.when(provider.createFolder(PATH))
					.thenReturn(CompletableFuture.failedFuture(new AlreadyExistsException(PATH.toString())));

			var actualResult = cloudFs.mkdir(PATH.toString(), Mockito.anyLong());

			Assertions.assertEquals(-ErrorCodes.EEXIST(), actualResult);
		}

		@ParameterizedTest(name = "mkdir(...) returns EIO on any other exception (expected or not)")
		@ValueSource(classes = {CloudProviderException.class, Exception.class})
		public void testReadReturnsEIOOnAnyException(Class<Exception> exceptionClass) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
			Exception e = exceptionClass.getDeclaredConstructor().newInstance();
			Mockito.when(provider.createFolder(PATH))
					.thenReturn(CompletableFuture.failedFuture(e));

			var actualResult = cloudFs.mkdir(PATH.toString(), Mockito.anyLong());

			Assertions.assertEquals(-ErrorCodes.EIO(), actualResult);
		}

	}

	@Nested
	class CreateTest {

		private FuseFileInfo fi;

		@BeforeEach
		public void setup() {
			fi = TestFileInfo.create();
		}

		@DisplayName("create(...) in not existing case returns 0 on success and opens file")
		@Test
		public void testNotExistingCaseReturnsZeroAndOpensFile() {
			long expectedHandle = 1337;
			fi.fh.set(0);
			fi.flags.set(0777);
			CloudItemMetadata itemMetadata = Mockito.mock(CloudItemMetadata.class);
			Mockito.when(itemMetadata.getPath()).thenReturn(PATH);
			Mockito.when(fileFactory.open(Mockito.any(Path.class), Mockito.anySet())).thenReturn(1337L);
			var openFlags = BitMaskEnumUtil.bitMaskToSet(OpenFlags.class, fi.flags.longValue());
			Mockito.when(provider.write(Mockito.any(Path.class), Mockito.anyBoolean(), Mockito.any(InputStream.class), Mockito.any(ProgressListener.class)))
					.thenReturn(CompletableFuture.completedFuture(itemMetadata));

			var actualResult = cloudFs.create(PATH.toString(), OpenFlags.O_RDWR.longValue(), fi);

			Assertions.assertEquals(0, actualResult);
			Mockito.verify(fileFactory).open(PATH, openFlags);
			Assertions.assertEquals(expectedHandle, fi.fh.longValue());
		}

		@DisplayName("create(...) in existing case returns 0 and opens file")
		@Test
		public void testExistingCaseReturnsZeroAndOpensFile() {
			long expectedHandle = 1337;
			fi.fh.set(0);
			fi.flags.set(0777);
			Mockito.when(fileFactory.open(Mockito.any(Path.class), Mockito.anySet())).thenReturn(1337L);
			var openFlags = BitMaskEnumUtil.bitMaskToSet(OpenFlags.class, fi.flags.longValue());
			Mockito.when(provider.write(Mockito.any(Path.class), Mockito.anyBoolean(), Mockito.any(InputStream.class), Mockito.any(ProgressListener.class)))
					.thenReturn(CompletableFuture.failedFuture(new AlreadyExistsException(PATH.toString())));

			var actualResult = cloudFs.create(PATH.toString(), OpenFlags.O_RDWR.longValue(), fi);

			Assertions.assertEquals(0, actualResult);
			Mockito.verify(fileFactory).open(PATH, openFlags);
			Assertions.assertEquals(expectedHandle, fi.fh.longValue());
		}

		@DisplayName("create(...) returns ENOENT on NotFoundException")
		@Test
		public void testNotFoundExceptionReturnsENOENT() {
			Mockito.when(provider.write(Mockito.any(Path.class), Mockito.anyBoolean(), Mockito.any(InputStream.class), Mockito.any(ProgressListener.class)))
					.thenReturn(CompletableFuture.failedFuture(new NotFoundException(PATH.toString())));

			var actualResult = cloudFs.create(PATH.toString(), OpenFlags.O_RDWR.longValue(), fi);

			Assertions.assertEquals(-ErrorCodes.ENOENT(), actualResult);
		}

		@ParameterizedTest(name = "create(...) returns EIO on any other exception (expected or not)")
		@ValueSource(classes = {TypeMismatchException.class, CloudProviderException.class, Exception.class})
		public void testReadReturnsEIOOnAnyException(Class<Exception> exceptionClass) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
			Exception e = exceptionClass.getDeclaredConstructor().newInstance();
			long mode = OpenFlags.O_RDWR.longValue();
			Mockito.when(provider.write(Mockito.any(Path.class), Mockito.anyBoolean(), Mockito.any(InputStream.class), Mockito.any(ProgressListener.class)))
					.thenReturn(CompletableFuture.failedFuture(e));

			var actualResult = cloudFs.create(PATH.toString(), mode, fi);

			Assertions.assertEquals(-ErrorCodes.EIO(), actualResult);
		}
	}

	@Nested
	class DeleteResourceTest {

		@DisplayName("deleteResource(...) returns 0 on success")
		@Test
		public void testOnSuccessReturnsZero() {
			Mockito.when(provider.delete(PATH))
					.thenReturn(CompletableFuture.completedFuture(null));

			var actualResult = cloudFs.deleteResource(PATH, "testCall() failed");

			Assertions.assertEquals(0, actualResult);
		}

		@DisplayName("deleteResource(...) returns ENOENT if path not found")
		@Test
		public void testNotFoundExceptionReturnsENOENT() {
			Mockito.when(provider.delete(PATH))
					.thenReturn(CompletableFuture.failedFuture(new NotFoundException()));

			var actualResult = cloudFs.deleteResource(PATH, "testCall() failed");

			Assertions.assertEquals(-ErrorCodes.ENOENT(), actualResult);
		}


		@ParameterizedTest(name = "deleteResource(...) returns EIO on any other exception (expected or not)")
		@ValueSource(classes = {CloudProviderException.class, Exception.class})
		public void testReadReturnsEIOOnAnyException(Class<Exception> exceptionClass) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
			Exception e = exceptionClass.getDeclaredConstructor().newInstance();
			Mockito.when(provider.delete(PATH))
					.thenReturn(CompletableFuture.failedFuture(e));

			var actualResult = cloudFs.deleteResource(PATH, "testCall() failed");

			Assertions.assertEquals(-ErrorCodes.EIO(), actualResult);
		}

		@Test
		public void testRmdirCallsDeleteResource() {
			var mockedCloudFs = Mockito.mock(CloudAccessFS.class);
			Mockito.doCallRealMethod().when(mockedCloudFs).rmdir(PATH.toString());

			mockedCloudFs.rmdir(PATH.toString());

			Mockito.verify(mockedCloudFs).deleteResource(PATH, "rmdir() failed");
		}

		@Test
		public void testUnlinkCallsDeleteResource() {
			var mockedCloudFs = Mockito.mock(CloudAccessFS.class);
			Mockito.doCallRealMethod().when(mockedCloudFs).unlink(PATH.toString());

			mockedCloudFs.unlink(PATH.toString());

			Mockito.verify(mockedCloudFs).deleteResource(PATH, "unlink() failed");
		}

	}

}
