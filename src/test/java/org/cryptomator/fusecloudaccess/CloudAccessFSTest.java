package org.cryptomator.fusecloudaccess;

import jnr.constants.platform.OpenFlags;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import org.cryptomator.cloudaccess.api.CloudItemMetadata;
import org.cryptomator.cloudaccess.api.CloudItemType;
import org.cryptomator.cloudaccess.api.CloudPath;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.exceptions.AlreadyExistsException;
import org.cryptomator.cloudaccess.api.exceptions.CloudProviderException;
import org.cryptomator.cloudaccess.api.exceptions.NotFoundException;
import org.cryptomator.cloudaccess.api.exceptions.TypeMismatchException;
import org.cryptomator.fusecloudaccess.locks.DataLock;
import org.cryptomator.fusecloudaccess.locks.LockManager;
import org.cryptomator.fusecloudaccess.locks.PathLock;
import org.cryptomator.fusecloudaccess.locks.PathLockBuilder;
import org.junit.jupiter.api.AfterEach;
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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class CloudAccessFSTest {

	private static final Runtime RUNTIME = Runtime.getSystemRuntime();
	private static final CloudPath PATH = CloudPath.of("some/path/to/resource");
	private static final int TIMEOUT = 100;

	private CloudAccessFS cloudFs;
	private CloudProvider provider;
	private OpenFileUploader uploader;
	private OpenFileFactory fileFactory;
	private OpenDirFactory dirFactory;
	private LockManager lockManager;
	private PathLockBuilder pathLockBuilder;
	private PathLock pathLock;
	private DataLock dataLock;

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
		uploader = Mockito.mock(OpenFileUploader.class);
		fileFactory = Mockito.mock(OpenFileFactory.class);
		dirFactory = Mockito.mock(OpenDirFactory.class);
		lockManager = Mockito.mock(LockManager.class);
		cloudFs = new CloudAccessFS(provider, CloudAccessFSTest.TIMEOUT, uploader, fileFactory, dirFactory, lockManager);

		pathLockBuilder = Mockito.mock(PathLockBuilder.class);
		pathLock = Mockito.mock(PathLock.class);
		dataLock = Mockito.mock(DataLock.class);
		Mockito.when(lockManager.createPathLock(PATH.toString())).thenReturn(pathLockBuilder);
		Mockito.when(pathLockBuilder.forReading()).thenReturn(pathLock);
		Mockito.when(pathLockBuilder.forWriting()).thenReturn(pathLock);
		Mockito.when(pathLock.lockDataForReading()).thenReturn(dataLock);
		Mockito.when(pathLock.lockDataForWriting()).thenReturn(dataLock);
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
		CompletableFuture future = CompletableFuture.failedFuture(new CloudProviderException());
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

		@AfterEach
		public void tearDown() {
			Mockito.verify(lockManager).createPathLock(PATH.toString());
			Mockito.verify(pathLockBuilder).forReading();
			Mockito.verify(pathLock).lockDataForReading();
			Mockito.verify(pathLock).close();
			Mockito.verify(dataLock).close();
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

		@AfterEach
		public void tearDown() {
			Mockito.verify(lockManager).createPathLock(PATH.toString());
			Mockito.verify(pathLockBuilder).forReading();
			Mockito.verify(pathLock).lockDataForReading();
			Mockito.verify(pathLock).close();
			Mockito.verify(dataLock).close();
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

		@AfterEach
		public void tearDown() {
			Mockito.verify(lockManager).createPathLock(PATH.toString());
			Mockito.verify(pathLockBuilder).forReading();
			Mockito.verify(pathLock).lockDataForReading();
			Mockito.verify(pathLock).close();
			Mockito.verify(dataLock).close();
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

		@DisplayName("readdir() returns ENOENT when directory not found")
		@Test
		public void testNotFoundExceptionReturnsENOENT() {
			FuseFillDir filler = Mockito.mock(FuseFillDir.class);
			Mockito.when(dirFactory.get(Mockito.anyLong())).thenReturn(Optional.of(dir));
			Mockito.when(dir.list(buf, filler, 0)).thenReturn(CompletableFuture.failedFuture(new NotFoundException()));

			var result = cloudFs.readdir(PATH.toString(), buf, filler, 0l, fi);

			Assertions.assertEquals(-ErrorCodes.ENOENT(), result);
		}

		@DisplayName("readdir() returns ENOTDIR when resource is not a directory (anymore)")
		@Test
		public void testTypeMismatchExceptionReturnsENOENT() {
			FuseFillDir filler = Mockito.mock(FuseFillDir.class);
			Mockito.when(dirFactory.get(Mockito.anyLong())).thenReturn(Optional.of(dir));
			Mockito.when(dir.list(buf, filler, 0)).thenReturn(CompletableFuture.failedFuture(new TypeMismatchException()));

			var result = cloudFs.readdir(PATH.toString(), buf, filler, 0l, fi);

			Assertions.assertEquals(-ErrorCodes.ENOTDIR(), result);
		}

		@ParameterizedTest(name = "readdir() returns ENOENT on any other exception (expected or not)")
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
			fi.fh.set(0L);
		}

		@AfterEach
		public void tearDown() {
			Mockito.verify(lockManager).createPathLock(PATH.toString());
			Mockito.verify(pathLockBuilder).forReading();
			Mockito.verify(pathLock).lockDataForReading();
			Mockito.verify(pathLock).close();
			Mockito.verify(dataLock).close();
		}

		@DisplayName("open() returns 0 in success and writes the handle to field FileInfo.fh")
		@Test
		public void testSuccessfulOpenReturnsZeroAndStoresHandle() throws IOException {
			CloudItemMetadata itemMetadata = Mockito.mock(CloudItemMetadata.class);
			Mockito.when(itemMetadata.getItemType()).thenReturn(CloudItemType.FILE);
			Mockito.when(fileFactory.open(Mockito.any(), Mockito.anySet(), Mockito.anyLong(), Mockito.any())).thenReturn(42l);
			Mockito.when(provider.itemMetadata(PATH)).thenReturn(CompletableFuture.completedFuture(itemMetadata));

			var result = cloudFs.open(PATH.toString(), fi);

			Assertions.assertEquals(0, result);
			Assertions.assertEquals(42l, fi.fh.get());
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
		private OpenFile openFile;
		private Pointer buf;

		@BeforeEach
		public void setup() {
			fi = TestFileInfo.create();
			openFile = Mockito.mock(OpenFile.class);
			buf = Mockito.mock(Pointer.class);
		}

		@AfterEach
		public void tearDown() {
			Mockito.verify(lockManager).createPathLock(PATH.toString());
			Mockito.verify(pathLockBuilder).forReading();
			Mockito.verify(pathLock).lockDataForReading();
			Mockito.verify(pathLock).close();
			Mockito.verify(dataLock).close();
		}

		@DisplayName("read() returns 0 on success")
		@Test
		public void testSuccessfulReadReturnsZero() {
			Mockito.when(fileFactory.get(Mockito.anyLong())).thenReturn(Optional.of(openFile));
			Mockito.when(openFile.read(buf, 1l, 2l)).thenReturn(CompletableFuture.completedFuture(0));

			var result = cloudFs.read(PATH.toString(), buf, 2l, 1l, fi);

			Assertions.assertEquals(0, result);
		}

		@DisplayName("read() returns ENOENT if resource is not found")
		@Test
		public void testNotFoundExceptionReturnsENOENT() {
			Mockito.when(fileFactory.get(Mockito.anyLong())).thenReturn(Optional.of(openFile));
			Mockito.when(openFile.read(buf, 1l, 2l)).thenReturn(CompletableFuture.failedFuture(new NotFoundException()));

			var result = cloudFs.read(PATH.toString(), buf, 2l, 1l, fi);

			Assertions.assertEquals(-ErrorCodes.ENOENT(), result);
		}

		@ParameterizedTest(name = "read() returns EIO on any other exception (expected or not)")
		@ValueSource(classes = {CloudProviderException.class, RuntimeException.class})
		public void testReadReturnsEIOOnAnyException(Class<Exception> exceptionClass) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
			Exception e = exceptionClass.getDeclaredConstructor().newInstance();
			Mockito.when(fileFactory.get(Mockito.anyLong())).thenReturn(Optional.of(openFile));
			Mockito.when(openFile.read(buf, 1l, 2l)).thenReturn(CompletableFuture.failedFuture(e));

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
	class WriteTest {

		private TestFileInfo fi;
		private OpenFile openFile;
		private Pointer buf;

		@BeforeEach
		public void setup() {
			fi = TestFileInfo.create();
			openFile = Mockito.mock(OpenFile.class);
			buf = Mockito.mock(Pointer.class);
		}

		@AfterEach
		public void tearDown() {
			Mockito.verify(lockManager).createPathLock(PATH.toString());
			Mockito.verify(pathLockBuilder).forReading();
			Mockito.verify(pathLock).lockDataForWriting();
			Mockito.verify(pathLock).close();
			Mockito.verify(dataLock).close();
		}

		@DisplayName("write() returns 0 on success")
		@Test
		public void testSuccessfulReadReturnsZero() throws IOException {
			Mockito.when(fileFactory.get(Mockito.anyLong())).thenReturn(Optional.of(openFile));
			Mockito.when(openFile.write(buf, 1l, 2l)).thenReturn(CompletableFuture.completedFuture(0));

			var result = cloudFs.write(PATH.toString(), buf, 2l, 1l, fi);

			Assertions.assertEquals(0, result);
		}

		@ParameterizedTest(name = "write() returns EIO on any other exception (expected or not)")
		@ValueSource(classes = {CloudProviderException.class, RuntimeException.class})
		public void testReadReturnsEIOOnAnyException(Class<Exception> exceptionClass) throws ReflectiveOperationException, IOException {
			Exception e = exceptionClass.getDeclaredConstructor().newInstance();
			Mockito.when(fileFactory.get(Mockito.anyLong())).thenReturn(Optional.of(openFile));
			Mockito.when(openFile.write(buf, 1l, 2l)).thenReturn(CompletableFuture.failedFuture(e));

			var result = cloudFs.write(PATH.toString(), buf, 2l, 1l, fi);

			Assertions.assertEquals(-ErrorCodes.EIO(), result);
		}

		@DisplayName("write() returns EBADF if file is not opened before")
		@Test
		public void testNotExistingHandleReturnsEBADF() {
			Mockito.when(fileFactory.get(Mockito.anyLong())).thenReturn(Optional.empty());

			var result = cloudFs.write(PATH.toString(), buf, 2l, 1l, fi);

			Assertions.assertEquals(-ErrorCodes.EBADF(), result);
		}

	}

	@Nested
	class RenameTest {

		private CloudPath oldPath = CloudPath.of("location/number/one");
		private CloudPath newPath = CloudPath.of("location/number/two");
		private PathLockBuilder oldPathLockBuilder = Mockito.mock(PathLockBuilder.class);
		private PathLockBuilder newPathLockBuilder = Mockito.mock(PathLockBuilder.class);
		private PathLock oldPathLock = Mockito.mock(PathLock.class);
		private PathLock newPathLock = Mockito.mock(PathLock.class);
		private DataLock oldDataLock = Mockito.mock(DataLock.class);
		private DataLock newDataLock = Mockito.mock(DataLock.class);

		@BeforeEach
		public void setup() {
			Mockito.when(lockManager.createPathLock(oldPath.toString())).thenReturn(oldPathLockBuilder);
			Mockito.when(lockManager.createPathLock(newPath.toString())).thenReturn(newPathLockBuilder);
			Mockito.when(oldPathLockBuilder.forWriting()).thenReturn(oldPathLock);
			Mockito.when(newPathLockBuilder.forWriting()).thenReturn(newPathLock);
			Mockito.when(oldPathLock.lockDataForWriting()).thenReturn(oldDataLock);
			Mockito.when(newPathLock.lockDataForWriting()).thenReturn(newDataLock);
		}

		@AfterEach
		public void tearDown() {
			Mockito.verify(lockManager).createPathLock(oldPath.toString());
			Mockito.verify(lockManager).createPathLock(newPath.toString());
			Mockito.verify(oldPathLockBuilder).forWriting();
			Mockito.verify(newPathLockBuilder).forWriting();
			Mockito.verify(oldPathLock).lockDataForWriting();
			Mockito.verify(newPathLock).lockDataForWriting();
			Mockito.verify(oldDataLock).close();
			Mockito.verify(newDataLock).close();
		}

		@DisplayName("rename(...) returns zero on success")
		@Test
		public void testSuccessReturnsZero() {
			Mockito.when(provider.move(oldPath, newPath, true)).thenReturn(CompletableFuture.completedFuture(newPath));

			var actualCode = cloudFs.rename(oldPath.toString(), newPath.toString());

			Assertions.assertEquals(0, actualCode);
			Mockito.verify(fileFactory).move(oldPath, newPath);
		}

		@DisplayName("rename(...) returns ENOENT if cannot be found")
		@Test
		public void testNotFoundExceptionReturnsENOENT() {
			Mockito.when(provider.move(oldPath, newPath, true)).thenReturn(CompletableFuture.failedFuture(new NotFoundException()));

			var actualCode = cloudFs.rename(oldPath.toString(), newPath.toString());

			Assertions.assertEquals(-ErrorCodes.ENOENT(), actualCode);
		}

		@DisplayName("rename(...) returns EEXIST if target already exists")
		@Test
		public void tesAlreadyExistsExceptionReturnsEEXIST() {
			Mockito.when(provider.move(oldPath, newPath, true)).thenReturn(CompletableFuture.failedFuture(new AlreadyExistsException(newPath.toString())));

			var actualCode = cloudFs.rename(oldPath.toString(), newPath.toString());

			Assertions.assertEquals(-ErrorCodes.EEXIST(), actualCode);
		}

		@ParameterizedTest(name = "rename() returns EIO on any other exception (expected or not)")
		@ValueSource(classes = {CloudProviderException.class, Exception.class})
		public void testReadReturnsEIOOnAnyException(Class<Exception> exceptionClass) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
			Exception e = exceptionClass.getDeclaredConstructor().newInstance();
			Mockito.when(provider.move(oldPath, newPath, true)).thenReturn(CompletableFuture.failedFuture(e));

			var actualCode = cloudFs.rename(oldPath.toString(), newPath.toString());

			Assertions.assertEquals(-ErrorCodes.EIO(), actualCode);
		}
	}

	@Nested
	class MkdirTest {

		@AfterEach
		public void tearDown() {
			Mockito.verify(lockManager).createPathLock(PATH.toString());
			Mockito.verify(pathLockBuilder).forWriting();
			Mockito.verify(pathLock).lockDataForWriting();
			Mockito.verify(pathLock).close();
			Mockito.verify(dataLock).close();
		}

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
		private Set<OpenFlags> openFlags;
		private long mode;

		@BeforeEach
		public void setup() {
			this.fi = TestFileInfo.create();
			this.openFlags = Set.of(OpenFlags.O_CREAT, OpenFlags.O_RDWR);
			this.mode = BitMaskEnumUtil.setToBitMask(openFlags);
		}

		@AfterEach
		public void tearDown() {
			Mockito.verify(lockManager).createPathLock(PATH.toString());
			Mockito.verify(pathLockBuilder).forWriting();
			Mockito.verify(pathLock).lockDataForWriting();
			Mockito.verify(pathLock).close();
			Mockito.verify(dataLock).close();
		}

		@DisplayName("create(...) in not existing case returns 0 on success and opens file")
		@Test
		public void testNotExistingCaseReturnsZeroAndOpensFile() throws IOException {
			fi.fh.set(0);
			CloudItemMetadata itemMetadata = Mockito.mock(CloudItemMetadata.class);
			Mockito.when(itemMetadata.getPath()).thenReturn(PATH);
			Mockito.when(fileFactory.open(Mockito.any(), Mockito.anySet(), Mockito.anyLong(), Mockito.any())).thenReturn(1337l);
			Mockito.when(provider.write(Mockito.eq(PATH), Mockito.eq(false), Mockito.any(), Mockito.anyLong(), Mockito.any())).thenReturn(CompletableFuture.completedFuture(itemMetadata));

			var actualResult = cloudFs.create(PATH.toString(), mode, fi);

			Assertions.assertEquals(0, actualResult);
			Mockito.verify(fileFactory).open(Mockito.eq(PATH), Mockito.any(), Mockito.eq(0l), Mockito.any());
			Assertions.assertEquals(1337l, fi.fh.longValue());
		}

		@DisplayName("create(...) in existing case returns 0 and opens file")
		@Test
		public void testExistingCaseReturnsZeroAndOpensFile() throws IOException {
			fi.fh.set(0);
			var e = new AlreadyExistsException(PATH.toString());
			CloudItemMetadata itemMetadata = Mockito.mock(CloudItemMetadata.class);
			Mockito.when(itemMetadata.getPath()).thenReturn(PATH);
			Mockito.when(itemMetadata.getSize()).thenReturn(Optional.of(42l));
			Mockito.when(fileFactory.open(Mockito.any(), Mockito.anySet(), Mockito.anyLong(), Mockito.any())).thenReturn(1337l);
			Mockito.when(provider.write(Mockito.eq(PATH), Mockito.eq(false), Mockito.any(), Mockito.anyLong(), Mockito.any())).thenReturn(CompletableFuture.failedFuture(e));
			Mockito.when(provider.itemMetadata(Mockito.eq(PATH))).thenReturn(CompletableFuture.completedFuture(itemMetadata));

			var actualResult = cloudFs.create(PATH.toString(), mode, fi);

			Assertions.assertEquals(0, actualResult);
			Mockito.verify(fileFactory).open(Mockito.eq(PATH), Mockito.any(), Mockito.eq(42l), Mockito.any());
			Assertions.assertEquals(1337l, fi.fh.longValue());
		}

		@DisplayName("create(...) returns ENOENT on NotFoundException")
		@Test
		public void testNotFoundExceptionReturnsENOENT() {
			var e = new NotFoundException(PATH.toString());
			Mockito.when(provider.write(Mockito.eq(PATH), Mockito.eq(false), Mockito.any(), Mockito.anyLong(), Mockito.any())).thenReturn(CompletableFuture.failedFuture(e));

			var actualResult = cloudFs.create(PATH.toString(), mode, fi);

			Assertions.assertEquals(-ErrorCodes.ENOENT(), actualResult);
		}

		@DisplayName("create(...) returns EISDIR on TypeMismatchException")
		@Test
		public void testTypeMismatchExceptionReturnsEISDIR() {
			var e = new TypeMismatchException(PATH.toString());
			Mockito.when(provider.write(Mockito.eq(PATH), Mockito.eq(false), Mockito.any(), Mockito.anyLong(), Mockito.any())).thenReturn(CompletableFuture.failedFuture(e));

			var actualResult = cloudFs.create(PATH.toString(), mode, fi);

			Assertions.assertEquals(-ErrorCodes.EISDIR(), actualResult);
		}

		@ParameterizedTest(name = "create(...) returns EIO on any other exception (expected or not)")
		@ValueSource(classes = {CloudProviderException.class, Exception.class})
		public void testReadReturnsEIOOnAnyException(Class<Exception> exceptionClass) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
			var e = exceptionClass.getDeclaredConstructor().newInstance();
			Mockito.when(provider.write(Mockito.eq(PATH), Mockito.eq(false), Mockito.any(), Mockito.anyLong(), Mockito.any())).thenReturn(CompletableFuture.failedFuture(e));

			var actualResult = cloudFs.create(PATH.toString(), mode, fi);

			Assertions.assertEquals(-ErrorCodes.EIO(), actualResult);
		}
	}

	@Nested
	class UnlinkTest {

		@AfterEach
		public void tearDown() {
			Mockito.verify(lockManager).createPathLock(PATH.toString());
			Mockito.verify(pathLockBuilder).forWriting();
			Mockito.verify(pathLock).lockDataForWriting();
			Mockito.verify(pathLock).close();
			Mockito.verify(dataLock).close();
		}

		@DisplayName("unlink(...) returns 0 on success")
		@Test
		public void testOnSuccessReturnsZero() {
			Mockito.when(provider.delete(PATH)).thenReturn(CompletableFuture.completedFuture(null));

			var actualResult = cloudFs.unlink(PATH.toString());

			Assertions.assertEquals(0, actualResult);
		}

		@DisplayName("unlink(...) returns ENOENT if path not found")
		@Test
		public void testNotFoundExceptionReturnsENOENT() {
			Mockito.when(provider.delete(PATH)).thenReturn(CompletableFuture.failedFuture(new NotFoundException()));

			var actualResult = cloudFs.unlink(PATH.toString());

			Assertions.assertEquals(-ErrorCodes.ENOENT(), actualResult);
		}


		@ParameterizedTest(name = "unlink(...) returns EIO on any other exception (expected or not)")
		@ValueSource(classes = {CloudProviderException.class, Exception.class})
		public void testReadReturnsEIOOnAnyException(Class<Exception> exceptionClass) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
			Exception e = exceptionClass.getDeclaredConstructor().newInstance();
			Mockito.when(provider.delete(PATH)).thenReturn(CompletableFuture.failedFuture(e));

			var actualResult = cloudFs.unlink(PATH.toString());

			Assertions.assertEquals(-ErrorCodes.EIO(), actualResult);
		}

	}

	@Nested
	class DeleteTest {

		@AfterEach
		public void tearDown() {
			Mockito.verify(lockManager).createPathLock(PATH.toString());
			Mockito.verify(pathLockBuilder).forWriting();
			Mockito.verify(pathLock).lockDataForWriting();
			Mockito.verify(pathLock).close();
			Mockito.verify(dataLock).close();
		}

		@DisplayName("unlink(...) returns 0 on success")
		@Test
		public void testUnlinkReturnsZeroOnSuccess() {
			Mockito.when(provider.delete(PATH)).thenReturn(CompletableFuture.completedFuture(null));

			var actualResult = cloudFs.unlink(PATH.toString());

			Assertions.assertEquals(0, actualResult);
		}

		@DisplayName("rmdir(...) returns 0 on success")
		@Test
		public void testRmdirReturnsZeroOnSuccess() {
			Mockito.when(provider.delete(PATH)).thenReturn(CompletableFuture.completedFuture(null));

			var actualResult = cloudFs.rmdir(PATH.toString());

			Assertions.assertEquals(0, actualResult);
		}

		@DisplayName("unlink(...) returns ENOENT if path not found")
		@Test
		public void testUnlinkReturnsENOENTOnNotFoundException() {
			Mockito.when(provider.delete(PATH)).thenReturn(CompletableFuture.failedFuture(new NotFoundException()));

			var actualResult = cloudFs.unlink(PATH.toString());

			Assertions.assertEquals(-ErrorCodes.ENOENT(), actualResult);
		}

		@DisplayName("rmdir(...) returns ENOENT if path not found")
		@Test
		public void testRmdirReturnsENOENTOnNotFoundException() {
			Mockito.when(provider.delete(PATH)).thenReturn(CompletableFuture.failedFuture(new NotFoundException()));

			var actualResult = cloudFs.rmdir(PATH.toString());

			Assertions.assertEquals(-ErrorCodes.ENOENT(), actualResult);
		}


		@DisplayName("unlink(...) returns EIO on any other exception (expected or not)")
		@ParameterizedTest(name = "unlink(...) returns EIO on {0}")
		@ValueSource(classes = {CloudProviderException.class, Exception.class})
		public void testUnlinkReturnsEIOOnException(Class<Exception> exceptionClass) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
			Exception e = exceptionClass.getDeclaredConstructor().newInstance();
			Mockito.when(provider.delete(PATH)).thenReturn(CompletableFuture.failedFuture(e));

			var actualResult = cloudFs.unlink(PATH.toString());

			Assertions.assertEquals(-ErrorCodes.EIO(), actualResult);
		}

		@DisplayName("rmdir(...) returns EIO on any other exception (expected or not)")
		@ParameterizedTest(name = "rmdir(...) returns EIO on {0}")
		@ValueSource(classes = {CloudProviderException.class, Exception.class})
		public void testRmdirReturnsEIOOnException(Class<Exception> exceptionClass) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
			Exception e = exceptionClass.getDeclaredConstructor().newInstance();
			Mockito.when(provider.delete(PATH)).thenReturn(CompletableFuture.failedFuture(e));

			var actualResult = cloudFs.rmdir(PATH.toString());

			Assertions.assertEquals(-ErrorCodes.EIO(), actualResult);
		}

	}

}
