package org.cryptomator.fusecloudaccess;

import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import org.cryptomator.cloudaccess.api.CloudItemList;
import org.cryptomator.cloudaccess.api.CloudItemMetadata;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.cryptomator.cloudaccess.api.exceptions.CloudProviderException;
import org.cryptomator.cloudaccess.api.exceptions.InvalidPageTokenException;
import org.cryptomator.cloudaccess.api.exceptions.NotFoundException;
import org.cryptomator.cloudaccess.api.exceptions.TypeMismatchException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class CloudAccessFSTest {

	static final int TIMEOUT = 100;

	private CloudAccessFS cloudFs;

	@BeforeEach
	public void setup() {
		CloudProvider cloudProvider = Mockito.mock(CloudProvider.class);
		cloudFs = new CloudAccessFS(cloudProvider, TIMEOUT);
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

	static class GetAttrTests {

		private static final Runtime RUNTIME = Runtime.getSystemRuntime();
		private static final Path PATH = Path.of("some/path/to/resource");

		private CloudProvider provider;
		private CloudAccessFS cloudFs;
		private FileStat fileStat;

		@BeforeEach
		public void setup() {
			provider = Mockito.mock(CloudProvider.class);
			cloudFs = new CloudAccessFS(provider, CloudAccessFSTest.TIMEOUT);
			fileStat = new FileStat(RUNTIME);
		}

		@DisplayName("getAttr() returns 0 on success")
		@Test
		public void testGetAttrSuccess() {
			Mockito.when(provider.itemMetadata(PATH))
					.thenReturn(CompletableFuture.completedFuture(CloudItemMetadataProvider.ofPath(PATH)));

			Assertions.assertEquals(0, cloudFs.getattr(PATH.toString(), fileStat));
		}

		@DisplayName("getAttr() returns ENOENT when resource is not found.")
		@Test
		public void testGetAttrNotFound() {
			Mockito.when(provider.itemMetadata(PATH))
					.thenReturn(CompletableFuture.failedFuture(new NotFoundException()));

			Assertions.assertEquals(-ErrorCodes.ENOENT(), cloudFs.getattr(PATH.toString(), fileStat));
		}

		@DisplayName("getAttr() returns EIO on CloudProviderException.")
		@Test
		public void testGetAttrCloudError() {
			Mockito.when(provider.itemMetadata(PATH))
					.thenReturn(CompletableFuture.failedFuture(new CloudProviderException()));

			Assertions.assertEquals(-ErrorCodes.EIO(), cloudFs.getattr(PATH.toString(), fileStat));
		}

		@DisplayName("getAttr() returns EIO on any exception.")
		@Test
		public void testGetAttrSomeException() {
			Mockito.when(provider.itemMetadata(PATH))
					.thenReturn(CompletableFuture.failedFuture(new Exception()));

			Assertions.assertEquals(-ErrorCodes.EIO(), cloudFs.getattr(PATH.toString(), fileStat));
		}
	}

	//@Nested
	static class ReadDirTests {

		private static final long OFFSET = 0L;
		private static final Path PATH = Path.of("some/path/to/resource");
		private static final List<CloudItemMetadata> ITEM_LISTING = List.of(
				CloudItemMetadataProvider.ofPath(PATH.resolve("asd")),
				CloudItemMetadataProvider.ofPath(PATH.resolve("qwe")),
				CloudItemMetadataProvider.ofPath(PATH.resolve("yxc")),
				CloudItemMetadataProvider.ofPath(PATH.resolve("justAnother"))
		);

		private CloudProvider provider;
		private CloudAccessFS cloudFs;
		private Pointer buf;
		private FuseFileInfo fi;

		@BeforeEach
		public void setup() {
			provider = Mockito.mock(CloudProvider.class);
			cloudFs = new CloudAccessFS(provider, CloudAccessFSTest.TIMEOUT);
			buf = Mockito.mock(Pointer.class);
			fi = Mockito.mock(FuseFileInfo.class);
		}

		@DisplayName("Successful readdir() returns 0 and lists all elements")
		@Test
		public void testSuccess() {
			var expectedListing = new ArrayList<String>();
			expectedListing.add(".");
			expectedListing.add("..");
			expectedListing.addAll(ITEM_LISTING.stream().map(CloudItemMetadata::getName).collect(Collectors.toList()));

			var actualListing = new ArrayList<String>();
			FuseFillDir filler = (pointer, byteBuffer, pointer1, l) -> {
				actualListing.add(new String(byteBuffer.array()));
				return 0;
			};

			Mockito.when(provider.listExhaustively(PATH))
					.thenReturn(CompletableFuture.completedFuture(new CloudItemList(ITEM_LISTING)));

			Assertions.assertEquals(0, cloudFs.readdir(PATH.toString(), buf, filler, OFFSET, fi));
			Assertions.assertIterableEquals(expectedListing, actualListing);
		}

		@DisplayName("readdir() returns ENOMEM if output buffer is full")
		@Test
		public void testFullNativeBufferReturnsENOMEM() {
			FuseFillDir filler = Mockito.mock(FuseFillDir.class);
			Mockito.when(filler.apply(Mockito.any(Pointer.class), Mockito.anyString(), Mockito.any(), Mockito.anyLong()))
					.thenReturn(1);

			Mockito.when(provider.listExhaustively(PATH))
					.thenReturn(CompletableFuture.completedFuture(new CloudItemList(ITEM_LISTING)));

			Assertions.assertEquals(-ErrorCodes.ENOMEM(), cloudFs.readdir(PATH.toString(), buf, filler, OFFSET, fi));
		}

		@DisplayName("readdir() returns ENOENT when directory not found")
		@Test
		public void testNotFoundReturnsENOENT() {
			FuseFillDir filler = Mockito.mock(FuseFillDir.class);
			Mockito.when(provider.listExhaustively(PATH))
					.thenReturn(CompletableFuture.failedFuture(new NotFoundException()));

			Assertions.assertEquals(-ErrorCodes.ENOENT(), cloudFs.readdir(PATH.toString(), buf, filler, OFFSET, fi));
		}

		@DisplayName("readdir() returns ENOTDIR when resource is not a directory")
		@Test
		public void testNotADirectoryReturnsENOTDIR() {
			FuseFillDir filler = Mockito.mock(FuseFillDir.class);

			Mockito.when(provider.listExhaustively(PATH))
					.thenReturn(CompletableFuture.failedFuture(new TypeMismatchException()));

			Assertions.assertEquals(-ErrorCodes.ENOTDIR(), cloudFs.readdir(PATH.toString(), buf, filler, OFFSET, fi));
		}

		@DisplayName("readdir() returns EIO on CloudProviderException ")
		@Test
		public void testCloudProviderExceptionReturnsEIO() {
			FuseFillDir filler = Mockito.mock(FuseFillDir.class);

			Mockito.when(provider.listExhaustively(PATH))
					.thenReturn(CompletableFuture.failedFuture(new CloudProviderException()));

			Assertions.assertEquals(-ErrorCodes.EIO(), cloudFs.readdir(PATH.toString(), buf, filler, OFFSET, fi));
		}

		@DisplayName("readdir() returns EIO on InvalidPageTokenException")
		@Test
		public void testInvalidTokenReturnsEIO() {
			FuseFillDir filler = Mockito.mock(FuseFillDir.class);

			Mockito.when(provider.listExhaustively(PATH))
					.thenReturn(CompletableFuture.failedFuture(new InvalidPageTokenException("Message")));

			Assertions.assertEquals(-ErrorCodes.EIO(), cloudFs.readdir(PATH.toString(), buf, filler, OFFSET, fi));
		}

		@DisplayName("readdir() returns EIO on any other exception")
		@Test
		public void testAnyExceptionReturnsEIO() {
			FuseFillDir filler = Mockito.mock(FuseFillDir.class);

			Mockito.when(provider.listExhaustively(PATH))
					.thenReturn(CompletableFuture.failedFuture(new Exception()));

			Assertions.assertEquals(-ErrorCodes.EIO(), cloudFs.readdir(PATH.toString(), buf, filler, OFFSET, fi));
		}
	}

	static class OpenTest {

		private static final Path PATH = Path.of("some/path/to/resource");

		private CloudProvider provider;
		private OpenFileFactory fileFactory;
		private CloudAccessFS cloudFs;
		private TestFileInfo fi;

		@BeforeEach
		public void setup() {
			provider = Mockito.mock(CloudProvider.class);
			fileFactory = Mockito.mock(OpenFileFactory.class);
			cloudFs = new CloudAccessFS(provider, CloudAccessFSTest.TIMEOUT, fileFactory);
			fi = TestFileInfo.create();
		}

		@DisplayName("open() returns 0 in success and writes the handle to field FileInfo.fh")
		@Test
		public void testSuccessfulOpenReturnsZeroAndStoresHandle() {
			long expectedHandle = 1337;

			Mockito.when(fileFactory.open(Mockito.any(Path.class), Mockito.anySet())).thenReturn(1337L);

			Mockito.when(provider.itemMetadata(PATH))
					.thenReturn(CompletableFuture.completedFuture(CloudItemMetadataProvider.ofPath(PATH)));
			Assertions.assertEquals(0, cloudFs.open(PATH.toString(), fi));
			Assertions.assertEquals(expectedHandle, fi.fh.get());
		}

		@DisplayName("open() returns ENOENT if the specified path is not found")
		@Test
		public void testNotFoundExceptionReturnsENOENT() {
			Mockito.when(provider.itemMetadata(PATH))
					.thenReturn(CompletableFuture.failedFuture(new NotFoundException()));
			Assertions.assertEquals(-ErrorCodes.ENOENT(), cloudFs.open(PATH.toString(), fi));
		}

		@DisplayName("open() returns EIO if an IO error occured at the provider side")
		@Test
		public void testCloudProviderExceptionReturnsEIO() {
			Mockito.when(provider.itemMetadata(PATH))
					.thenReturn(CompletableFuture.failedFuture(new CloudProviderException()));
			Assertions.assertEquals(-ErrorCodes.EIO(), cloudFs.open(PATH.toString(), fi));
		}

		@DisplayName("open() returns generic EIO if any exception is thrown")
		@Test
		public void testAnyExceptionReturnsEIO() {
			Mockito.when(provider.itemMetadata(PATH))
					.thenReturn(CompletableFuture.failedFuture(new Exception()));
			Assertions.assertEquals(-ErrorCodes.EIO(), cloudFs.open(PATH.toString(), fi));
		}

	}

	static class ReadTest {

		//public int read(String path, Pointer buf, long size, long offset, FuseFileInfo fi)
		private static final Path PATH = Path.of("some/path/to/resource");
		private static final OpenFile FILE = Mockito.mock(OpenFile.class);
		private static final Pointer BUF = Mockito.mock(Pointer.class);
		private static final long SIZE = 2L;
		private static final long OFFSET = 1L;

		private OpenFileFactory fileFactory;
		private CloudAccessFS cloudFs;
		private TestFileInfo fi;

		@BeforeEach
		public void setup() {
			fileFactory = Mockito.mock(OpenFileFactory.class);
			cloudFs = new CloudAccessFS(Mockito.mock(CloudProvider.class), CloudAccessFSTest.TIMEOUT, fileFactory);
			fi = TestFileInfo.create();
		}

		@DisplayName("read() returns 0 on success")
		@Test
		public void testSuccessfulReadReturnsZero() {
			Mockito.when(fileFactory.get(Mockito.anyLong()))
					.thenReturn(Optional.of(FILE));
			Mockito.when(FILE.read(BUF, OFFSET, SIZE))
					.thenReturn(CompletableFuture.completedFuture(0));
			Assertions.assertEquals(0, cloudFs.read(PATH.toString(), BUF, SIZE, OFFSET, fi));
		}

		@DisplayName("read() returns ENOENT if resource is not found")
		@Test
		public void testNotFoundExceptionReturnsENOENT() {
			Mockito.when(fileFactory.get(Mockito.anyLong()))
					.thenReturn(Optional.of(FILE));
			Mockito.when(FILE.read(BUF, OFFSET, SIZE))
					.thenReturn(CompletableFuture.failedFuture(new NotFoundException()));
			Assertions.assertEquals(-ErrorCodes.ENOENT(), cloudFs.read(PATH.toString(), BUF, SIZE, OFFSET, fi));
		}

		@DisplayName("read() returns EIO if CloudProviderException occurs")
		@Test
		public void testCloudProviderExceptionReturnsEIO() {
			Mockito.when(fileFactory.get(Mockito.anyLong()))
					.thenReturn(Optional.of(FILE));
			Mockito.when(FILE.read(BUF, OFFSET, SIZE))
					.thenReturn(CompletableFuture.failedFuture(new CloudProviderException()));
			Assertions.assertEquals(-ErrorCodes.EIO(), cloudFs.read(PATH.toString(), BUF, SIZE, OFFSET, fi));
		}

		@DisplayName("read() returns EIO if any exception occurs")
		@Test
		public void testAnyExceptionReturnsEIO() {
			Mockito.when(fileFactory.get(Mockito.anyLong()))
					.thenReturn(Optional.of(FILE));
			Mockito.when(FILE.read(BUF, OFFSET, SIZE))
					.thenReturn(CompletableFuture.failedFuture(new Exception()));
			Assertions.assertEquals(-ErrorCodes.EIO(), cloudFs.read(PATH.toString(), BUF, SIZE, OFFSET, fi));
		}

		@DisplayName("read() returns EBADF if file is not opened before")
		@Test
		public void testNotExistingHandleReturnsEBADF() {
			Mockito.when(fileFactory.get(Mockito.anyLong()))
					.thenReturn(Optional.empty());
			Assertions.assertEquals(-ErrorCodes.EBADF(), cloudFs.read(PATH.toString(), BUF, SIZE, OFFSET, fi));
		}

	}

}
