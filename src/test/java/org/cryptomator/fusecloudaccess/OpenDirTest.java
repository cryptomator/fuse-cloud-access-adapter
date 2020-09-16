package org.cryptomator.fusecloudaccess;

import jnr.ffi.Pointer;
import org.cryptomator.cloudaccess.api.CloudItemList;
import org.cryptomator.cloudaccess.api.CloudItemMetadata;
import org.cryptomator.cloudaccess.api.CloudItemType;
import org.cryptomator.cloudaccess.api.CloudPath;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.serce.jnrfuse.FuseFillDir;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class OpenDirTest {

	private final CloudPath path = CloudPath.of("/path/to/dir");
	private final CloudItemMetadata m1 = new CloudItemMetadata("m1", path.resolve("m1"), CloudItemType.FILE);
	private final CloudItemMetadata m2 = new CloudItemMetadata("m2", path.resolve("m2"), CloudItemType.FILE);
	private final CloudItemMetadata m3 = new CloudItemMetadata("m3", path.resolve("m3"), CloudItemType.FILE);
	private final CloudItemMetadata m4 = new CloudItemMetadata("m4", path.resolve("m4"), CloudItemType.FILE);
	private final CloudItemMetadata m5 = new CloudItemMetadata("m5", path.resolve("m5"), CloudItemType.FILE);
	private CloudProvider provider;
	private OpenDir dir;
	private Pointer buf;
	private FuseFillDir filler;
	private Predicate<String> listingFilter;

	@BeforeEach
	public void setup() {
		provider = Mockito.mock(CloudProvider.class);
		listingFilter = Mockito.mock(Predicate.class);
		dir = new OpenDir(provider, listingFilter, path);
		buf = Mockito.mock(Pointer.class);
		filler = Mockito.mock(FuseFillDir.class);
	}

	@Test
	@DisplayName("list until EOF")
	public void testListUntilEOF() {
		var part1 = new CloudItemList(List.of(m1, m2, m3), Optional.of("token1"));
		var part2 = new CloudItemList(List.of(m4), Optional.of("token2"));
		var part3 = new CloudItemList(List.of(m5), Optional.empty());
		Mockito.when(provider.list(path, Optional.empty())).thenReturn(CompletableFuture.completedFuture(part1));
		Mockito.when(provider.list(path, Optional.of("token1"))).thenReturn(CompletableFuture.completedFuture(part2));
		Mockito.when(provider.list(path, Optional.of("token2"))).thenReturn(CompletableFuture.completedFuture(part3));
		Mockito.when(listingFilter.test(Mockito.any())).thenReturn(true);

		var futureResult = dir.list(buf, filler, 0);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertEquals(0, result);
		Mockito.verify(filler).apply(buf, ".", null, 1);
		Mockito.verify(filler).apply(buf, "..", null, 2);
		Mockito.verify(filler).apply(buf, "m1", null, 3);
		Mockito.verify(filler).apply(buf, "m2", null, 4);
		Mockito.verify(filler).apply(buf, "m3", null, 5);
		Mockito.verify(filler).apply(buf, "m4", null, 6);
		Mockito.verify(filler).apply(buf, "m5", null, 7);
		Mockito.verifyNoMoreInteractions(filler);
	}

	@Test
	@DisplayName("list large dir (20k entries)")
	public void testListLargeDir() {
		var children = IntStream.range(0, 20_000).mapToObj(i -> m1).collect(Collectors.toList());
		var part1 = new CloudItemList(children, Optional.empty());
		Mockito.when(provider.list(path, Optional.empty())).thenReturn(CompletableFuture.completedFuture(part1));
		Mockito.when(listingFilter.test(Mockito.any())).thenReturn(true);

		var futureResult = dir.list(buf, filler, 0);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertEquals(0, result);
		Mockito.verify(filler, Mockito.times(20_000 + 2)).apply(Mockito.eq(buf), Mockito.anyString(), Mockito.any(), Mockito.anyLong());
	}

	@Test
	@DisplayName("list until EOM")
	public void testListUntilEOM() {
		Mockito.when(filler.apply(buf, ".", null, 1)).thenReturn(1);

		var futureResult = dir.list(buf, filler, 0);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertEquals(0, result);
		Mockito.verify(filler).apply(buf, ".", null, 1);
		Mockito.verifyNoMoreInteractions(filler);
	}

	@Test
	@DisplayName("filter list for specific elements")
	public void testFilter() {
		var children = new CloudItemList(List.of(m1, m2, m3, m4), Optional.empty());
		Mockito.when(provider.list(path, Optional.empty())).thenReturn(CompletableFuture.completedFuture(children));
		Mockito.doAnswer(invocation -> {
			String resourceName = invocation.getArgument(0);
			return resourceName.equals("m1") || resourceName.equals("m3");
		}).when(listingFilter).test(Mockito.any());

		var futureResult = dir.list(buf, filler, 0);
		var result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(100), () -> futureResult.toCompletableFuture().get());

		Assertions.assertEquals(0, result);
		Mockito.verify(filler).apply(Mockito.eq(buf), Mockito.eq("m1"), Mockito.any(), Mockito.anyLong());
		Mockito.verify(filler).apply(Mockito.eq(buf), Mockito.eq("m3"), Mockito.any(), Mockito.anyLong());
		Mockito.verify(filler, Mockito.never()).apply(Mockito.eq(buf), Mockito.eq("m2"), Mockito.any(), Mockito.anyLong());
		Mockito.verify(filler, Mockito.never()).apply(Mockito.eq(buf), Mockito.eq("m4"), Mockito.any(), Mockito.anyLong());
	}

}