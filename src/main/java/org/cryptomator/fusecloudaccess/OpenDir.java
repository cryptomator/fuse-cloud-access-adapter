package org.cryptomator.fusecloudaccess;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import jnr.ffi.Pointer;
import org.cryptomator.cloudaccess.api.CloudItemMetadata;
import org.cryptomator.cloudaccess.api.CloudProvider;
import ru.serce.jnrfuse.FuseFillDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

class OpenDir {

	private final CloudProvider provider;
	private final Path path;
	private Optional<String> pageToken;
	private List<String> children;
	private boolean reachedEof;

	public OpenDir(CloudProvider provider, Path path) {
		this.provider = provider;
		this.path = path;
		this.pageToken = Optional.empty();
		this.children = new ArrayList<>();
		this.children.add(".");
		this.children.add("..");
	}

	private CompletionStage<Void> loadNext() {
		Preconditions.checkState(!reachedEof);
		return provider.list(path, pageToken).thenAccept(itemList -> {
			itemList.getItems().stream().map(CloudItemMetadata::getName).forEachOrdered(children::add);
			pageToken = itemList.getNextPageToken();
			reachedEof = itemList.getNextPageToken().isEmpty();
		});
	}

	// https://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201001/homework/fuse/fuse_doc.html#readdir-details
	public CompletionStage<Integer> list(Pointer buf, FuseFillDir filler, int offset) {
		if (offset < children.size()) {
			var childName = children.get(offset);
			int nextOffset = offset + 1;
			int result = filler.apply(buf, childName, null, nextOffset);
			if (result != 0) {
				return CompletableFuture.completedFuture(0);
			} else {
				return list(buf, filler, nextOffset);
			}
		} else if (reachedEof) {
			return CompletableFuture.completedFuture(0);
		} else {
			assert offset >= children.size();
			return loadNext().thenCompose(v -> list(buf, filler, offset));
		}
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(OpenDir.class) //
				.add("path", path) //
				.toString();
	}
}
