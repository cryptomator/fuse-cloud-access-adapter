package org.cryptomator.fusecloudaccess;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import jnr.ffi.Pointer;
import org.cryptomator.cloudaccess.api.CloudItemMetadata;
import org.cryptomator.cloudaccess.api.CloudPath;
import org.cryptomator.cloudaccess.api.CloudProvider;
import ru.serce.jnrfuse.FuseFillDir;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

class OpenDir {

	private final CloudProvider provider;
	private final CloudPath path;
	private Optional<String> pageToken;
	private List<String> children;
	private boolean reachedEof;

	public OpenDir(CloudProvider provider, CloudPath path) {
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
		// fill with loaded children:
		int i = offset;
		for (; i < children.size(); i++) {
			var childName = children.get(i);
			int result = filler.apply(buf, childName, null, i + 1);
			if (result != 0) { // buffer is full
				return CompletableFuture.completedFuture(0);
			}
		}

		// load next children (unless reached EOF) and continue listing at current position:
		if (reachedEof) {
			return CompletableFuture.completedFuture(0);
		} else {
			final int j = i;
			return loadNext().thenCompose(v -> list(buf, filler, j));
		}
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(OpenDir.class) //
				.add("path", path) //
				.toString();
	}
}
