package org.cryptomator.fusecloudaccess;

import dagger.BindsInstance;
import dagger.Component;
import org.cryptomator.cloudaccess.api.CloudPath;
import org.cryptomator.cloudaccess.api.CloudProvider;

import java.nio.file.Path;

@Component(modules = CloudAccessFSModule.class)
@FileSystemScoped
public interface CloudAccessFSComponent {

	CloudAccessFS filesystem();

	@Component.Builder
	interface Builder {

		@BindsInstance
		Builder timeoutInMillis(int timeoutInMillis);

		@BindsInstance
		Builder cloudProvider(CloudProvider cloudProvider);

		@BindsInstance
		Builder cacheDir(Path cacheDir);

		@BindsInstance
		Builder uploadDir(CloudPath uploadDir);

		CloudAccessFSComponent build();
	}
}
