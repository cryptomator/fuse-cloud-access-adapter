package org.cryptomator.fusecloudaccess;

import dagger.BindsInstance;
import dagger.Component;
import org.cryptomator.cloudaccess.api.CloudProvider;

@Component(modules = CloudAccessFSModule.class)
@FileSystemScoped
public interface CloudAccessFSComponent {

	CloudAccessFS filesystem();

	@Component.Builder
	interface Builder {

		@BindsInstance
		Builder cloudProvider(CloudProvider cloudProvider);

		CloudAccessFSComponent build();
	}
}
