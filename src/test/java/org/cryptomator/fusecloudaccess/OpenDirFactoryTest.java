package org.cryptomator.fusecloudaccess;

import org.cryptomator.cloudaccess.api.CloudPath;
import org.cryptomator.cloudaccess.api.CloudProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;


public class OpenDirFactoryTest {

	private static final CloudPath PATH = CloudPath.of("/open/this/directory");

	private CloudProvider cloudProvider = Mockito.mock(CloudProvider.class);
	private OpenDirFactory openDirs;

	@BeforeEach
	public void setup(){
		openDirs = new OpenDirFactory(cloudProvider);
	}

	@Test
	public void testOpenReturnsFileHandleAndOpensDir(){
		long handle = openDirs.open(PATH);
		//Assertions.assertEquals();
	}
}
