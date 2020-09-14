package org.cryptomator.fusecloudaccess;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import dagger.Module;
import dagger.Provides;
import org.cryptomator.cloudaccess.api.CloudPath;

import javax.inject.Named;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.StampedLock;

@Module
class CloudAccessFSModule {

	private static final ThreadFactory SCHEDULER_THREAD_FACTORY = new ThreadFactoryBuilder().setDaemon(false).setNameFormat("scheduler-%d").build();

	@Provides
	@FileSystemScoped
	static ScheduledExecutorService provideScheduler() {
		return Executors.newSingleThreadScheduledExecutor(SCHEDULER_THREAD_FACTORY);
	}

	@Provides
	@FileSystemScoped
	static ExecutorService provideExecutorService() {
		return Executors.newCachedThreadPool();
	}

	@Provides
	@FileSystemScoped
	@Named("openFiles")
	static ConcurrentMap<CloudPath, OpenFile> provideOpenFilesMap() {
		return new ConcurrentHashMap<>();
	}

	@Provides
	@FileSystemScoped
	@Named("uploadTasks")
	static ConcurrentMap<CloudPath, Future<?>> provideUploadTasksMap() {
		return new ConcurrentHashMap<>();
	}
}
