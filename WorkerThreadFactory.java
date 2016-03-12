package concurrency.fileprocessing;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class WorkerThreadFactory implements ThreadFactory{
	protected final AtomicInteger threadNumber = new AtomicInteger(0);

	public Thread newThread(Runnable r){
		Thread th = new Thread(r, "ninja-" + threadNumber.incrementAndGet());
		return th;
	} 
}