package picoded.dstack.module.thread;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.net.InetAddress;
import java.security.SecureRandom;

import picoded.dstack.module.*;
import picoded.dstack.*;
import picoded.core.common.MSLongTime;
import picoded.core.conv.*;

/**
 * # RunnableTaskCluster
 * 
 * Setup a runnable task cluster node,
 * which will automatically execute all scheduled runnable tasks
 * distributed across all its instances (synced via the CommonStack)
 */
public class RunnableTaskCluster extends RunnableTaskClusterBase {
	
	//----------------------------------------------------------------
	//
	//  Constructor
	//
	//----------------------------------------------------------------
	
	/**
	 * Setup RunnableTaskCluster structure given a stack, and its name
	 *
	 * @param  CommonStack / DStack system to use
	 * @param  Name used to setup the prefix of the complex structure
	 **/
	public RunnableTaskCluster(CommonStack inStack, String inName) {
		super(inStack, inName);
		setupBackgroundExecutor();
	}
	
	/**
	 * Setup RunnableTaskCluster structure given its internal structures
	 *
	 * @param  inTaskMap used to track various task states
	 * @param  inLockMap used to handle task locking
	 **/
	public RunnableTaskCluster(DataObjectMap inTaskMap, KeyLongMap inLockMap) {
		super(inTaskMap, inLockMap);
		setupBackgroundExecutor();
	}
	
	//----------------------------------------------------------------
	//
	//  ScheduledExecutorService setup
	//
	//----------------------------------------------------------------
	
	// Setup the executor service to be used internally
	protected ScheduledExecutorService executorService = Executors
		.newSingleThreadScheduledExecutor();
	
	// Minimum delay control, in ms (default is 5 seconds)
	protected long minimumDelay = 5000;
	
	/**
	 * Call `tryAllRunnableScheduledTask` with an induced Thread.sleep
	 * if its completed "too fast"
	 */
	protected void tryAllRunnableScheduledTask_withMinimumDelay() {
		// Get the start time
		long startTime = System.currentTimeMillis();
		
		// Execute all the scheduled task
		tryAllRunnableScheduledTask();
		
		// Calculate the time taken
		long endinTime = System.currentTimeMillis();
		long timeTaken = endinTime - startTime;
		
		// If time taken, is less then minimum delay - induce it
		if (timeTaken < minimumDelay) {
			try {
				// System.out.println("Doing min delay sleep : "+(minimumDelay - timeTaken));
				Thread.sleep(minimumDelay - timeTaken);
			} catch (InterruptedException e) {
				// does nothing
			}
		}
	}
	
	/**
	 * Setup ScheduledExecutorService, to trigger runnable tasks in the background
	 */
	protected void setupBackgroundExecutor() {
		RunnableTaskCluster self = this;
		Runnable runTask = () -> {
			self.tryAllRunnableScheduledTask_withMinimumDelay();
		};
		executorService.scheduleWithFixedDelay(runTask, 1l, 1l, TimeUnit.MILLISECONDS);
	}
	
	/**
	 * Does the immediate shutdown of the task executor
	 * To facilitate garbage collection, etc
	 */
	public void shutdownTaskExecutor() {
		executorService.shutdownNow();
	}

	//----------------------------------------------------------------
	//
	//  Minimum delay controls
	//
	//----------------------------------------------------------------
	
	/**
	 * @return minimal delay interval between executor checks and execution.
	 * This typically kicks in when minimum to no task were executed, to prevent wasted "CPU" cycles on no task
	 */
	public long minimumExecutorDelay() {
		return minimumDelay;
	}
	
	/**
	 * Configure the minimum executor delay, note that this might only kick in the next cycle if there are race conditions.
	 * 
	 * @param delay in between execution cycles - the minimum is 1 ms
	 * @return
	 */
	public long minimumExecutorDelay(long delay) {
		return minimumDelay = Math.max(1l, delay);
	}
}