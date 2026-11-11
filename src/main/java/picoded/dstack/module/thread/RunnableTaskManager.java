package picoded.dstack.module.thread;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.net.InetAddress;
import java.security.SecureRandom;

import picoded.dstack.module.lock.LockTokenManager;
import picoded.dstack.module.*;
import picoded.dstack.*;
import picoded.core.common.MSLongTime;
import picoded.core.conv.*;

/**
 * Given vairous runnable tasks, execute them without any overlaps across a cluster.
 * 
 * Its primary use case, is used to coordinate various background task in a cluster of server.
 */
public class RunnableTaskManager extends ModuleStructure {
	
	//----------------------------------------------------------------
	//
	//  Constructor
	//
	//----------------------------------------------------------------
	
	/**
	 * Setup RunnableTaskManager structure given a stack, and its name
	 *
	 * @param  CommonStack / DStack system to use
	 * @param  Name used to setup the prefix of the complex structure
	 **/
	public RunnableTaskManager(CommonStack inStack, String inName) {
		super(inStack, inName);
		internalStructureList = setupInternalStructureList();
	}
	
	/**
	 * Setup RunnableTaskManager structure given its internal structures
	 *
	 * @param  inTaskMap used to track various task states
	 * @param  inLockMap used to handle task locking
	 **/
	public RunnableTaskManager(KeyLongMap inLockMap) {
		lockMap = inLockMap;
		internalStructureList = setupInternalStructureList();
	}
	
	/**
	 * Setup RunnableTaskManager without any structures
	 * (internal use only)
	 **/
	protected RunnableTaskManager() {
	}
	
	//----------------------------------------------------------------
	//
	//  Class setup
	//
	//----------------------------------------------------------------
	
	/**
	 * Internal lock map
	 */
	protected KeyLongMap lockMap = null;
	
	/**
	 * Internal lock token manager 
	 */
	protected LockTokenManager lockManager = null;
	
	/**
	 * Internal Runnable map, which maybe unique to this instance
	 */
	protected ConcurrentHashMap<String, Runnable> runnableMap = new ConcurrentHashMap<>();
	
	/**
	 * Setup the internal structure given the stack + name, if needed
	 * @return internal common structures, for used by the various initialize / teardown commands
	 */
	protected List<CommonStructure> setupInternalStructureList() {
		// Safety check
		if (lockMap == null) {
			if (stack == null || name == null) {
				throw new RuntimeException("Missing required datastructures requried to be initialized");
			}
		}
		
		// The internal Maps required, 
		if (lockMap == null) {
			lockMap = stack.keyLongMap(name + "_lock");
		}
		// Initialize the lock token manager
		if (lockManager == null) {
			lockManager = new LockTokenManager(lockMap);
		}
		
		// Return as a list collection
		return Arrays.asList(new CommonStructure[] { lockMap });
	}
	
	// ExecutorService used to coordinate / cleanup all the various runnable threads
	//
	// @TODO consider using a fixed pool, see - https://stackoverflow.com/a/34285268
	ExecutorService runnableExecutor = Executors.newCachedThreadPool();
	
	/**
	 * Dedicated single-thread ScheduledExecutorService for periodic lock token renewal
	 * and execution timeout cap enforcement across running tasks.
	 *
	 * Decoupling lock renewal monitoring from `runnableExecutor` ensures that worker pool threads
	 * in fixed-size pools (e.g. `Executors.newFixedThreadPool`) execute task code directly without
	 * submitting nested child tasks to the same pool, preventing thread pool starvation deadlocks.
	 */
	protected ScheduledExecutorService lockRenewalExecutor = Executors.newSingleThreadScheduledExecutor();
	
	/**
	 * Extends close operations to close all existing runnableExecutors and lockRenewalExecutors
	 */
	@Override
	public void close() {
		super.close();
		runnableExecutor.shutdownNow();
		lockRenewalExecutor.shutdownNow();
	}
	
	//----------------------------------------------------------------
	//
	//  Internal timings
	//
	//----------------------------------------------------------------
	
	// Frequency of task status update (prevents disconnect)
	protected long taskUpdateInterval = 15 * MSLongTime.SECOND;
	
	// Time it takes for an inactive task to which is lacking
	// update to be considered "disconnected" and removed
	protected long taskInactiveTimeout = 30 * MSLongTime.SECOND;
	
	// Interrupt timeout to use
	protected long taskInterruptTimeout = 15 * MSLongTime.SECOND;
	
	// Maximum execution duration timeout cap per task in milliseconds (0 = unlimited)
	protected long maxTaskExecutionTimeoutMs = 0L;

	/**
	 * @return maximum execution duration timeout cap in milliseconds
	 */
	public long getMaxTaskExecutionTimeoutMs() {
		return maxTaskExecutionTimeoutMs;
	}

	/**
	 * Set the maximum execution duration timeout cap in milliseconds (0 = unlimited)
	 * @param timeoutMs Maximum timeout cap in milliseconds
	 */
	public void setMaxTaskExecutionTimeoutMs(long timeoutMs) {
		this.maxTaskExecutionTimeoutMs = Math.max(0L, timeoutMs);
	}
	
	//----------------------------------------------------------------
	//
	//  Runnable task registration / management
	//
	//----------------------------------------------------------------
	
	/**
	 * Register a given runnable as a task, with the respective name
	 * 
	 * @param taskName  to register as
	 * @param runner    runner to use
	 */
	public void registerRunnableTask(String taskName, Runnable runner) {
		runnableMap.put(taskName, runner);
	}
	
	//----------------------------------------------------------------
	//
	// Reusable output logger
	//
	//----------------------------------------------------------------
	
	/**
	 * logging interface
	 *
	 * This is not a static class, so that the this object inherits
	 * any extensions if needed
	 **/
	public Logger log() {
		if (logObj != null) {
			return logObj;
		}
		logObj = Logger.getLogger(this.getClass().getName());
		return logObj;
	}
	
	// Memoizer for log() function
	protected Logger logObj = null;
	
	//----------------------------------------------------------------
	//
	//  Task execution utility functions
	//
	//----------------------------------------------------------------
	
	/**
	 * Get an existing runnable, or throw an exception
	 * @param taskName  to get the runnable from
	 * @return runnable if it exists
	 */
	protected Runnable getRunnable_orThrowException(String taskName) {
		// Get the Runnable first
		Runnable runner = runnableMap.get(taskName);
		
		// Failed to execute, as task does not exist
		if (runner == null) {
			throw new RuntimeException("Unable to execute taskName as it does not exist : " + taskName);
		}
		
		// Return the runner
		return runner;
	}
	
	/**
	 * [To be extended for - RunnableTaskCluster]
	 * 
	 * Issue out a lock
	 * 
	 * See: LockTokenManager.issueLockToken
	 * 
	 * @param taskName               `lockID` in LockTokenManager
	 * 
	 * @return  renewLockToken result
	 */
	protected long issueLockToken(String taskName) {
		return lockManager.issueLockToken(taskName, taskInactiveTimeout);
	}
	
	/**
	 * [To be extended for - RunnableTaskCluster]
	 * 
	 * Perform lock renewal for a previously issued lock
	 * 
	 * See: LockTokenManager.renewLockToken
	 * 
	 * @param taskName               `lockID` in LockTokenManager
	 * @param lockToken              `originalToken` in LockTokenManager
	 * 
	 * @return  renewLockToken result
	 */
	protected long renewLockToken(String taskName, long lockToken) {
		return lockManager.renewLockToken(taskName, lockToken, taskInactiveTimeout);
	}
	
	/**
	 * [To be extended for - RunnableTaskCluster]
	 * 
	 * Release a previously issued lock
	 * 
	 * See: LockTokenManager.returnLockToken
	 * 
	 * @param taskName               `lockID` in LockTokenManager
	 * @param lockToken              `originalToken` in LockTokenManager
	 * 
	 * @return  renewLockToken result
	 */
	protected void returnLockToken(String taskName, long lockToken) {
		lockManager.returnLockToken(taskName, lockToken);
	}
	
	/**
	 * Executes a task directly on the current thread while managing background lock token renewals
	 * and execution duration timeout enforcement.
	 * 
	 * Lifecycle & Invocations:
	 * - Invoked once per task execution immediately after `issueLockToken(taskName)` successfully acquires a
	 *   distributed lock token (either during periodic scheduled cluster runs or via manual triggers).
	 * - While `runner.run()` executes on the current thread, a shared background `lockRenewalExecutor` fires
	 *   periodically every `taskUpdateInterval` (default 15s) to renew the lock token and check for execution
	 *   timeout caps (`maxTaskExecutionTimeoutMs`).
	 * - Automatically releases the lock token via `returnLockToken(...)` upon task completion or failure.
	 * 
	 * Design Rationale:
	 * Executing `runner.run()` directly on the current worker thread (rather than submitting a nested sub-task to
	 * `runnableExecutor`) ensures each task consumes exactly 1 thread pool slot. This prevents thread pool starvation
	 * deadlocks when `runnableExecutor` is configured as a fixed-size thread pool (e.g., `Executors.newFixedThreadPool(4)`).
	 * If lock renewal fails or `maxTaskExecutionTimeoutMs` is exceeded, the background monitor interrupts `workerThread`.
	 * 
	 * @param taskName     Name of the task to execute and perform lock renewals/unlocks on
	 * @param runner       Runnable containing the actual task execution logic
	 * @param inLockToken  Distributed lock token issued by `issueLockToken(...)`
	 * 
	 * @return true if the runnable completed successfully without lock loss or timeout interruptions
	 */
	protected boolean executeRunnable_withExistingLock(String taskName, Runnable runner,
		long inLockToken) {
		final long startTime = System.currentTimeMillis();
		final Thread workerThread = Thread.currentThread();

		final AtomicLong currentLockToken = new AtomicLong(inLockToken);
		final AtomicBoolean wasAborted = new AtomicBoolean(false);

		// Schedule periodic lock token renewal and execution timeout cap monitoring on background lockRenewalExecutor.
		// This runs every `taskUpdateInterval` milliseconds while the task runner is executing on `workerThread`.
		ScheduledFuture<?> renewalFuture = lockRenewalExecutor.scheduleAtFixedRate(() -> {
			try {
				// 1. Check for maximum execution duration timeout cap
				long elapsedTime = System.currentTimeMillis() - startTime;
				if (maxTaskExecutionTimeoutMs > 0L && elapsedTime > maxTaskExecutionTimeoutMs) {
					log()
						.warning(
							"WARNING (taskName="
								+ taskName
								+ ") - Execution time ("
								+ elapsedTime
								+ " ms) exceeded maximum timeout cap ("
								+ maxTaskExecutionTimeoutMs
								+ " ms). Aborting task...");
					wasAborted.set(true);
					workerThread.interrupt(); // Signal the worker thread to stop execution
					return;
				}

				// 2. Perform lock token renewal in central store / database
				long renewedToken = renewLockToken(taskName, currentLockToken.get());
				if (renewedToken <= 0) {
					log()
						.warning(
							"WARNING (taskName="
								+ taskName
								+ ") - Aborting a running task, due to lock token renewal failure, lockToken = "
								+ currentLockToken.get());
					wasAborted.set(true);
					workerThread.interrupt(); // Interrupt worker thread if lock ownership was lost
				} else {
					currentLockToken.set(renewedToken); // Save updated lock token for next renewal cycle
				}
			} catch (Exception e) {
				// Ignore transient exceptions during lock renewal monitor ticks to allow retry on next interval
			}
		}, taskUpdateInterval, taskUpdateInterval, TimeUnit.MILLISECONDS);

		try {
			// Execute task code directly on current worker thread to avoid nested pool submission and thread starvation
			runner.run();
			return !wasAborted.get();
		} catch (Exception e) {
			// Exception occurred during task execution
			log()
				.warning( //
					"-----------------------------------------------------------------------------------------------"
						+ "\n WARNING (taskName="
						+ taskName
						+ ") - Uncaught exception : "
						+ e.getMessage()
						+ "\n !!! Note that the 'backgroundProcess' should be designed to never throw an exception,"
						+ "\n !!! As it will simply be ignored and diverted into the logs (with this message)"
						+ "\n-----------------------------------------------------------------------------------------------"
						+ "\n"
						+ picoded.core.exception.ExceptionUtils.getStackTrace(e) //
						+ "\n-----------------------------------------------------------------------------------------------");
			return false;
		} finally {
			// Cancel the background lock renewal monitor task
			renewalFuture.cancel(false);

			// Release lock token in central store / database upon task completion or failure
			try {
				long finalToken = currentLockToken.get();
				if (finalToken > 0) {
					returnLockToken(taskName, finalToken);
				}
			} catch (Exception e) {
				log()
					.warning( //
						"-----------------------------------------------------------------------------------------------"
							+ "\n WARNING (taskName="
							+ taskName
							+ ") - returnLockToken exception : "
							+ e.getMessage()
							+ "\n-----------------------------------------------------------------------------------------------"
							+ "\n"
							+ picoded.core.exception.ExceptionUtils.getStackTrace(e) //
							+ "\n-----------------------------------------------------------------------------------------------");
			}
		}
	}
	
	//----------------------------------------------------------------
	//
	//  Task execution
	//
	//----------------------------------------------------------------
	
	/**
	 * Checks if the task is runnable, note that due to the async nature of this request,
	 * you cannot gurantee that isRunnableTask remains true / false after evaluation.
	 * 
	 * You can only gurantee its state at its point of execution.
	 * 
	 * This is designed to facilitate more complex fail-fast logic flows with complex lock operations.
	 * 
	 * Note it is NOT requried to use this inconjuncture executeRunnableTask - as this would already be
	 * optimized for fail fast mechanics, with better lock/failover timings/performance internally.
	 * 
	 * @param taskName
	 * @return false, if task is not runnable (true - does not gurantee runnability)
	 */
	public boolean isRunnableTask(String taskName) {
		return lockManager.isLocked(taskName) == false;
	}
	
	/**
	 * Executes a previously registered task, only if lock was succesfully acquired
	 * This function BLOCKS the current thread, till the task is complete. Or if task failed.
	 * 
	 * @param taskName  to execute
	 * 
	 * @return true if executed succesfully, else return false if lock failed
	 */
	public boolean executeRunnableTask(String taskName) {
		// Get the Runnable first
		Runnable runner = getRunnable_orThrowException(taskName);
		
		// Task exist, lets get a lock on it
		long lockToken = issueLockToken(taskName);
		
		// Lock failed, return false
		if (lockToken <= 0) {
			return false;
		}
		
		// Execute the runnable, and block accordingly
		return executeRunnable_withExistingLock(taskName, runner, lockToken);
	}
	
	/**
	 * Executes a previously registered task, only if lock was succesfully acquired
	 * This function executes the task asyncronously in a new thread, returning a "Future" object
	 * 
	 * @param taskName  to execute
	 * 
	 * @return true if executed succesfully, else return false if lock failed
	 */
	public Future<Boolean> executeRunnableTask_async(String taskName) {
		RunnableTaskManager self = this;
		return runnableExecutor.submit(() -> {
			return self.executeRunnableTask(taskName);
		});
	}
	
	/**
	 * Try to execute all runnable task, blocks till its completed.
	 * 
	 * Tasks are executed seqeuntially.
	 * Task are skipped if they have an existing lock.
	 */
	public void tryAllRunnableTask() {
		// Get list of objects
		List<String> taskSet = new ArrayList<>(runnableMap.keySet());
		
		// Shuffle it
		Collections.shuffle(taskSet);
		
		// Iterate the taskSet - and attempt to run each one of them
		for (String taskName : taskSet) {
			executeRunnableTask(taskName);
		}
	}
	
}