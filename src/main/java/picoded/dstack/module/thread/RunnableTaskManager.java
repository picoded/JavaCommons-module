package picoded.dstack.module.thread;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
	 * Extends close operations to close all existing runnableExecutors
	 */
	@Override
	public void close() {
		super.close();
		runnableExecutor.shutdownNow();
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
	 * Given a previously initialized lockToken, taskName, and runner
	 * Does the continous relocking and "thread sleep loop", till the task is complete.
	 * 
	 * This automatically performs an unlock on task complete
	 * 
	 * This function BLOCKS the current thread, till the task is complete. Or if task failed.
	 * 
	 * @param taskName     to perform locking renewal on / unlocks on
	 * @param runThread    thread to actually start, and wait for (to join)
	 * @param inLockToken  lock token to use for renewal
	 * 
	 * @return true if runnable completes without interruptions
	 */
	protected boolean executeRunnable_withExistingLock(String taskName, Runnable runner,
		long inLockToken) {
		// Lock token to use
		long lockToken = inLockToken;
		
		// Lock was succesful, lets run the thread
		try {
			// Pass it to the executor, start it, and get the Future object
			Future<?> futureObj = runnableExecutor.submit(runner);
			
			// While its running, does the lock renewals
			while ((futureObj.isCancelled() || futureObj.isDone()) != true) {
				
				// Lets wait for it to complete with a short nap
				try {
					futureObj.get(taskUpdateInterval, TimeUnit.MILLISECONDS);
				} catch (TimeoutException e) {
					// does nothing on timeout exception
				}
				
				// Renew the token
				lockToken = renewLockToken(taskName, lockToken);
				
				// If renew failed - ABORT
				if (lockToken <= 0) {
					// ABORT warning (to trace it if needed)
					/// @TODO - consider an option to silence this or configure logging level (not important as of now due to edge case handling)
					log()
						.warning(
							"WARNING (taskName="
								+ taskName
								+ ") - Aborting a running task, due to lock token renewal failure, lockToken = "
								+ lockToken);
					
					// Trigger an interrupt
					futureObj.cancel(true);
					
					// Check for interruption failure - technically this should be impossible?
					if ((futureObj.isCancelled() || futureObj.isDone()) != true) {
						throw new RuntimeException("Failed to abort task (due to failed lock renewal) : "
							+ taskName);
					}
					
					// Return false
					return false;
				}
			}
			
			// Succesful execution and join
			return true;
		} catch (Exception e) {
			// Exception occured =[
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
		} finally {
			// Attempt to release the lockToken, if its valid
			try {
				if (lockToken > 0) {
					lockManager.returnLockToken(taskName, lockToken);
				}
			} catch (Exception e) {
				// Exception occured =[
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
		
		// I dunno how it reached here, but it probably means things went bad
		return false;
	}
	
	//----------------------------------------------------------------
	//
	//  Task execution
	//
	//----------------------------------------------------------------
	
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
		Set<String> taskSet = new HashSet<>(runnableMap.keySet());
		
		// Iterate the taskSet - ant attempt to run each one of them
		for (String taskName : taskSet) {
			executeRunnableTask(taskName);
		}
	}
	
}