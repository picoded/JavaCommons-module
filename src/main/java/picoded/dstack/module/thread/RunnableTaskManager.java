package picoded.dstack.module.thread;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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
	//  Runnable task managerment
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
		Runnable runner = runnableMap.get(taskName);
		
		// Failed to execute, as task does not exist
		if (runner == null) {
			throw new RuntimeException("Unable to execute taskName as it does not exist : " + taskName);
		}
		
		// Task exist, lets get a lock on it
		long lockToken = lockManager.issueLockToken(taskName, taskInactiveTimeout);
		
		// Lock failed, return false
		if (lockToken <= 0) {
			return false;
		}
		
		// Lock was succesful, lets run the thread
		try {
			// Prepare the thread, and start it
			Thread runThread = new Thread(runner);
			runThread.start();
			
			// Wait till its completed, renewing the lock if needed
			while (runThread.isAlive()) {
				// Wait for it to complete, with a short nap
				runThread.join(taskUpdateInterval);
				
				// Renew the token
				lockToken = lockManager.renewLockToken(taskName, lockToken, taskInactiveTimeout);
				
				// If renew failed - ABORT
				if (lockToken <= 0) {
					// Trigger an interrupt
					runThread.interrupt();
					
					// Wait for interrupt to complete
					runThread.join(taskInterruptTimeout);
					
					// Check for interruption failure
					if (runThread.isAlive()) {
						throw new RuntimeException("Failed to abort task (due to failed lock renewal) : "
							+ taskName);
					}
					
					// Return false
					return false;
				}
			}
			
			// Succesful execution and join
			return true;
		} catch (InterruptedException e) {
			// Interrupt occured
			return false;
		} finally {
			// Attempt to release the lockToken, if its valid
			if (lockToken > 0) {
				lockManager.returnLockToken(taskName, lockToken);
			}
		}
	}
	
	/**
	 * Try to execute all runnable task, blocks till its completed.
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