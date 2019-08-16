package picoded.dstack.module.thread;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.net.InetAddress;
import java.security.SecureRandom;

import picoded.dstack.module.*;
import picoded.dstack.*;
import picoded.core.common.MSLongTime;
import picoded.core.conv.*;

/**
 * Extends RunnableTaskManager 
 * 
 * - add support for DataObjectMap storing state information (that can be queryed)
 * - add task scheduling support
 * 
 * @TODO - this is incomplete!!! - use RunnableTaskManager instead
 */
public class RunnableTaskCluster extends RunnableTaskManager {
	
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
		internalStructureList = setupInternalStructureList();
	}
	
	/**
	 * Setup RunnableTaskCluster structure given its internal structures
	 *
	 * @param  inTaskMap used to track various task states
	 * @param  inLockMap used to handle task locking
	 **/
	public RunnableTaskCluster(DataObjectMap inTaskMap, KeyLongMap inLockMap) {
		taskMap = inTaskMap;
		lockMap = inLockMap;
		internalStructureList = setupInternalStructureList();
	}
	
	//----------------------------------------------------------------
	//
	//  Class setup
	//
	//----------------------------------------------------------------
	
	/**
	 * Task global state map
	 */
	protected DataObjectMap taskMap = null;
	
	/**
	 * Setup the internal structure given the stack + name, if needed
	 * @return internal common structures, for used by the various initialize / teardown commands
	 */
	protected List<CommonStructure> setupInternalStructureList() {
		// Safety check
		if (lockMap == null || taskMap == null) {
			if (stack == null || name == null) {
				throw new RuntimeException("Missing required datastructures requried to be initialized");
			}
		}
		
		// The internal Maps required, 
		super.setupInternalStructureList();
		if (taskMap == null) {
			taskMap = stack.dataObjectMap(name + "_state");
		}
		
		// Return as a list collection
		return Arrays.asList(new CommonStructure[] { lockMap, taskMap });
	}
	
	//----------------------------------------------------------------
	//
	//  Class setup
	//
	//----------------------------------------------------------------
	
	/**
	 * Internal task to DataObject OID mapping
	 */
	protected ConcurrentHashMap<String, String> taskOIDMap = new ConcurrentHashMap<>();

	/**
	 * Internal last known task ending, or check done (to skip any lock calls when its not needed, saving DB hits)
	 */
	protected ConcurrentHashMap<String, Long> lastKnownTaskUpdateMap = new ConcurrentHashMap<>();

	/**
	 * Internal last known task start, or check done (to skip any lock calls when its not needed, saving DB hits)
	 */
	protected ConcurrentHashMap<String, Long> lastKnownTaskStartMap = new ConcurrentHashMap<>();

	/**
	 * Inveral mapping between task
	 */
	protected ConcurrentHashMap<String, Long> intervalMap = new ConcurrentHashMap<>();

	/**
	 * Inveral mapping between task
	 */
	protected ConcurrentHashMap<String, Long> delayMap = new ConcurrentHashMap<>();

	//----------------------------------------------------------------
	//
	//  Internal Manager identifier setup
	//
	//----------------------------------------------------------------
	
	/**
	 * The current RunnableTaskManager GUID, initialized on instance construction
	 */
	protected String _managerID = GUID.base58();
	
	/**
	 * @return the unique base 58 GUID of the task manager
	 */
	public String getManagerID() {
		return _managerID;
	}
	
	// Server host address memoizer caching
	protected String _hostAddress = null;
	
	/**
	 * @return the server instance various host address, note that this maybe cached
	 */
	public String getHostAddress() {
		// Return cached address
		if (_hostAddress != null) {
			return _hostAddress;
		}
		
		// Lets get it
		try {
			InetAddress host = InetAddress.getLocalHost();
			_hostAddress = "" + host.getHostAddress();
		} catch (Exception e) {
			// Host address fetch failed
			_hostAddress = "unknown";
		}
		
		// Return the configured host address
		return _hostAddress;
	}
	
	//----------------------------------------------------------------
	//
	//  Execute scheduled task
	//
	//----------------------------------------------------------------

	/*
	 * # Execute scheduled task
	 * 
	 * WARNING: Due to the highly complex nature of the execute scheduled task,
	 * especially on its sequence of events intentionally designed to avoid lock contention
	 * and / or save conflicts. 
	 * 
	 * This entire comment block is outline draft the approximate persudo code.
	 * On how a scheduled task is decided if it should be executed (or not)
	 * 
	 * The general idea, is to "fail early" if possible, without doing expensive locks
	 * unless it is truely required.
	 * 
	 * Functions declared in the pesudo code, arre actions which are repeated
	 * 
	 * ```
	 * //---------------------------------------------------
	 * //
	 * // Note the following is done WITHOUT a task lock
	 * //
	 * //---------------------------------------------------
	 * 
	 * if ( is missing delay and interval config, aka, not a scheduled task ) {
	 *    throw exception : not a scheduled task
	 * }
	 * 
	 * //
	 * // Internal validation (without DB call)
	 * //
	 * 
	 * function validateTimestamp {
	 *    if ( has last known test start ) {
	 *       if ( now < ( start timestamp + min interval ) ) {
	 *          return false
	 *       }
	 *    }
	 *    if( has last known test activity ) {
	 *       if ( now < ( activity timestamp + min delay ) ) {
	 *          return false
	 *       }
	 *    }
	 * }
	 * 
	 * // Validate the timestamp
	 * if( !validateTimestamp() ) {
	 *    return false;
	 * }
	 * 
	 * //
	 * // Cluster validation (with DB call)
	 * //
	 * 
	 * function getExistingTaskObject {
	 *    if ( has a known, non blank OID associated to taskName ) {
	 *       get the `taskObject` by OID to be used later
	 *       if ( taskObject == null ) {
	 *          update known OID with a blank string
	 *       }
	 *    } else if ( known OID is null (not blank) ) {
	 *       get any `taskObject` using a query for the latest copy
	 *       if( taskObject != null ) {
	 *          update known OID with taskObject OID
	 *       }
	 *    }
	 *    return taskObject
	 * }
	 * 
	 * // Get an existing test object, and process it
	 * taskObject = getExistingTaskObject()
	 * 
	 * function validateTaskObject {
	 *    if ( taskObject != null ) {
	 *       if( lastKnownTaskActivity < taskObject.updateTimestamp ) {
	 *          update lastKnownTaskActivity with taskObject.updateTimestamp
	 *       }
	 *       if( lastKnownTaskStart < taskObject.startTimestamp ) {
	 *          update lastKnownTaskStart with taskObject.startTimestamp
	 *       }
	 *       // Revalidate the timestamps
	 *       if( !validateTimestamp() ) {
	 *          return false;
	 *       }
	 *    }
	 * }
	 * 
	 * // Revalidate the taskObject
	 * if( validateTaskObject() == false ) {
	 *    return false;
	 * }
	 * 
	 * //---------------------------------------------------
	 * //
	 * // Note the following is done WITH a task lock
	 * // (including getting the lock itself)
	 * //
	 * //---------------------------------------------------
	 * 
	 * try to get task lock
	 * if ( task lock failed ) {
	 *    return false;
	 * }
	 * 
	 * //
	 * // Task lock is assumed from here
	 * //
	 * 
	 * // Get an existing test object, and process it
	 * taskObject = getExistingTaskObject()
	 * 
	 * // Revalidate the taskObject
	 * if( validateTaskObject() == false ) {
	 *    return false;
	 * }
	 * 
	 * // Create a new taskObject if needed
	 * if( taskObject == null ) {
	 *    create new taskObject
	 * }
	 * 
	 * // Finally execute the task
	 * execute the task thread
	 * 
	 * update taskObject with start time, update time, and blank ending time (0), and start status
	 * taskObject.saveDelta()
	 * 
	 * while ( task is running ) {
	 *    wait abit (to prevent infinite loop)
	 *    
	 *    update taskObject with update time, running status
	 *    taskObject.saveDelta()
	 * }
	 * 
	 * update taskObject with update time, ending time, and success/failure status
	 * taskObject.saveDelta()
	 * 
	 * Finally return the final task status
	 * ```
	 */

	// Get data object id, given the taskname - if it does not exist


	protected DataObject getCachedDataObject(String taskName) {
		// Get a known OID
		String knownOID = taskOIDMap.get(taskName);

		// Object to returned
		DataObject retObj = null;

		// Get DataObject using the known knownOID
		if( knownOID != null ) {
			retObj = taskMap.get(knownOID);
			// If its valid (not null)
			if( retObj != null ) {
				return retObj;
			}
		}

		// No object found - return null
		return null;
	}

	// Get dataObject 
	// - cache the _oid (to skip query lookup)
	// - get and return data object
	
	// Claim data object ownership, but setting host + managerID

	//----------------------------------------------------------------
	//
	//  DataObject integrations
	//
	//----------------------------------------------------------------
	
	/**
	 * Issue out a lock
	 * 
	 * See: LockTokenManager.issueLockToken
	 * 
	 * @param taskName               `lockID` in LockTokenManager
	 * 
	 * @return  renewLockToken result
	 */
	protected long issueLockToken(String taskName) {
		long ret = super.issueLockToken(taskName);
		
		return ret;
	}
	
	/**
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
		long ret = super.renewLockToken(taskName, lockToken);
		
		// Return the result
		return ret;
	}
	
	/**
	 * Given a previously initialized lockToken, taskName, and runner
	 * Does the continous relocking and "thread sleep loop", till the task is complete
	 * 
	 * This function BLOCKS the current thread, till the task is complete. Or if task failed.
	 * 
	 * This function is extended to update 
	 * 
	 * @param taskName     to perform locking renewal on / unlocks on
	 * @param runThread    thread to actually start, and wait for (to join)
	 * @param inLockToken  lock token to use for renewal
	 * 
	 * @return true if runnable completes without interruptions
	 */
	protected boolean executeRunnable_withExistingLock(String taskName, Runnable runner,
		long inLockToken) {
		
		// Execute with results
		boolean res = super.executeRunnable_withExistingLock(taskName, runner, inLockToken);
		
		// Return final result
		return res;
	}

	//----------------------------------------------------------------
	//
	//  Task scheduling
	//
	//----------------------------------------------------------------

	/**
	 * Schedule a runnable, with interval and delay configured
	 * 
	 * @param taskName          to register as
	 * @param runner            runner to use
	 * @param minIntervalRate   minimum interval between task runs in milliseconds
	 * @param minDelay          minimum delay between task runs in milliseconds
	 */
	public void scheduleRunnableTask(String taskName, Runnable runner, long minIntervalRate, long minDelay) {
		registerRunnableTask(taskName, runner);
		intervalMap.put(taskName, Math.max(0l, minIntervalRate));
		delayMap.put(taskName, Math.max(0l, minDelay));
	}
	
}