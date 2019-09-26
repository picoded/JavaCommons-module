package picoded.dstack.module.thread;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.net.InetAddress;
import java.security.SecureRandom;

import picoded.dstack.module.*;
import picoded.dstack.*;
import picoded.dstack.connector.jsql.JSqlException;
import picoded.core.common.MSLongTime;
import picoded.core.conv.*;
import picoded.core.struct.query.*;

/**
 * Extends RunnableTaskManager 
 * This is not meant for direct use - use RunnableTaskCluster instead
 * 
 * This intentionally does not implement the ExecutorService integration (which is done on RunnableTaskCluster)
 * 
 * - add support for DataObjectMap storing state information (that can be queryed - in the future)
 * - add task scheduling awarness when executing
 */
class RunnableTaskClusterBase extends RunnableTaskManager {
	
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
	public RunnableTaskClusterBase(CommonStack inStack, String inName) {
		super(inStack, inName);
		internalStructureList = setupInternalStructureList();
	}
	
	/**
	 * Setup RunnableTaskCluster structure given its internal structures
	 *
	 * @param  inTaskMap used to track various task states
	 * @param  inLockMap used to handle task locking
	 **/
	public RunnableTaskClusterBase(DataObjectMap inTaskMap, KeyLongMap inLockMap) {
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
	 * Task global state map - this acts as a persistent store for state mapping
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
	 * Internal task to DataObject OID mapping cache
	 */
	protected ConcurrentHashMap<String, String> cache_taskOIDMap = new ConcurrentHashMap<>();
	
	/**
	 * Internal last known task ending cache, or check done (to skip any lock calls when its not needed, saving DB hits)
	 */
	protected ConcurrentHashMap<String, Long> cache_lastKnownTaskUpdateMap = new ConcurrentHashMap<>();
	
	/**
	 * Internal last known task start cache, or check done (to skip any lock calls when its not needed, saving DB hits)
	 */
	protected ConcurrentHashMap<String, Long> cache_lastKnownTaskStartMap = new ConcurrentHashMap<>();
	
	/**
	 * Inveral mapping between task
	 */
	protected ConcurrentHashMap<String, Long> intervalMap = new ConcurrentHashMap<>();
	
	/**
	 * Inveral mapping between task
	 */
	protected ConcurrentHashMap<String, Long> delayMap = new ConcurrentHashMap<>();
	
	// //----------------------------------------------------------------
	// //
	// //  Internal Manager identifier setup
	// //
	// //----------------------------------------------------------------
	
	// /**
	//  * The current RunnableTaskManager GUID, initialized on instance construction
	//  */
	// protected String _managerID = GUID.base58();
	
	// /**
	//  * @return the unique base 58 GUID of the task manager
	//  */
	// public String getManagerID() {
	// 	return _managerID;
	// }
	
	// // Server host address memoizer caching
	// protected String _hostAddress = null;
	
	// /**
	//  * @return the server instance various host address, note that this maybe cached
	//  */
	// public String getHostAddress() {
	// 	// Return cached address
	// 	if (_hostAddress != null) {
	// 		return _hostAddress;
	// 	}
	
	// 	// Lets get it
	// 	try {
	// 		InetAddress host = InetAddress.getLocalHost();
	// 		_hostAddress = "" + host.getHostAddress();
	// 	} catch (Exception e) {
	// 		// Host address fetch failed
	// 		_hostAddress = "unknown";
	// 	}
	
	// 	// Return the configured host address
	// 	return _hostAddress;
	// }
	
	//----------------------------------------------------------------
	//
	//  scheduled task setup
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
	public void scheduleRunnableTask(String taskName, Runnable runner, long minIntervalRate,
		long minDelay) {
		registerRunnableTask(taskName, runner);
		intervalMap.put(taskName, Math.max(1l, minIntervalRate));
		delayMap.put(taskName, Math.max(1l, minDelay));
	}
	
	/**
	 * Schedule a runnable, with minimum interval, minDelay is set as 1l
	 * 
	 * @param taskName          to register as
	 * @param runner            runner to use
	 * @param minIntervalRate   minimum interval between task runs in milliseconds
	 */
	public void scheduleRunnableTask(String taskName, Runnable runner, long minIntervalRate) {
		scheduleRunnableTask(taskName, runner, minIntervalRate, 1l);
	}
	
	/**
	 * Returns true, if taskName is configured with a task schedule
	 * 
	 * @param taskName
	 * @return true, if task was scheduled
	 */
	public boolean isScheduledTask(String taskName) {
		return intervalMap.containsKey(taskName) && delayMap.containsKey(taskName);
	}
	
	/**
	 * Strict varient of isScheduledTask - throws an exception if its not a scheduled task
	 * 
	 * @param taskName
	 */
	protected void isScheduledTask_strict(String taskName) {
		if (!isScheduledTask(taskName)) {
			throw new IllegalArgumentException("Given taskName - is not a scheduled task : "
				+ taskName);
		}
	}
	
	//----------------------------------------------------------------
	//
	//  isRunnable extension / overwrite
	//
	//----------------------------------------------------------------
	
	//
	//  WARNING !!!
	//
	// The following pesudo code IS intentional
	//
	// this topic of concurrency management is a compelx topic,
	// especially when taking into account simlutanious threads in race condition
	//
	// Before making modification to the following code
	// read and understand the pesudo code.
	// 
	// And consult eugene (PicoCreator) if the change you made has a major
	// negative impact (or worse bugs) on the logical flow
	//
	// If its hard enough for me to need to have all this pesudo code to visualize
	// how race conditions would play out - its probably means you need it too!
	//
	
	/*
	 * # Fail fast "isRunnableTask" - pesudo code
	 * 
	 * The general idea, is to "fail early" if possible, without doing expensive locks
	 * unless it is truely required. This is designed to use the locally stored cached
	 * state of various task to automate the runnable process
	 * 
	 * Functions declared in the pesudo code, are actions which are repeated (maybe)
	 * And should ideally be a function (duh)
	 * 
	 * This whole comment block is done to facilitate the complex understanding 
	 * of how the "fail fast" was designed to work
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
	 * function validateCachedTimestamp {
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
	 * ```
	 */
	
	/**
	 * Check existing cached of timestamp, if its runnable.
	 * Given the task name.
	 * 
	 * @param taskName
	 * @return false, if task is not runnable (true - does not gurantee runnability)
	 */
	protected boolean isRunnableTask_validateCachedTimestamp(String taskName) {
		// Get the current timestamp
		long now = System.currentTimeMillis();
		
		// Get existing lastStart / update timestamp
		long lastStart = cache_lastKnownTaskStartMap.getOrDefault(taskName, 0l);
		long lastUpdate = cache_lastKnownTaskUpdateMap.getOrDefault(taskName, 0l);
		
		//System.out.println("Evaluating isRunnableTask_validateCachedTimestamp - now/start/update "+now+"/"+lastStart+"/"+lastUpdate);
		
		// Fast fail any invalid start / update timings
		if (lastStart > 0l) {
			if (now < (lastStart + intervalMap.getOrDefault(taskName, 0l))) {
				return false;
			}
		}
		if (lastUpdate > 0l) {
			if (now < (lastUpdate + delayMap.getOrDefault(taskName, 0l))) {
				return false;
			}
		}
		
		// Return valid =]
		//System.out.println("Evaluating isRunnableTask_validateCachedTimestamp - true");
		return true;
	}
	
	/**
	 * Get the cached task object, if possible
	 * 
	 * @param taskName
	 * @return if found, else null
	 */
	protected DataObject getCachedTaskObject(String taskName) {
		// Get a known OID
		String knownOID = cache_taskOIDMap.get(taskName);
		
		// Object to returned
		DataObject retObj = null;
		
		// Check if knownOID was configured
		if (knownOID != null) {
			// Fast exit on blank cache (requires object setup with lock)
			if (knownOID.isEmpty()) {
				return null;
			}
			
			// Get DataObject using the known knownOID
			retObj = taskMap.get(knownOID);
			// If its valid - return (not null)
			if (retObj != null) {
				// Name safety check
				if (taskName.equals(retObj.getString("name"))) {
					return retObj;
				}
				// Failed ! - move on to failure handling
			}
		} else {
			// No known OID, lets do a quick query search
			retObj = taskMap.queryAny("name = ?", new Object[] { taskName });
			// If its valid - cache and return (not null)
			if (retObj != null) {
				cache_taskOIDMap.put(taskName, retObj._oid());
				return retObj;
			}
		}
		
		// If invalid - blank out the cache
		// note that if this was originally blanked ""
		// a return null happens above when knownOID.isEmpty()
		cache_taskOIDMap.put(taskName, "");
		
		// No object found - return null
		return null;
	}
	
	/**
	 * Given the task object, get updated task activity / start timestamp
	 * And revaluate "isRunnable" logic
	 * 
	 * @param taskObj 
	 * @return false, if task is not runnable (true - does not gurantee runnability)
	 */
	protected boolean isRunnableTask_validateTaskObject(String taskName, DataObject taskObj) {
		// Get existing lastStart / update timestamp
		long cache_lastStart = cache_lastKnownTaskStartMap.getOrDefault(taskName, 0l);
		long cache_lastUpdate = cache_lastKnownTaskUpdateMap.getOrDefault(taskName, 0l);
		
		// Get the task start / update
		long lastStart = taskObj.getLong("lastStartTime", 0l);
		long lastUpdate = taskObj.getLong("lastUpdateTime", 0l);
		
		// Update caches if needed
		if (cache_lastStart < lastStart) {
			cache_lastKnownTaskStartMap.put(taskName, lastStart);
		}
		if (cache_lastUpdate < lastUpdate) {
			cache_lastKnownTaskUpdateMap.put(taskName, lastUpdate);
		}
		// Udpate the task map caching (if needed)
		cache_taskOIDMap.put(taskName, taskObj._oid());
		
		// Revalidate cached timestamp
		return isRunnableTask_validateCachedTimestamp(taskName);
	}
	
	/**
	 * Given the task name, check if task is runnable - WITHOUT checking for locks
	 * @param taskName
	 * @return false, if task is not runnable (true - does not gurantee runnability)
	 */
	protected boolean isRunnableTask_cacheOnly_withoutLockCheck_norStrictScheduleCheck(
		String taskName) {
		// Cached timestamp is invalid - return false
		if (isRunnableTask_validateCachedTimestamp(taskName) == false) {
			//System.out.println("Cache validation failed");
			return false;
		}
		// Get a cached task object
		DataObject taskObj = getCachedTaskObject(taskName);
		if (taskObj != null) {
			//System.out.println("Found task object - lets validate");
			if (isRunnableTask_validateTaskObject(taskName, taskObj) == false) {
				return false;
			}
		}
		
		//System.out.println("Should return true");
		// Return true, as all checks above passed
		return true;
	}
	
	/**
	 * Varient of isRunnableTask - specifically for scheduled tasks
	 * 
	 * @param taskName
	 * @return false, if task is not runnable (true - does not gurantee runnability)
	 */
	public boolean isRunnableScheduledTask(String taskName) {
		// Does a strict check if its a scheulded task
		isScheduledTask_strict(taskName);
		
		// Check using the cache only, return false early if needed
		if (isRunnableTask_cacheOnly_withoutLockCheck_norStrictScheduleCheck(taskName) == false) {
			return false;
		}
		
		// Does the original "isLocked"
		return super.isRunnableTask(taskName);
	}
	
	//----------------------------------------------------------------
	//
	//  Execute scheduled task
	//
	//----------------------------------------------------------------
	
	/*
	 * # Execute scheduled task - pesudo code
	 * 
	 * Functions declared in the pesudo code, are actions which are repeated (maybe)
	 * And should ideally be a function (duh)
	 * 
	 * This whole comment block is done to facilitate the complex understanding 
	 * of how a scheduled task is validated and handled
	 * 
	 * ```
	 * //---------------------------------------------------
	 * //
	 * // Note the following is done WITHOUT a task lock
	 * //
	 * //---------------------------------------------------
	 * 
	 * // Does a fail fast isRunnable
	 * if( isRunnableTask_validateTaskObject() == false ) {
	 *    return false;
	 * }
	 * 
	 * //---------------------------------------------------
	 * //
	 * // Note the following is done WITH a task lock
	 * // (including getting the lock itself)
	 * //
	 * // Most of this is implemented by extending 
	 * // onto function "hooks" of RunnableTaskManager
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
	
	//----------------------------------------------------------------
	//
	//  RunnableTaskManager integrations
	//
	//----------------------------------------------------------------
	
	/**
	 * !!! This is ASSUMED to be done with an existing lock - which is needed
	 * to work around race condition edge cases
	 * 
	 * Get a cached task object (if found), else issue a new task object
	 * 
	 * @param taskName
	 * @return
	 */
	protected DataObject getOrIssueTaskObject(String taskName) {
		// Return cached object quickly if valid
		DataObject ret = getCachedTaskObject(taskName);
		if (ret != null) {
			return ret;
		}
		
		// Alrighto, time to make a new object
		ret = taskMap.newEntry();
		ret.put("name", taskName);
		ret.saveAll();
		
		// Return the task object
		return ret;
	}
	
	/**
	 * !!! This is ASSUMED to be done with an existing lock - which is needed
	 * to work around race condition edge cases
	 * 
	 * Update the created / updated timestamp for a task object
	 * 
	 * @param taskName
	 * @param setStartTime   if true, also update the "lastStartTime"
	 * @return
	 */
	protected DataObject updateTaskObject(String taskName, boolean setStartTime, String status) {
		// DataObject to return
		DataObject ret = getOrIssueTaskObject(taskName);
		
		// Get the current timestamp, and update it
		long now = System.currentTimeMillis();
		ret.put("lastUpdateTime", now);
		cache_lastKnownTaskUpdateMap.put(taskName, now);
		if (setStartTime) {
			ret.put("lastStartTime", now);
			ret.put("runCount", ret.getLong("runCount", 0) + 1L);
			cache_lastKnownTaskStartMap.put(taskName, now);
		}
		ret.put("status", status);

		// Due to the high "lock" rate, saveDelta is 
		// retried on failure
		try {
			ret.saveDelta();
		} catch(JSqlException e) {
			ret.saveDelta();
		}
		
		// And the OID mapping
		cache_taskOIDMap.put(taskName, ret._oid());
		
		// Return the task object
		return ret;
	}
	
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
		// Get the lock token
		long ret = super.issueLockToken(taskName);
		
		// Lets get / issue the task object with a valid lock
		if (ret > 0) {
			updateTaskObject(taskName, true, "started");
		}
		
		// Return the lock token
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
		
		// Lets update the task object with a valid lock
		if (ret > 0) {
			updateTaskObject(taskName, false, "running");
		}
		
		// Return the result
		return ret;
	}
	
	/**
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
		// Lets do the update first, before the return
		if (lockToken > 0) {
			updateTaskObject(taskName, false, "completed");
		}
		
		// Execute with results
		super.returnLockToken(taskName, lockToken);
	}
	
	//----------------------------------------------------------------
	//
	//  Task Execution
	//
	//----------------------------------------------------------------
	
	/**
	 * Executes a previously registered task, only if lock was succesfully acquired
	 * This function BLOCKS the current thread, till the task is complete. Or if task failed.
	 * 
	 * @param taskName         to execute
	 * 
	 * @return true if executed succesfully, else return false if lock failed
	 */
	public boolean executeRunnableTask(String taskName) {
		// Default is to execute within the schedule if it has one
		if (isScheduledTask(taskName)) {
			return executeRunnableTask(taskName, false);
		}
		// Else run it without schedule checks
		return executeRunnableTask(taskName, false);
	}
	
	/**
	 * Executes a previously registered task, only if lock was succesfully acquired
	 * This function BLOCKS the current thread, till the task is complete. Or if task failed.
	 * 
	 * @param taskName         to execute
	 * @param ignoreSchedule   ignore the schedule, and just execute - now - if not locked
	 * 
	 * @return true if executed succesfully, else return false if lock failed
	 */
	public boolean executeRunnableTask(String taskName, boolean ignoreSchedule) {
		// Lets fail fast with the schedule first
		if (ignoreSchedule == false) {
			if (isRunnableTask_cacheOnly_withoutLockCheck_norStrictScheduleCheck(taskName) == false) {
				//System.out.println("Skipping execution : "+taskName);
				return false;
			}
		}
		
		// Execute the runnable, and block accordingly
		//System.out.println("Trying to execute : "+taskName);
		return super.executeRunnableTask(taskName);
	}
	
	/**
	 * Try to execute all runnable scheduled task, blocks till its completed.
	 * 
	 * Tasks are executed seqeuntially.
	 * Task are skipped if they have an existing lock.
	 */
	public void tryAllRunnableScheduledTask() {
		// Get list of objects
		Set<String> taskSet = new HashSet<>(delayMap.keySet());
		
		// Iterate the taskSet - ant attempt to run each one of them
		for (String taskName : taskSet) {
			executeRunnableTask(taskName, false);
		}
	}
	
	// taskMap

	//----------------------------------------------------------------
	//
	//  Task status query
	//
	//----------------------------------------------------------------
	
	/**
	 * Get and return a list of running tasks, sorted by the orderBY string
	 * 
	 * @param orderBy  sorting to perform, default to name if null or blank
	 * 
	 * @return list of task execution status (stored in central store)
	 */
	public List<Map<String,Object>> getRunningTaskList(String orderBy) {
		// Result list
		List<Map<String,Object>> ret = new ArrayList<>();

		// Get the task array and iterate it
		Collection<DataObject> taskArray = taskMap.values();
		for( DataObject obj : taskArray ) {
			// Prepare return result
			ret.add( new HashMap<String,Object>(obj) );

			// Update OID cache
			cache_taskOIDMap.put( obj.getString("name"), obj._oid() );
		}

		// Prepare the orderBy
		if(orderBy == null || orderBy.length() <= 0) {
			orderBy = "name";
		}
		OrderBy sorter = new OrderBy(orderBy);

		// Apply the sorting
		Collections.sort(ret, sorter);

		// Return it
		return ret;
	}
}