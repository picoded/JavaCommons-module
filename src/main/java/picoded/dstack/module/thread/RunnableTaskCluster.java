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
 * - to add more analytics
 * - etc
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
		if ( lockMap == null || taskMap == null ) {
			if ( stack == null || name == null ) {
				throw new RuntimeException(
					"Missing required datastructures requried to be initialized");
			}
		}
		
		// The internal Maps required, 
		super.setupInternalStructureList();
		if ( taskMap == null ) {
			taskMap = stack.dataObjectMap(name+"_state");
		}

		// Return as a list collection
		return Arrays.asList(new CommonStructure[] { lockMap, taskMap });
	}
	
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

	// Server host address
	protected String _hostAddress = null;

	/**
	 * @return the server instance various host address, note that this maybe cached
	 */
	public String getHostAddress() {
		// Return cached address
		if( _hostAddress != null ) {
			return _hostAddress;
		}

		// Lets get it
		try {
			InetAddress host = InetAddress.getLocalHost();
			_hostAddress = ""+host.getHostAddress();
		} catch(Exception e) {
			// Host address fetch failed
			_hostAddress = "unknown";
		}

		// Return the configured host address
		return _hostAddress;
	}
 

}