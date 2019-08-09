package picoded.dstack.module.lock;

import java.util.*;
import java.security.SecureRandom;

import picoded.dstack.module.*;
import picoded.dstack.*;
import picoded.core.conv.*;

/**
 * # LockTokenManager
 * 
 * Module used to handle concurrency locking of `lockID` - this require the usage
 * of an Atomic compliant backend for KeyLongMap.
 * 
 * The class design is intentionally different from traditional java lock API, 
 * to enforce the usage of timeout, and lock renewal.
 * 
 * This allow the lock manager to be more reliably be used if for example.
 * In a configuration with a "low" timeout (<= 1 seconds) 
 * 
 **/
public class LockTokenManager extends ModuleStructure {
	
	//----------------------------------------------------------------
	//
	//  Constructor
	//
	//----------------------------------------------------------------
	
	/**
	 * Setup SimpleConcurrencyManager structure given a stack, and its name
	 *
	 * @param  CommonStack / DStack system to use
	 * @param  Name used to setup the prefix of the complex structure
	 **/
	public LockTokenManager(CommonStack inStack, String inName) {
		super(inStack, inName);
		internalStructureList = setupInternalStructureList();
	}
	
	/**
	 * Setup SimpleConcurrencyManager structure given its internal structures
	 *
	 * @param  inLockMap used to handle currenct concurrencyID to its count, soft and hard limits
	 **/
	public LockTokenManager(KeyLongMap inLockMap) {
		lockMap = inLockMap;
		internalStructureList = setupInternalStructureList();
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
	 * Setup the internal structure given the stack + name, if needed
	 * @return internal common structures, for used by the various initialize / teardown commands
	 */
	protected List<CommonStructure> setupInternalStructureList() {
		// Safety check
		if ( lockMap == null ) {
			if ( stack == null || name == null ) {
				throw new RuntimeException(
					"Missing required Map, and the stack/name param requried to be initialized");
			}
		}
		
		// The internal Maps required, 
		if ( lockMap == null ) {
			lockMap = stack.keyLongMap(name);
		}
		
		// Return as a list collection
		return Arrays.asList(new CommonStructure[] { lockMap });
	}
	
	//----------------------------------------------------------------
	//
	//  Utility functions
	//
	//----------------------------------------------------------------
	
	/**
	 * Internal random number generator
	 */
	SecureRandom randObj = new SecureRandom();

	/**
	 * Issues a lock token internally
	 * 
	 * @param lockID         to use
	 * @param existingToken  to renew, else use 0l to issue a new token
	 * @param lockTimeout    lock timeout for token
	 * 
	 * @return the lock token if valid, -1 if no valid token issued
	 */
	protected long setupLock(String lockID, long existingToken, long lockTimeout) {
		// Lets derive the "new" lock token - randomly!
		long nextLockToken = Math.abs( (randObj).nextLong() );
		
		// Lets attempt to get a lock
		if( lockMap.weakCompareAndSet(lockID,existingToken,nextLockToken) ) {
			// YAY lock succesful - enforce expiry
			lockMap.setLifeSpan(lockID, lockTimeout);

			// Validate the existing value, this guard against a narrow
			// lock expriy window where a 0-value, or renewel occur between
			// a weakCompareAndSet, and the setLifeSpan command.
			long registeredToken = lockMap.getLong(lockID);
			if( lockMap.getLong(lockID) != nextLockToken ) {
				return -1;
			}

			// Return the lock
			return nextLockToken;
		}

		// Double check if there is an expiry - reapply if needed
		// this is to work around hypothetical crashes after weakCompare
		if( lockMap.getExpiry(lockID) == 0 ) {
			lockMap.setLifeSpan(lockID, lockTimeout);
		}

		// Return -1 on lock failure
		return -1;
	}

	//----------------------------------------------------------------
	//
	//  Class setup
	//
	//----------------------------------------------------------------
	
	/**
	 * Issues a lock token for the given lockID
	 * 
	 * @param lockID         to use
	 * @param lockTimeout    lock timeout for token
	 * 
	 * @return the lock token if valid, -1 if no valid token issued
	 */
	public long issueLockToken(String lockID, long lockTimeout) {
		return setupLock(lockID, 0l, lockTimeout);
	}

	/**
	 * Issues a lock token for the given lockID
	 * 
	 * @param lockID         to use
	 * @param existingToken  existing token, to renew
	 * @param lockTimeout    lock timeout for token
	 * 
	 * @return the lock token if valid, -1 if no valid token issued
	 */
	public long renewLockToken(String lockID, long originalToken, long lockTimeout) {
		if(originalToken <= 0) {
			throw new RuntimeException("Invalid lock token : "+originalToken);
		}
		return setupLock(lockID, originalToken, lockTimeout);
	}

	/**
	 * Unlock an existing lock, using the lockID
	 * 
	 * @param lockID         to use
	 * @param lockTimeout    lock timeout for token
	 * 
	 * @return true, if unlock was succesful, else false if failed
	 */
	public boolean releaseLockToken(String lockID, long lockToken) {
		// Lets attempt to do an unlock!
		if( lockMap.weakCompareAndSet(lockID,lockToken,0l) ) {
			// Lets setup an unlock expiry (to remove the 0l eventually)
			lockMap.setLifeSpan(lockID, 60000); // 1 minute unlock expiry

			// Return res true for succesful unlock
			return true;
		}
		// Unlock fail
		return false;
	}
}