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
		if (lockMap == null) {
			if (stack == null || name == null) {
				throw new RuntimeException(
					"Missing required Map, and the stack/name param requried to be initialized");
			}
		}
		
		// The internal Maps required, 
		if (lockMap == null) {
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
	 * @return the lock token if valid, or a negative if no valid token issued
	 */
	protected long setupToken(String lockID, long existingToken, long lockTimeout) {
		// Lets derive the "new" lock token - randomly!
		//
		// This is intentionally an integer value, as it is "random enough"
		// and would avoid a known issue with mysql long accuracy
		long nextLockToken = Math.abs((randObj).nextInt());
		
		// Lets attempt to get a lock
		if (lockMap.weakCompareAndSet(lockID, existingToken, nextLockToken)) {
			// YAY lock succesful - enforce expiry
			lockMap.setLifeSpan(lockID, lockTimeout);
			
			// Validate the existing value, this guard against a narrow
			// lock expriy window which occurs between
			// a weakCompareAndSet, and the setLifeSpan command.
			long registeredToken = lockMap.getLong(lockID);
			if (registeredToken != nextLockToken) {
				return -1;
			}
			
			// Return the lock
			return nextLockToken;
		}
		
		// Double check if there is an expiry - reapply if needed
		// this is to work around hypothetical crashes after weakCompare
		//
		// # UNLOCK_DEADLOCK_WARNING
		//
		// This is intentional to guard against accidental deadlock
		// done during an unlock call. By ensuring when such a large "lock"
		// occurs - multiple "setupToken" will resolve the deadlock.
		//
		// See UNLOCK_DEADLOCK_WARNING below in `unlockToken`
		long currentLifespan = lockMap.getLifespan(lockID);
		if (currentLifespan == 0 || currentLifespan > lockTimeout) {
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
	 * Check and return true, if the given lockID "is locked"
	 * 
	 * @param lockID         to lookup
	 * 
	 * @return true, if lock exists
	 */
	public boolean isLocked(String lockID) {
		// This fetches the expiry, and lock value in a single call
		KeyLong val = lockMap.get(lockID);
		// Validate it
		return val != null && val.longValue() != 0l;
	}
	
	/**
	 * Get an existing lock remaining lifespan
	 * 
	 * @param lockID         to lookup
	 * 
	 * @return -1 if lock does not exist, 0 if no lifespan is configured (possible in race condition), >0 is lifespan left
	 */
	public long getLockLifespan(String lockID) {
		return lockMap.getLifespan(lockID);
	}

	/**
	 * Issues a lock token for the given lockID.
	 * 
	 * NOTE: That if an existing lock exists, with a larger lockTimeout value,
	 *       it will be updated to the lower lockTimeout value
	 * 
	 * @param lockID         to use
	 * @param lockTimeout    lock timeout for token
	 * 
	 * @return the lock token if valid, -1 if no valid token issued
	 */
	public long issueLockToken(String lockID, long lockTimeout) {
		return setupToken(lockID, 0l, lockTimeout);
	}
	
	/**
	 * Issues a lock token for the given lockID
	 * 
	 * NOTE: That if an existing lock exists, with a larger lockTimeout value,
	 *       it will be updated to the lower lockTimeout value
	 * 
	 * @param lockID         to use
	 * @param existingToken  existing token, to renew
	 * @param lockTimeout    lock timeout for token
	 * 
	 * @return the lock token if valid, -1 if no valid token issued
	 */
	public long renewLockToken(String lockID, long originalToken, long lockTimeout) {
		if (originalToken <= 0) {
			throw new RuntimeException("Invalid lock token used (lockID = " + lockID + ") : "
				+ originalToken);
		}
		return setupToken(lockID, originalToken, lockTimeout);
	}
	
	/**
	 * Unlock an existing lock, using the lockID
	 * 
	 * @param lockID         to use
	 * @param existingToken  existing token, to renew
	 * 
	 * @return true, if unlock was succesful, else false if failed
	 */
	public boolean returnLockToken(String lockID, long existingToken) {
		// Lets attempt to do an unlock!
		if (lockMap.weakCompareAndSet(lockID, existingToken, 0l)) {
			//
			// Unlock is done, lets make sure a value expiry is configured.
			//
			// This is done to help ensure unused lock tokenID will eventually 
			// be cleared from the system. However by doing so it introduces,
			// various edge case race conditions, which is addressed below.
			// 
			
			// Get the existing KeyLong
			KeyLong val = lockMap.get(lockID);
			
			// Value was invalidated / expired
			// this is a small time window for this to be able to occur
			// but oh well, it happened, and unlock is succesful - return
			if (val == null) {
				return true;
			}
			
			// Check if there is an existing lock expiry,
			// if configured - returns true if it exists
			// and follow that lock expiry.
			if (val.getExpiry() > 0l) {
				return true;
			}
			
			// Check if value is != 0, meaning its this unlockToken
			// expiry time, belongs to another "setupToken" call
			if (val.longValue() != 0l) {
				return true;
			}
			
			//
			// Lets setup an unlock expiry (to remove the 0l eventually)
			// Currently this is configured as a 24 hour unlock window
			//
			// # UNLOCK_DEADLOCK_WARNING
			//
			// There exist a potential deadlock window, when a succesful lock
			// occurs between an unlock `weakCompareAndSet` and `setLifespan`.
			// (the time window in this comment block?)
			// 
			// This would result in the subsequent setLifespan,
			// either being too long, or too short. As such the default
			// value of this "setLifeSpan", is configured to be 24 hours,
			// a number significantly higher then all current expected use cases.
			//
			// Letting the setupToken, be in charge of 
			// updating the lifespan if needed.
			//
			// Additional measures such as getting the existing keyLong,
			// and validating it for a lifespan value was done above
			// to reduce the possible surface vector.
			// 
			// See UNLOCK_DEADLOCK_WARNING above in `setupToken`
			//
			// @TODO consideration - add "setLifespan_ifBlank" support to KeyLongMap
			//
			lockMap.setLifeSpan(lockID, 24 * 60 * 60 * 1000); // PLEASE READ COMMENT ABOVE (do not remove)
			
			// Return res true for succesful unlock
			return true;
		}
		// Unlock fail
		return false;
	}
}