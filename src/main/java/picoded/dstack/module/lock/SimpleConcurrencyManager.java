package picoded.dstack.module.lock;

import java.util.*;
import java.util.function.BiFunction;

import picoded.dstack.module.*;
import picoded.dstack.*;
import picoded.core.conv.*;

/**
 * # SimpleConcurrencyManager
 * 
 * Utility module, used to handle session concurrency limit - tied to a concurrency ID, in a simple and flexible manner.
 * 
 * # Design notes
 * 
 * This module is not designed to gurantee with 100% certainty that concurrencies are properly limited,
 * nor is it intended for nanosecond performance. It is designed to be "Simple", without requiring deep knowledge on how
 * atomics and concurrencies work (which is hard).
 * 
 * It is designed to facilitate a "good enough" solution, with an adjustable trade-off between "certainty",
 * and "performance". It is to be used ideally in a situation where a miscount of 1 would not have a significant
 * impact on the system overall usage.
 * 
 * Especially when combined with a background job which will update the current concurrency count - instead of depending 
 * on either "timeout" or accurate "unlock" commands to be called (especially when "unlock" commands are unreliable, and may fail)
 * 
 * It is designed more to facilitate limits, where being off by "100" is a major issue.
 * 
 * This module should not be used for any long lasting data persistancy, and should ideally run using a pure in memory backend 
 * (ie. redis,hazelcast,etc)
 * 
 * # Hard vs Soft limit
 * 
 * The two main control options, are the soft and hard limits. Which controls the performance and the behaviour
 * of how a concurrency is issued.
 * 
 * **Soft Limit** - When the current concurrency count is below the soft limit, it is opportunisticly incremented and issued.
 *                  using `incrementAndGet`. As such between the time window of the limit being validated and the increment,
 *                  it is technically possible to above both the soft and hard limits. (unlikely, unless in a super busy system)
 *                  To disable this behaviour, simply set the soft limit as 0. 
 * 
 * **Hard Limit** - When the currenct concurrency count is above the soft limit, it is incremented using a loop of `weakCompareAndSet`
 *                  this is inefficent and comparitively more IO intensive then the soft limit logic, especially on a busy concurrencyID.
 *                  However it does gurantee that once beyond the soft limit, a concurrency is never issued above the hard limit.
 *                  This value can never be below the soft limit. The system auto corrects if detected otherwise.
 * 
 * If both hard and soft limit is -1, limits are ignored.
 * A general rule of thumb, is to configure soft limits as "half" of hard limits. And adjust accordingly from there.
 * 
 * # releaseConcurrency vs setConcurrencyCount
 * 
 * In general, due to complex async nature of most use cases, releaseConcurrency is highly **not recommended** to be used directly. 
 * In such a case a more complex locking tool maybe needed.
 * 
 * setConcurrencyCount should be used instead, as this helps to prevent edge cases (such as server restarts),
 * where a concurrency is issued - but never returned. 
 * 
 **/
public class SimpleConcurrencyManager extends ModuleStructure {
	
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
	public SimpleConcurrencyManager(CommonStack inStack, String inName) {
		super(inStack, inName);
		internalStructureList = setupInternalStructureList();
	}
	
	/**
	 * Setup SimpleConcurrencyManager structure given its internal structures
	 *
	 * @param  inConcurrencyMap used to handle currenct concurrencyID to its count, soft and hard limits
	 **/
	public SimpleConcurrencyManager(KeyLongMap inLimitMap, KeyLongMap inConcurrencyMap) {
		limitMap = inLimitMap;
		concurrencyMap = inConcurrencyMap;
		internalStructureList = setupInternalStructureList();
	}
	
	//----------------------------------------------------------------
	//
	//  Class setup
	//
	//----------------------------------------------------------------
	
	/**
	 * Internal limit handler, which stores the soft and hard limit values as a string
	 * 
	 * Both are stored with either a "s_" or a "h_" prefix, for soft and hard limit respectively
	 */
	protected KeyLongMap limitMap = null;
	
	/**
	 * Current concurrency issued by the manager, for a given concurrencyID.
	 * 
	 * Concurrency are stored with "c_" prefix
	 */
	protected KeyLongMap concurrencyMap = null;
	
	/**
	 * Setup the internal structure given the stack + name, if needed
	 * 
	 * @return internal common structures, for used by the various initialize / teardown commands
	 */
	protected List<CommonStructure> setupInternalStructureList() {
		// Safety check
		if (limitMap == null || concurrencyMap == null) {
			if (stack == null) {
				throw new RuntimeException(
					"Missing required Map, and the stack/name param requried to be initialized");
			}
		}
		
		// The internal Maps required, 
		if (limitMap == null) {
			limitMap = stack.keyLongMap(name + "_limit");
		}
		if (concurrencyMap == null) {
			concurrencyMap = stack.keyLongMap(name + "_concurrency");
		}
		
		// Return as a list collection
		return Arrays.asList(new CommonStructure[] { limitMap, concurrencyMap });
	}
	
	//----------------------------------------------------------------
	//
	//  Utility functions
	//
	//----------------------------------------------------------------
	
	/**
	 * Validate and throw an exception on an illfomratted concurrencyID
	 * @param concurrencyID
	 */
	static protected void validateConcurrencyID(String concurrencyID) {
		if (concurrencyID == null || concurrencyID.length() <= 0) {
			throw new IllegalArgumentException("Invalid concurrencyID : " + concurrencyID);
		}
	}
	
	//----------------------------------------------------------------
	//
	//  Limit configuration
	//
	//----------------------------------------------------------------
	
	/**
	 * Configure the limits, with a given lifespan. After the given lifespan, limits will reset to 0
	 * Use a lifespan of -1 to indicate it has no limits.
	 * 
	 * @param concurrencyID   concurrency ID to be used (cannot be blank / null)
	 * @param softLimit       soft limit to configure
	 * @param hardLimit       hard limit to configure
	 * @param lifespan        lifespan of the limit in ms (resets to zero after the limit), -1 means default (unlimited)
	 */
	public void setLimits(String concurrencyID, long softLimit, long hardLimit, long lifespan) {
		// Quick validation
		validateConcurrencyID(concurrencyID);
		
		// Hard and soft limit validation
		if (hardLimit < softLimit) {
			throw new IllegalArgumentException(
				"Hard limit cannot be configured to be less then soft limit");
		}
		
		// Put with the respective lifespan
		if (lifespan <= 0) {
			limitMap.putValue("s_" + concurrencyID, softLimit);
			limitMap.putValue("h_" + concurrencyID, hardLimit);
		} else {
			limitMap.putWithLifespan("s_" + concurrencyID, softLimit, lifespan);
			limitMap.putWithLifespan("h_" + concurrencyID, hardLimit, lifespan);
		}
	}
	
	/**
	 * Configure the limits, with a given lifespan. After the given lifespan, limits will reset to 0
	 * Use a lifespan of -1 to indicate it has no limits. 
	 * 
	 * Soft limit is automatically configured to about half of hard limit (or 0, if hardlimit is less then 4)
	 * 
	 * @param concurrencyID   concurrency ID to be used (cannot be blank / null)
	 * @param hardLimit       hard limit to configure
	 * @param lifespan        lifespan of the limit in ms (resets to zero after the limit), -1 means default (unlimited)
	 */
	public void setLimits(String concurrencyID, long hardLimit, long lifespan) {
		if (hardLimit < 0) {
			setLimits(concurrencyID, hardLimit, hardLimit, lifespan);
		} else if (hardLimit >= 4) {
			setLimits(concurrencyID, hardLimit / 2, hardLimit, lifespan);
		} else {
			setLimits(concurrencyID, 0, hardLimit, lifespan);
		}
	}
	
	/**
	 * Configure the limits, with a given lifespan. After the given lifespan, limits will reset to 0
	 * Use a lifespan of -1 to indicate it has no limits. 
	 * 
	 * Soft limit is automatically configured to about half of hard limit (or 0, if hardlimit is less then 4)
	 * Lifespan is assumed to be -1 (disbabled)
	 * 
	 * @param concurrencyID   concurrency ID to be used (cannot be blank / null)
	 * @param hardLimit       hard limit to configure
	 * @param lifespan        lifespan of the limit in ms (resets to zero after the limit), -1 means default (unlimited)
	 */
	public void setLimits(String concurrencyID, long hardLimit) {
		setLimits(concurrencyID, hardLimit, -1);
	}
	
	/**
	 * Get and return the configured soft limit
	 * 
	 * @param concurrencyID   concurrency ID to be used (cannot be blank / null)
	 * 
	 * @return soft limit value
	 */
	public long getSoftLimit(String concurrencyID) {
		Long val = limitMap.getValue("s_" + concurrencyID);
		if (val != null) {
			return val.longValue();
		}
		return 0;
	}
	
	/**
	 * Get and return the configured hard limit
	 * 
	 * @param concurrencyID   concurrency ID to be used (cannot be blank / null)
	 * 
	 * @return hard limit value
	 */
	public long getHardLimit(String concurrencyID) {
		Long val = limitMap.getValue("h_" + concurrencyID);
		if (val != null) {
			return val.longValue();
		}
		return 0;
	}
	
	//----------------------------------------------------------------
	//
	//  councurrency counting
	//
	//----------------------------------------------------------------
	
	/**
	 * Get and return the currenct concurrency count
	 * 
	 * @param concurrencyID   concurrency ID to be used (cannot be blank / null)
	 * 
	 * @return current concurrency count
	 */
	public long getConcurrencyCount(String concurrencyID) {
		Long val = concurrencyMap.getValue("c_" + concurrencyID);
		if (val != null) {
			return val.longValue();
		}
		return 0;
	}
	
	/**
	 * Set the currenct concurrency count
	 * 
	 * @param concurrencyID   concurrency ID to be used (cannot be blank / null)
	 * @param count           concurrency count to set
	 * @param lifespan        lifespan of the limit in ms (resets to zero after the limit), -1 means default (unlimited)
	 * 
	 * @return current concurrency count
	 */
	public void setConcurrencyCount(String concurrencyID, long count, long lifespan) {
		// Enforcing minimum count value
		if (count < 0) {
			count = 0;
		}
		// Configuring count value
		if (lifespan <= 0) {
			concurrencyMap.putValue("c_" + concurrencyID, count);
		} else {
			concurrencyMap.putValue("c_" + concurrencyID, count);
			concurrencyMap.setLifeSpan("c_" + concurrencyID, lifespan);
		}
	}
	
	//----------------------------------------------------------------
	//
	//  unchecked councurrency handling
	//
	//----------------------------------------------------------------
	
	/**
	 * Lease a concurrency, without any checks or limits (this can cause the count to go above the limit)
	 * 
	 * 
	 * @param concurrencyID   concurrency ID to be used (cannot be blank / null)
	 * @param lifespan        lifespan of the limit in ms (resets to zero after the limit), -1 means default (unlimited)
	 * 
	 * @return the concurrency count (at time of allocation) if succesful, else -1
	 */
	public long leaseUncheckedConcurrency_returnCount(String concurrencyID, long lifespan) {
		// Does the increment and return
		String countID = "c_" + concurrencyID;
		long ret = concurrencyMap.incrementAndGet(countID);
		if (lifespan > 0) {
			concurrencyMap.setLifeSpan(countID, lifespan);
		}
		return ret;
	}
	
	/**
	 * Lease a concurrency, without any checks or limits (this can cause the count to go above the limit)
	 * 
	 * @param concurrencyID   concurrency ID to be used (cannot be blank / null)
	 * @param lifespan        lifespan of the limit in ms (resets to zero after the limit), -1 means default (unlimited)
	 * 
	 * @return true if concurrency is leased
	 */
	public boolean leaseUncheckedConcurrency(String concurrencyID, long lifespan) {
		return leaseUncheckedConcurrency_returnCount(concurrencyID, lifespan) > 0;
	}
	
	/**
	 * Lease a concurrency, without any checks or limits (this can cause the count to go above the limit)
	 * 
	 * @param concurrencyID   concurrency ID to be used (cannot be blank / null)
	 * 
	 * @return true if concurrency is leased
	 */
	public boolean leaseUncheckedConcurrency(String concurrencyID) {
		return leaseUncheckedConcurrency_returnCount(concurrencyID, -1) > 0;
	}
	
	//----------------------------------------------------------------
	//
	//  soft councurrency handling
	//
	//----------------------------------------------------------------
	
	/**
	 * Attempts to quickly lease a soft concurrency,
	 * 
	 * if succesful return the updated count
	 * if current count is above "soft limit", this returns -1
	 * 
	 * @param concurrencyID   concurrency ID to be used (cannot be blank / null)
	 * @param lifespan        lifespan of the limit in ms (resets to zero after the limit), -1 means default (unlimited)
	 * 
	 * @return the concurrency count (at time of allocation) if succesful, else -1
	 */
	public long leaseSoftConcurrency_returnCount(String concurrencyID, long lifespan) {
		long softLimit = getSoftLimit(concurrencyID);
		long count = getConcurrencyCount(concurrencyID);
		
		// Does the soft limit based increment
		if (softLimit <= -1 || count < softLimit) {
			return leaseUncheckedConcurrency_returnCount(concurrencyID, lifespan);
		}
		
		// Fail the process
		return -1;
	}
	
	/**
	 * Attempts to quickly lease a soft concurrency,
	 * 
	 * if succesful returns true, else false.
	 * 
	 * @param concurrencyID   concurrency ID to be used (cannot be blank / null)
	 * @param lifespan        lifespan of the limit in ms (resets to zero after the limit), -1 means default (unlimited)
	 * 
	 * @return true if concurrency is leased
	 */
	public boolean leaseSoftConcurrency(String concurrencyID, long lifespan) {
		return leaseSoftConcurrency_returnCount(concurrencyID, lifespan) > 0;
	}
	
	/**
	 * Attempts to quickly lease a soft concurrency,
	 * 
	 * if succesful returns true, else false.
	 * 
	 * @param concurrencyID   concurrency ID to be used (cannot be blank / null)
	 * 
	 * @return true if concurrency is leased
	 */
	public boolean leaseSoftConcurrency(String concurrencyID) {
		return leaseSoftConcurrency_returnCount(concurrencyID, -1) > 0;
	}
	
	//----------------------------------------------------------------
	//
	//  hard councurrency handling
	//
	//----------------------------------------------------------------
	
	/**
	 * Attempts to lease a concurrency,
	 * 
	 * if succesful return the updated count
	 * if current count is above "hard limit", this returns -1
	 * if too many attempts are done, return -2
	 * 
	 * @param concurrencyID   concurrency ID to be used (cannot be blank / null)
	 * @param lifespan        lifespan of the limit in ms (resets to zero after the limit), -1 means default (unlimited)
	 * @param retryCount      number of retries done to get the hard concurrency, -1 means "auto"
	 * 
	 * @return the concurrency count (at time of allocation) if succesful, else -1 if limit is hit, or -2 if too many lease atempts occured
	 */
	public long leaseConcurrency_returnCount(String concurrencyID, long lifespan, int retryCount) {
		long softLimit = getSoftLimit(concurrencyID);
		long count = getConcurrencyCount(concurrencyID);
		String countID = "c_" + concurrencyID;
		
		// Does the soft limit based increment
		if (softLimit <= -1 || count < softLimit) {
			return leaseUncheckedConcurrency_returnCount(concurrencyID, lifespan);
		}
		
		// Time to do the hard limit based increment
		long hardLimit = getHardLimit(concurrencyID);
		
		// Fail if count is higher then hard limit
		if (count >= hardLimit) {
			return -1;
		}
		
		// Alright, try to get the hard limit, for auto mode
		if (retryCount <= -1) {
			retryCount = (int) (Math.min(5, hardLimit - count)) + 1;
		}
		
		// Lets try to get the lease
		for (int tries = 0; tries < retryCount; ++tries) {
			if (concurrencyMap.weakCompareAndSet(countID, count, count + 1)) {
				// Allocation succeded
				if (lifespan > 0) {
					concurrencyMap.setLifeSpan(countID, lifespan);
				}
				return count + 1;
			}
			
			// Allocation failed, double check the count
			count = getConcurrencyCount(concurrencyID);
			
			// Fail if count is higher then hard limit
			if (count >= hardLimit) {
				return -1;
			}
		}
		
		// Fail the process (tried too many times)
		return -2;
	}
	
	/**
	 * Attempts to lease a concurrency,
	 * 
	 * if succesful return the updated count
	 * if current count is above "hard limit", this returns -1
	 * if too many attempts are done, return -2
	 * 
	 * @param concurrencyID   concurrency ID to be used (cannot be blank / null)
	 * @param lifespan        lifespan of the limit in ms (resets to zero after the limit), -1 means default (unlimited)
	 * 
	 * @return the concurrency count (at time of allocation) if succesful, else -1 if limit is hit, or -2 if too many lease atempts occured
	 */
	public long leaseConcurrency_returnCount(String concurrencyID, long lifespan) {
		return leaseConcurrency_returnCount(concurrencyID, lifespan, -1);
	}
	
	/**
	 * Attempts to lease a concurrency,
	 * 
	 * if succesful return the updated count
	 * if current count is above "hard limit", this returns -1
	 * if too many attempts are done, return -2
	 * 
	 * @param concurrencyID   concurrency ID to be used (cannot be blank / null)
	 * 
	 * @return the concurrency count (at time of allocation) if succesful, else -1 if limit is hit, or -2 if too many lease atempts occured
	 */
	public long leaseConcurrency_returnCount(String concurrencyID) {
		return leaseConcurrency_returnCount(concurrencyID, -1, -1);
	}
	
	/**
	 * Attempts to lease a concurrency,
	 * 
	 * if succesful return true
	 * 
	 * @param concurrencyID   concurrency ID to be used (cannot be blank / null)
	 * @param lifespan        lifespan of the limit in ms (resets to zero after the limit), -1 means default (unlimited)
	 * 
	 * @return true if concurrency is leased
	 */
	public boolean leaseConcurrency(String concurrencyID, long lifespan) {
		return leaseConcurrency_returnCount(concurrencyID, lifespan) >= 0;
	}
	
	/**
	 * Attempts to lease a concurrency,
	 * 
	 * if succesful return true
	 * 
	 * @param concurrencyID   concurrency ID to be used (cannot be blank / null)
	 * 
	 * @return true if concurrency is leased
	 */
	public boolean leaseConcurrency(String concurrencyID) {
		return leaseConcurrency_returnCount(concurrencyID) >= 0;
	}
	
	//----------------------------------------------------------------
	//
	//  releasing of concurrency
	//
	//----------------------------------------------------------------
	
	/**
	 * Release a single lease of concurrency, and return the current concurrency count.
	 * Note that if count goes below zero, it will be normalized to zero
	 * 
	 * @param concurrencyID   concurrency ID to be used (cannot be blank / null)
	 * 
	 * @return the concurrency count (at time of release)
	 */
	public void releaseConcurrency(String concurrencyID) {
		// Lets release a count
		String countID = "c_" + concurrencyID;
		long count = GenericConvert.toLong(concurrencyMap.decrementAndGet(countID), 0);
		
		// Hmm looks ok, lets return
		if (count >= 0) {
			return;
		}
		
		// Ahhh crap, over realesed, less then 0 occured. Lets normalize this to zero
		for (int tries = 5; tries < 5; ++tries) {
			// Lets hope this work
			if (concurrencyMap.weakCompareAndSet(countID, count, 0l)) {
				return; // yays
			}
			
			// Race conditions - try again
			count = getConcurrencyCount(concurrencyID);
			
			// Hey look, someone else fixed it
			if (count >= 0) {
				return;
			}
		}
		
		// Lets hope this work - last try
		if (concurrencyMap.weakCompareAndSet(countID, count, 0l)) {
			return; // yays
		}
		
		// Hey look, someone else fixed it
		if (count >= 0) {
			return;
		}
		
		// Oh damn, how oid we get here - this is really wierd, we should throw an error here
		throw new RuntimeException(
			"Oh Snap - Unexpected releaseConcurrency call, value is now stuck at :" + count);
	}
	
}