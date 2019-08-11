package picoded.dstack.module.lock;

import java.util.*;

import org.junit.*;
import static org.junit.Assert.*;

import picoded.dstack.module.*;
import picoded.dstack.*;

public class LockTokenManager_test extends BaseTestStack {

	// Test setup
	//-----------------------------------------------------
	
	/**
	 * Internal test object being tested
	 */
	public LockTokenManager testObj = null;

	/**
	 * [to override if needed]
	 * Does the internal stack setup
	 */
	@Before
	public void systemSetup() {
		super.systemSetup();
		testObj = new LockTokenManager( stack.keyLongMap( ramdomTableName() ) );
		testObj.systemSetup();
	}

	// Sanity Test
	//-----------------------------------------------------
	
	/**
	 * Quick test that the testObj is initialized, and not null
	 */
	@Test
	public void testObjSanityTest() {
		assertNotNull(testObj);
	}

	// Lets get testing!
	//-----------------------------------------------------

	// Timeout range to be used across test
	long lockTimeoutRange() {
		return 1000;
	}

	// issueing of lock tokens
	@Test
	public void getLockToken() {
		assertTrue( testObj.getLockToken("hello", lockTimeoutRange()) > 0l );
		assertEquals( -1, testObj.getLockToken("hello", lockTimeoutRange()) );
	}

	// issueing of two tokens
	@Test
	public void lockToken_2() {
		getLockToken();
		assertTrue( testObj.getLockToken("world", lockTimeoutRange()) > 0l );
		assertEquals( -1, testObj.getLockToken("world", lockTimeoutRange()) );
	}

	// validating of isLocked
	@Test
	public void isLocked() {
		// Validate not locked
		assertFalse( testObj.isLocked("hello") );
		// Issue it
		getLockToken();
		// Validate locked
		assertTrue( testObj.isLocked("hello") );
	}

	// Locking, unlocking, and locking again
	@Test
	public void lockAndUnlock() {
		// Lets first lock it
		long token_v1 = -1;
		token_v1 = testObj.getLockToken("hello", lockTimeoutRange());
		assertTrue( token_v1 > 0l );
		assertEquals( -1, testObj.getLockToken("hello", lockTimeoutRange()) );
		assertTrue( testObj.isLocked("hello") );

		// Then unlock it
		assertTrue( testObj.returnLockToken("hello", token_v1) );
		assertFalse( testObj.isLocked("hello") );

		// And lock it again
		assertTrue( testObj.getLockToken("hello", lockTimeoutRange()) > 0l );
		assertTrue( testObj.isLocked("hello") );
		assertEquals( -1, testObj.getLockToken("hello", lockTimeoutRange()) );
		assertTrue( testObj.isLocked("hello") );
	}

	// Lock and renew
	@Test
	public void lockAndRenew() {
		// Lets first lock it
		long token_v1 = -1;
		token_v1 = testObj.getLockToken("hello", lockTimeoutRange());
		assertTrue( token_v1 > 0l );
		assertEquals( -1, testObj.getLockToken("hello", lockTimeoutRange()) );
		assertTrue( testObj.isLocked("hello") );

		// Lock renewal
		long token_renew = -1;
		token_renew = testObj.renewLockToken("hello", token_v1, lockTimeoutRange());
		assertTrue( token_renew > 0l );
		assertEquals( -1, testObj.getLockToken("hello", lockTimeoutRange()) );
		assertTrue( testObj.isLocked("hello") );

		// Lets fail to renew the old one
		assertEquals( -1, testObj.renewLockToken("hello", token_v1, lockTimeoutRange()) );
		assertEquals( -1, testObj.getLockToken("hello", lockTimeoutRange()) );
		assertTrue( testObj.isLocked("hello") );
	}
}
