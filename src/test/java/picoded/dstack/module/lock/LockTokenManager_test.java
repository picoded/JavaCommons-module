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
	public void lockToken() {
		assertTrue( testObj.lockToken("hello", lockTimeoutRange()) > 0l );
		assertEquals( -1, testObj.lockToken("hello", lockTimeoutRange()) );
	}

	// issueing of two tokens
	@Test
	public void lockToken_2() {
		lockToken();
		assertTrue( testObj.lockToken("world", lockTimeoutRange()) > 0l );
		assertEquals( -1, testObj.lockToken("world", lockTimeoutRange()) );
	}

	// validating of isLocked
	@Test
	public void isLocked() {
		// Validate not locked
		assertFalse( testObj.isLocked("hello") );
		// Issue it
		lockToken();
		// Validate locked
		assertTrue( testObj.isLocked("hello") );
	}

	@Test
	public void lockAndUnlock() {
		// Lets first lock it
		long token_v1 = -1;
		token_v1 = testObj.lockToken("hello", lockTimeoutRange());
		assertTrue( token_v1 > 0l );
		assertEquals( -1, testObj.lockToken("hello", lockTimeoutRange()) );
		// Then unlock it
		assertTrue( testObj.unlockToken("hello", token_v1) );
		// And lock it again
		assertTrue( testObj.lockToken("hello", lockTimeoutRange()) > 0l );
		assertEquals( -1, testObj.lockToken("hello", lockTimeoutRange()) );
	}

}
