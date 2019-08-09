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
}
