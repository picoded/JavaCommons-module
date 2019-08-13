package picoded.dstack.module.thread;

// Java imports
import java.util.*;

// Lib imports
import org.junit.*;
import static org.junit.Assert.*;

// JC imports
import picoded.dstack.module.*;
import picoded.dstack.*;

public class RunnableTaskManager_test extends BaseTestStack {
	
	// Test setup
	//-----------------------------------------------------
	
	/**
	 * Internal test object being tested
	 */
	public RunnableTaskManager testObj = null;
	
	/**
	 * [to override if needed]
	 * Does the internal stack setup
	 */
	@Before
	public void systemSetup() {
		super.systemSetup();
		testObj = new RunnableTaskManager(stack.keyLongMap(ramdomTableName()));
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
	
	@Test
	public void simpleTask() {
		testObj.registerRunnableTask("hello", () -> {
			return;
		});
	}
}
