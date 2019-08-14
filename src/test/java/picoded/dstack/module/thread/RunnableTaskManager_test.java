package picoded.dstack.module.thread;

// Java imports
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

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
		// Counter to increment
		final AtomicInteger helloCounter = new AtomicInteger(0);
		// Task to run
		testObj.registerRunnableTask("hello", () -> {
			helloCounter.incrementAndGet();
		});

		// Test it
		assertTrue(testObj.executeRunnableTask("hello"));
		assertEquals(1, helloCounter.get());
		assertTrue(testObj.executeRunnableTask("hello"));
		assertEquals(2, helloCounter.get());
	}
}
