package picoded.dstack.module.thread;

// Java imports
import java.util.*;
import java.util.concurrent.Future;
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
		final AtomicInteger testCount = new AtomicInteger(0);
		// Task to run
		testObj.registerRunnableTask("hello", () -> {
			testCount.incrementAndGet();
		});
		
		// Test it
		assertTrue(testObj.executeRunnableTask("hello"));
		assertEquals(1, testCount.get());
		assertTrue(testObj.executeRunnableTask("hello"));
		assertEquals(2, testCount.get());
	}
	
	@Test
	public void multipleTask() {
		// Counter to increment
		final AtomicInteger testCount = new AtomicInteger(0);
		// Tasks to run
		testObj.registerRunnableTask("one", () -> {
			testCount.incrementAndGet();
		});
		testObj.registerRunnableTask("two", () -> {
			testCount.incrementAndGet();
		});
		
		// Run and assert
		testObj.tryAllRunnableTask();
		assertEquals(2, testCount.get());
	}
	
	@Test
	public void checkForTaskRejectOnOverlap() throws Exception {
		// Counter to increment
		final AtomicInteger testCount = new AtomicInteger(0);
		// Task to run
		testObj.registerRunnableTask("hello", () -> {
			try {
				Thread.sleep(2000); // 2 second sleep
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		testCount.incrementAndGet();
	}  );
		
		// Lets trigger 1 asyncronously
		Future<Boolean> asyncTask = testObj.executeRunnableTask_async("hello");
		// Lets do a small sleep (ensurre asynTask actually had time to start)
		Thread.yield();
		Thread.sleep(10); // 2 second sleep
		
		// Lets assert the same task will fail to strart (overlapping)
		assertFalse(testObj.executeRunnableTask("hello"));
		// Lets wait for the async task to finish
		asyncTask.get();
		// Lets get the final result count 
		assertEquals(1, testCount.get());
	}
}
