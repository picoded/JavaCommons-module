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

	/**
	 * Verify that configuring a maximum execution duration timeout cap on RunnableTaskManager
	 * automatically interrupts and cancels long-running worker tasks that exceed the timeout limit.
	 *
	 * Test Flow:
	 * 1. Set a 200 ms maximum execution timeout cap on the task manager.
	 * 2. Register a task that attempts to run for 2000 ms (2 seconds).
	 * 3. Execute the task and verify that executeRunnableTask returns false due to timeout cancellation,
	 *    ensuring the long-running logic was interrupted and did not complete.
	 */
	@Test
	public void testMaxTaskExecutionTimeoutCap() {
		// Step 1: Configure a low taskUpdateInterval (100 ms) so the monitoring loop evaluates frequently in test
		testObj.taskUpdateInterval = 100L;
		testObj.setMaxTaskExecutionTimeoutMs(200L);
		assertEquals(200L, testObj.getMaxTaskExecutionTimeoutMs());

		// Step 2: Register a worker task that sleeps for 2000 ms (violating the 200 ms cap)
		final AtomicInteger testCount = new AtomicInteger(0);
		testObj.registerRunnableTask("slowTask", () -> {
			try {
				Thread.sleep(2000); // Exceeds 200 ms cap
				testCount.incrementAndGet();
			} catch (InterruptedException e) {
				// InterruptedException is expected when futureObj.cancel(true) interrupts the worker thread
			}
		});

		// Step 3: Execute the worker and verify it was aborted by the execution timeout cap
		assertFalse("Task should be aborted and return false when execution time exceeds maxTaskExecutionTimeoutMs",
			testObj.executeRunnableTask("slowTask"));
		assertEquals("Counter should remain 0 because the long-running task was cancelled before completion",
			0, testCount.get());
	}
}
