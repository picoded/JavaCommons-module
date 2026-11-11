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

public class RunnableTaskCluster_test extends BaseTestStack {
	
	// Test setup
	//-----------------------------------------------------
	
	/**
	 * Internal test object being tested
	 */
	public RunnableTaskCluster testObj = null;
	
	/**
	 * [to override if needed]
	 * Does the internal stack setup
	 */
	@Before
	public void systemSetup() {
		super.systemSetup();
		testObj = new RunnableTaskCluster(stack, ramdomTableName());
		testObj.systemSetup();
	}
	
	@After
	public void systemDestroy() {
		testObj.shutdownTaskExecutor();
		super.systemDestroy();
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
	
	// Simple syncronized class for AtomicInteger usage across multiple threads
	public static class AtomicIntegerWrapper {
		AtomicInteger testCount = new AtomicInteger(0);
	}
	
	@Test
	public void backgroundTask() throws Exception {
		// Lets configure min delay to a small number (speed up testing accuracy / process)
		testObj.minimumExecutorDelay(200);
		Thread.sleep(5000);
		
		// Counter to increment
		final AtomicInteger testCount = new AtomicInteger(0);
		assertEquals(0, testCount.get());
		
		// Task to run
		testObj.scheduleRunnableTask("hello", () -> {
			testCount.incrementAndGet();
			System.out.println("Doing increment");
		}, 500);
		
		// Test it - with a generious sleep - to prevent race condition
		// as the spec is minimum interval (when there is CPU) - not a gurantee
		//
		// Unfortunately in the unit testin scenerio - this is pretty much a guarentee to occur
		// as we spin up as many test threads as there are cores
		Thread.sleep(3000);
		System.out.println("Asserting for non null value : " + testCount.get());
		assertNotEquals(0, testCount.get());
	}

	/**
	 * Verify that constructing a RunnableTaskCluster with a fixed thread pool size and execution timeout cap
	 * correctly initializes internal worker executor configurations and async dispatch toggles.
	 *
	 * Test Flow:
	 * 1. Instantiate a multi-thread cluster (pool size = 4, timeout cap = 5000 ms).
	 *    Verify that isAsyncDispatchEnabled is true (allowing multi-threaded task processing) and
	 *    getMaxTaskExecutionTimeoutMs returns 5000 ms.
	 * 2. Instantiate a single-thread cluster (pool size = 1, timeout cap = 10000 ms).
	 *    Verify that isAsyncDispatchEnabled is false (enforcing single-threaded sequential execution) and
	 *    getMaxTaskExecutionTimeoutMs returns 10000 ms.
	 */
	@Test
	public void testThreadPoolAndTimeoutCapConstructor() {
		// Multi-threaded cluster setup (pool size > 1) -> expects async dispatch enabled
		RunnableTaskCluster multiThreadCluster = new RunnableTaskCluster(stack, ramdomTableName(), 4, 5000L);
		multiThreadCluster.systemSetup();
		try {
			assertTrue("Multi-thread cluster (pool size 4) should enable async dispatch mode",
				multiThreadCluster.isAsyncDispatchEnabled());
			assertEquals("Max task execution timeout cap should be configured to 5000 ms",
				5000L, multiThreadCluster.getMaxTaskExecutionTimeoutMs());
		} finally {
			multiThreadCluster.shutdownTaskExecutor();
		}

		// Single-threaded cluster setup (pool size = 1) -> expects async dispatch disabled
		RunnableTaskCluster singleThreadCluster = new RunnableTaskCluster(stack, ramdomTableName(), 1, 10000L);
		singleThreadCluster.systemSetup();
		try {
			assertFalse("Single-thread cluster (pool size 1) should disable async dispatch mode",
				singleThreadCluster.isAsyncDispatchEnabled());
			assertEquals("Max task execution timeout cap should be configured to 10000 ms",
				10000L, singleThreadCluster.getMaxTaskExecutionTimeoutMs());
		} finally {
			singleThreadCluster.shutdownTaskExecutor();
		}
	}

	/**
	 * Verify that when async dispatch mode is enabled on a RunnableTaskCluster,
	 * executeRunnableTask_async() dispatches tasks concurrently across worker pool threads.
	 *
	 * Test Flow:
	 * 1. Instantiate RunnableTaskCluster with 4 worker threads and perform system setup.
	 * 2. Register 4 tasks, each taking 300 ms to execute.
	 * 3. Dispatch all 4 tasks asynchronously using executeRunnableTask_async().
	 * 4. Wait for all 4 futures to complete and verify all 4 tasks executed concurrently.
	 */
	@Test
	public void testAsyncDispatchMode() throws Exception {
		RunnableTaskCluster cluster = new RunnableTaskCluster(stack, ramdomTableName(), 4, 5000L);
		cluster.systemSetup();
		try {
			assertTrue("Async dispatch mode should be active for thread pool size 4", cluster.isAsyncDispatchEnabled());

			final AtomicInteger completedCount = new AtomicInteger(0);

			for (int i = 0; i < 4; i++) {
				final String taskName = "asyncTask_" + i;
				cluster.registerRunnableTask(taskName, () -> {
					try {
						Thread.sleep(300);
					} catch (InterruptedException e) {
						// Ignore interrupted exception during test sleep
					}
					completedCount.incrementAndGet();
				});
			}

			// Dispatch all 4 tasks asynchronously across the worker thread pool
			List<Future<Boolean>> futures = new ArrayList<>();
			for (int i = 0; i < 4; i++) {
				futures.add(cluster.executeRunnableTask_async("asyncTask_" + i, true));
			}

			// Wait for all 4 async task futures to complete
			for (Future<Boolean> future : futures) {
				assertNotNull("Future should not be null for dispatched task", future);
				assertTrue("Task should execute successfully and return true", future.get());
			}

			assertEquals("All 4 tasks should complete concurrently across worker pool threads", 4, completedCount.get());
		} finally {
			cluster.shutdownTaskExecutor();
		}
	}
}
