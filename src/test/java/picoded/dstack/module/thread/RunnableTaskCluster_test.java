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
	
}
