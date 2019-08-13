package picoded.dstack.module;

import org.junit.*;
import static org.junit.Assert.*;
import org.apache.commons.lang3.RandomStringUtils;

import picoded.core.struct.*;
import picoded.dstack.*;
import picoded.dstack.struct.simple.*;

public class BaseTestStack {
	
	// To override for implementation
	//-----------------------------------------------------
	
	/**
	 * [to override if needed]
	 * @return  Stack implementation being test (default is StructSimpleStack)
	 */
	public CommonStack stackImplementation() {
		return new StructSimpleStack(new GenericConvertHashMap<String, Object>());
	}
	
	/**
	 * [to override if needed]
	 * Does the internal stack setup
	 */
	@Before
	public void systemSetup() {
		stack = stackImplementation();
	}
	
	/**
	 * [to override if needed]
	 * Does the stack destruction (if not null)
	 */
	@After
	public void systemDestroy() {
		if (stack != null) {
			stack.systemDestroy();
		}
	}
	
	// Internal vars / Utility function to use
	//-----------------------------------------------------
	
	// Internal stack object (initialized by system setup)
	public CommonStack stack = null;
	
	/**
	 * @return a andom table name string (sutible as a prefix)
	 */
	public String ramdomTableName() {
		return "T" + RandomStringUtils.randomAlphanumeric(7).toUpperCase();
	}
	
	// Sanity Test
	//-----------------------------------------------------
	
	/**
	 * Quick test that the stack is initialized, and not null
	 */
	@Test
	public void stackSanityTest() {
		assertNotNull(stack);
	}
}
