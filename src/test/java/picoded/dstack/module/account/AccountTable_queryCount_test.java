package picoded.dstack.module.account;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import picoded.core.struct.GenericConvertHashMap;
import picoded.dstack.CommonStack;
import picoded.dstack.struct.simple.StructSimpleStack;

public class AccountTable_queryCount_test {

	public CommonStack testStack = null;
	public AccountTable accountTable = null;

	@Before
	public void systemSetup() {
		testStack = new StructSimpleStack(new GenericConvertHashMap<String, Object>());
		accountTable = new AccountTable(testStack, "test_acc_");
		accountTable.systemSetup();
	}

	@After
	public void systemDestroy() {
		if (accountTable != null) {
			accountTable.systemDestroy();
			accountTable = null;
		}
		if (testStack != null) {
			testStack.systemDestroy();
			testStack = null;
		}
	}

	@Test
	public void testQueryCount() {
		assertEquals(0, accountTable.queryCount(null, null));
		assertEquals(0, accountTable.queryCount("", null));

		AccountObject user1 = accountTable.newEntry("user1@example.com");
		assertNotNull(user1);
		user1.saveAll();

		AccountObject user2 = accountTable.newEntry("user2@example.com");
		assertNotNull(user2);
		user2.saveAll();

		assertEquals(2, accountTable.queryCount(null, null));
		assertEquals(2, accountTable.queryCount("", null));

		// Filter query on accountDataObjectMap fields (e.g. _oid)
		assertEquals(1, accountTable.queryCount("_oid = ?", new Object[]{ user1._oid() }));
		assertEquals(0, accountTable.queryCount("_oid = ?", new Object[]{ "nonexistent_oid" }));
	}
}
