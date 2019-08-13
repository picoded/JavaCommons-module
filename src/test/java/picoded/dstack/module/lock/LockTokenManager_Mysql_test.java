package picoded.dstack.module.lock;

import java.util.*;

import org.junit.*;
import static org.junit.Assert.*;

import picoded.dstack.module.*;
import picoded.dstack.jsql.*;
import picoded.core.struct.GenericConvertHashMap;
import picoded.dstack.*;
import picoded.dstack.jsql.JSqlStack;
import picoded.dstack.connector.jsql.*;

public class LockTokenManager_Mysql_test extends LockTokenManager_Sqlite_test {
    
	// To override for implementation
	//-----------------------------------------------------
	
	/**
	 * @return JSql connection to use for the test
	 */
	public JSql jsqlConnection() {
		return JSqlTestConnection.mysql();
	}
	
}
