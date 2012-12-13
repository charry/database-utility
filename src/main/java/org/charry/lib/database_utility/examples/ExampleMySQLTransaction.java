package org.charry.lib.database_utility.examples;

import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.charry.lib.database_utility.DatabaseFactory;
import org.charry.lib.database_utility.DatabaseFactory.ResultSetEx;


public class ExampleMySQLTransaction {
	private static Log log = LogFactory.getLog(ExampleMySQLTransaction.class);
	private static final String DB_ALIAS = "apple";

	public static void main(String s[]) {
		testTrx();
	}

	private static void testTrx() {
		String sql = "update test set NAME='aaa' where ID=1";
		DatabaseFactory.getInstance().executeUpdate(sql).close();

		sql = "update test set NAME='bbb' where ID=2";
		DatabaseFactory.getInstance().executeUpdate(sql).close();

		sql = "update test set NAsME='ccc' where ID=3";
		DatabaseFactory.getInstance().executeUpdate(sql).close();

		sql = "update test set NAME='ddd' where ID=4";
		DatabaseFactory.getInstance().executeUpdate(sql).close();
	}
}
