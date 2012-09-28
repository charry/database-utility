package org.charry.lib.database_utility.examples;

import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.charry.lib.database_utility.DatabaseFactory;
import org.charry.lib.database_utility.DatabaseFactory.ResultSetEx;


public class ExampleOracle {
	private static Log log = LogFactory.getLog(ExampleOracle.class);
	private static final String DB_ALIAS = "olive";

	public static void main(String s[]) {
		testOracle();
	}

	private static void testOracle() {
		// test1
		DatabaseFactory.getInstance(DB_ALIAS).resetSQLCache()
				.setTargetTable("foo").setFieldStringValue("v", "hello")
				.insertRecord().close();

		// test2
		ResultSetEx x = DatabaseFactory.getInstance(DB_ALIAS).executeQuery(
				"insert into foo values('eee')");

		// test3
		ResultSetEx rx = DatabaseFactory.getInstance(DB_ALIAS).executeQuery(
				"select * from foo");
		try {
			while (rx.getResultSet().next()) {
				log.info(rx.getResultSet().getString("V"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
