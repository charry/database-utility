package org.charry.lib.database_utility.examples;

import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.charry.lib.database_utility.DatabaseFactory;
import org.charry.lib.database_utility.DatabaseFactory.ResultSetEx;


public class ExampleMySQLBasic {
	private static Log log = LogFactory.getLog(ExampleMySQLBasic.class);
	private static final String DB_ALIAS = "apple";

	public static void main(String s[]) {
		
		testTrx();
	}

	private static void testTrx() {
		String sql = "select * from catv";
		ResultSetEx rx = DatabaseFactory.getInstance().executeQuery(sql);

		try {
			while (rx.getResultSet().next()) {
				System.out.println(rx.getResultSet().getString("ID"));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		rx.close();

		// only valid in transaction mode
		log.info("has error:" + DatabaseFactory.getInstance().hasError());
	}
}
