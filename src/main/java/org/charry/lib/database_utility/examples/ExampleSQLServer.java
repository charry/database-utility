package org.charry.lib.database_utility.examples;

import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.charry.lib.database_utility.DatabaseFactory;
import org.charry.lib.database_utility.DatabaseFactory.ResultSetEx;


public class ExampleSQLServer {
	private static Log log = LogFactory.getLog(ExampleSQLServer.class);

	public static void main(String s[]) {
		testSQLServer();
	}

	private static void testSQLServer() {
		ResultSetEx x = DatabaseFactory.getInstance("kiwi").executeUpdate(
				"insert into foo values('eee')");

		log.info("ID" + x.getLastId());

		ResultSetEx rx = DatabaseFactory.getInstance("kiwi").executeQuery(
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
