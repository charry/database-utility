package org.charry.lib.database_utility.examples;

import java.sql.SQLException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.charry.lib.database_utility.DatabaseFactory;
import org.charry.lib.database_utility.Orm;
import org.charry.lib.database_utility.DatabaseFactory.ResultSetEx;

public class ExampleORM {
	private static Log log = LogFactory.getLog(ExampleORM.class);
	private static final String DB_ALIAS = "apple";

	public static void main(String s[]) {
		testObject2DB();
		testDB2Object();
		testDB2Object2();
	}

	private static void testObject2DB() {
		UserInfo u = new UserInfo();
		u.setUser("Charry");
		u.setPassword("hellopass");
	}

	private static void testDB2Object() {
		log.info("-----Orm-----");
		String sql = "select * from USER_INFO";
		ResultSetEx rx = DatabaseFactory.getInstance().executeQuery(sql);

		Orm x = new Orm();

		List list = x.dumpResultSet(rx.getResultSet(), UserInfo.class);

		for (int i = 0; i < list.size(); ++i) {
			UserInfo u = (UserInfo) list.get(i);
			log.info("result: " + u.getUser() + ", " + u.getPassword());
		}
	}

	private static void testDB2Object2() {
		log.info("-----ResultSetEx toList()-----");
		String sql = "select * from USER_INFO";
		ResultSetEx rx = DatabaseFactory.getInstance().executeQuery(sql);

		List list = rx.toList(UserInfo.class);

		for (int i = 0; i < list.size(); ++i) {
			UserInfo u = (UserInfo) list.get(i);
			log.info("result: " + u.getUser() + ", " + u.getPassword());
		}
	}
}
