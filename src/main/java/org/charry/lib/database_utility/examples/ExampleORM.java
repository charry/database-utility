package org.charry.lib.database_utility.examples;

import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.charry.lib.database_utility.DatabaseFactory;
import org.charry.lib.database_utility.DatabaseFactory.ResultSetEx;


public class ExampleORM {
	private static Log log = LogFactory.getLog(ExampleORM.class);
	private static final String DB_ALIAS = "apple";

	public static void main(String s[]) {
		testOrm();
	}

	private static void testOrm() {
		UserInfo u = new UserInfo();
		u.setUser("Charry");
		u.setPassword("hellopass");
		DatabaseFactory.getInstance().saveObject(UserInfo.class, u).close();

		log.info(DatabaseFactory.getInstance().getLastSQL());
	}
}
