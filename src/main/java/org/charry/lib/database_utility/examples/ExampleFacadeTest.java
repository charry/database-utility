package org.charry.lib.database_utility.examples;

import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.charry.lib.database_utility.DatabaseConfig;
import org.charry.lib.database_utility.DatabaseFactory;
import org.charry.lib.database_utility.DatabaseFactory.ResultSetEx;


public class ExampleFacadeTest {
	private static Log log = LogFactory.getLog(ExampleFacadeTest.class);
	private static final String DB_ALIAS = "apple";

	public static void main(String s[]) {
		DatabaseFactory.FactoryFacade.setDefaultDatabaseAlias("olsive");
		DatabaseFactory.FactoryFacade.setWaitTimeout(111);
		DatabaseFactory.getInstance();
	}
}
