package org.charry.lib.database_utility.examples;

import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.charry.lib.database_utility.DatabaseFactory;
import org.charry.lib.database_utility.DatabaseFactory.ResultSetEx;
import org.charry.lib.database_utility.util.BatchInsertSQLHelper;


public class ExampleBatchInsertSQLHelper {
	private static Log log = LogFactory
			.getLog(ExampleBatchInsertSQLHelper.class);
	private static final String DB_ALIAS = "apple";

	public static void main(String s[]) {
		test1();
	}

	private static void test1() {
		BatchInsertSQLHelper helper = new BatchInsertSQLHelper();
		helper.resetSQLCache().setTargetTable("test3");

		for (int i = 0; i < 10; i++) {
			helper.setFieldStringValue("V1", "he,llo")
					.setFieldStringValue("V2", "ss").setFieldValue("V3", 833);
		}

		log.info(helper);
		DatabaseFactory.getInstance(DB_ALIAS).executeUpdate(helper.toString());
	}
}
