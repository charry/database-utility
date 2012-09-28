package org.charry.lib.database_utility.examples;

import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.charry.lib.database_utility.DatabaseFactory;
import org.charry.lib.database_utility.DatabaseFactory.ResultSetEx;


public class ExampleMySQLBatch {
	private static Log log = LogFactory.getLog(ExampleMySQLBatch.class);
	private static final String DB_ALIAS = "apple";

	public static void main(String s[]) {
		test1();
		test2();
	}

	private static void test1() {
		long t1 = System.currentTimeMillis();

		java.util.Date d = new java.util.Date();

		try {
			for (int i = 0; i < 100000; ++i) {
				String sql = String.format("insert into foo(V) values('%s')",
						d.getTime());
				DatabaseFactory.getInstance().executeUpdate(sql).close();
			}
		} catch (Exception e) {
			System.out.println(e);
		}

		long t2 = System.currentTimeMillis();

		System.out.println(t2 - t1);
	}

	private static void test2() {
		long t1 = System.currentTimeMillis();

		java.util.Date d = new java.util.Date();

		ArrayList<String> x = new ArrayList<String>();
		try {
			for (int i = 0; i < 100; ++i) {
				for (int j = 0; j < 1000; j++) {
					String sql = String.format(
							"insert into foo(V) values('%s')", d.getTime());

					x.add(sql);
				}

				DatabaseFactory.getInstance().executeUpdateBatch(x);
				x.clear();
			}
		} catch (Exception e) {
			System.out.println(e);
		}

		long t2 = System.currentTimeMillis();

		System.out.println(t2 - t1);
	}
}
