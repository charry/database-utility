package org.charry.lib.database_utility;

import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.charry.lib.database_utility.DatabaseFactory.ResultSetEx;

/**
 * 
 * @author charry
 * @version 0.1.0 beta
 */
public class App {
	private static Log log = LogFactory.getLog(App.class);

	public static void main(String[] args) {
		// NOOP
		while (true) {
			ResultSetEx rx = DatabaseFactory.getInstance().executeQuery(
					"select * from catv");

			try {
				while (rx.getResultSet() != null && rx.getResultSet().next()) {
					log.info(rx.getResultSet().getString(1));
				}
			} catch (Exception e1) {
				e1.printStackTrace();
			} finally {
				rx.close();
			}
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
