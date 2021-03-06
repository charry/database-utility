package org.charry.lib.database_utility;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.charry.lib.database_utility.util.SleepManager;
import org.charry.lib.database_utility.util.StackUtil;

/**
 * Database utility, it's for commonly-used DML, for advanced feature, such as
 * transaction, please use getConnection() to get the database handler directly.
 * 
 * @version 0.2.3 beta
 */
public final class DatabaseFactory {
	private Connection connection = null;
	private String databaseAlias;
	private static String defaultDatabaseAlias = "default";
	private static int waitTimeout = 28800; // in seconds, = 8 hours
	private long lastActiveTime = 0;
	private static Map<String, DatabaseFactory> databaseInstanceMap = new HashMap<String, DatabaseFactory>();
	private static Log log = LogFactory.getLog(DatabaseFactory.class);

	/**
	 * Close all database connections
	 */
	public static synchronized void closeAllConnections() {
		Iterator<Map.Entry<String, DatabaseFactory>> iter = databaseInstanceMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<String, DatabaseFactory> entry = iter.next();
			// Object key = entry.getKey();
			DatabaseFactory databaseInstance = entry.getValue();

			databaseInstance.closeConnection();
		}

		databaseInstanceMap.clear();
	}

	/**
	 * By default, it'll return a database connection whose alias is 'default',
	 * with this setting, you must create a corresponding item(default) in
	 * configuration file. But you could specify the default database with:
	 * 
	 * <pre>
	 * setDefaultDatabaseAlias(...);
	 * </pre>
	 * 
	 * @return An instance of database factory
	 */
	public static synchronized DatabaseFactory getInstance() {
		return DatabaseFactory.getInstance(defaultDatabaseAlias);
	}

	/**
	 * Get the database factory based on configuration.
	 * 
	 * @param config
	 *            database configuration
	 * @return A database instance.
	 */
	public static synchronized DatabaseFactory getInstance(DatabaseConfig config) {
		Object obj = databaseInstanceMap.get(config.getAlias());
		long lNow = new java.util.Date().getTime();

		DatabaseFactory databaseInstance = null;
		if (obj == null) {
			databaseInstance = new DatabaseFactory(config);
			log.info("create db connection:" + config.getAlias());
			databaseInstanceMap.put(config.getAlias(), databaseInstance);
		} else {
			databaseInstance = (DatabaseFactory) obj;

			try {
				/**
				 * If database connection is closed or idle time > X seconds,
				 * recreate the connection
				 */
				if (databaseInstance.getConnection().isClosed()
						|| (lNow - databaseInstance.lastActiveTime > waitTimeout * 1000)) {
					databaseInstance.closeConnection();
					databaseInstance = null;

					databaseInstance = new DatabaseFactory(config);
					log.info("db connection is closed or idle for a long time(>8hrs), recreate it:" + config.getAlias());
					databaseInstanceMap.put(config.getAlias(), databaseInstance);
				}
			} catch (Exception e) {
				StackUtil.logStackTrace(log, e);
			}
		}

		databaseInstance.lastActiveTime = lNow;

		return databaseInstance;
	}

	/**
	 * Singleton, the public interface to retrieve the instance.
	 * 
	 * @param alias
	 *            database alias
	 * @return a shared database factory instance
	 */
	public static synchronized DatabaseFactory getInstance(String alias) {
		DatabaseConfig config = DatabaseConfig.getConfig(alias);

		return DatabaseFactory.getInstance(config);
	}

	/**
	 * Initialize a database factory based on configuration.
	 * 
	 * @param config
	 *            database configuration
	 */
	private DatabaseFactory(final DatabaseConfig config) {
		createConnection(config);
	}

	/**
	 * Close the database connection.
	 */
	public synchronized void closeConnection() {
		try {
			if (connection != null && connection.isClosed() == false)
				connection.close();
			connection = null;
		} catch (SQLException e) {
			StackUtil.logStackTrace(log, e);
		} catch (Exception e) {
			StackUtil.logStackTrace(log, e);
		}

		databaseInstanceMap.remove(databaseAlias);
		log.info("close connection, count of db instance:" + databaseInstanceMap.size());
	}

	/**
	 * Create a database factory based on configuration.
	 * 
	 * @param config
	 *            database configuration
	 */
	private synchronized void createConnection(final DatabaseConfig config) {
		String url = config.getConnectionString();
		String user = config.getUser();
		String pwd = config.getPassword();
		String driverName = config.getDriver();

		while (true) {
			try {
				Class.forName(driverName);
				connection = DriverManager.getConnection(url, user, pwd);
			} catch (SQLException e) {
				log.error("SQLException:" + e);
			} catch (ClassNotFoundException e) {
				log.error("ClassNotFoundException:" + e);
			} catch (Exception e) {
				log.error("failed to connect to database:" + e + url + user + pwd);
			}

			if (connection == null) {
				int sleepInterval = SleepManager.getNextSleepInterval() * 1000;
				log.error("failed to connect to db, sleep " + sleepInterval + " secs, try again later" + url + user
						+ pwd);
				try {
					Thread.sleep(sleepInterval);
				} catch (Exception e) {
					StackUtil.logStackTrace(log, e);
				}
			} else {
				SleepManager.resetSleepTick();
				break;
			}
		}

		this.databaseAlias = config.getAlias();
	}

	/**
	 * Delete record from database.
	 * 
	 * @param targetTable
	 *            target table
	 * @param condition
	 *            where condition
	 */
	public synchronized void delete(String targetTable, String condition) {
		String sql = String.format("DELETE FROM %s WHERE %s", targetTable, condition);

		ResultSetEx rx = executeUpdate(sql);
		rx.close();
	}

	/**
	 * The client code must close the returned Statement and its corresponding
	 * ResultSet, or else, it'll cause the resource leaks.
	 * 
	 * @param sql
	 *            SQL string
	 * @return prepared statement
	 */
	public synchronized PreparedStatement executePreparedQuery(final String sql) {
		PreparedStatement stmt = null;

		try {
			stmt = connection.prepareStatement(sql);
		} catch (SQLRecoverableException e) {
			log.error("SQLRecoverableException");
			StackUtil.logStackTrace(log, e);
			closeConnection();
		} catch (SQLException e) {
			log.error("SQLException:" + e);
			log.error("alias:" + this.databaseAlias + ":" + sql);
		} catch (Exception e) {
			log.error("alias:" + this.databaseAlias + ":" + sql);
			StackUtil.logStackTrace(log, e);
		}

		return stmt;
	}

	/**
	 * Query database and return an encapsulated result set.
	 * 
	 * <p />
	 * The client code must call its close() method.For convenience, all
	 * exceptions are caught, the client code doesn't need to handle it.
	 * 
	 * @param sql
	 *            query string
	 * @return result set
	 */
	public synchronized ResultSetEx executeQuery(final String sql) {
		ResultSetEx rsEx = new ResultSetEx();

		ResultSet rs = null;
		Statement stmt = null;

		try {
			stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			rs = stmt.executeQuery(sql);

			rsEx.setResultSet(rs);
			rsEx.setStatement(stmt);
		} catch (SQLRecoverableException e) {
			log.error("SQLRecoverableException");
			StackUtil.logStackTrace(log, e);
			closeConnection();
		} catch (SQLException e) {
			log.error("SQLException:" + e);
			log.error("alias:" + this.databaseAlias + ":" + sql);
			StackUtil.logStackTrace(log, e);
		} catch (Exception e) {
			log.error("alias:" + this.databaseAlias + ":" + sql);
			StackUtil.logStackTrace(log, e);
		}

		return rsEx;
	}

	/**
	 * Execute an SQL.
	 * 
	 * The client code must close the ResultSetEx explicitly. For convenience,
	 * all exceptions are caught, the client code doesn't need to handle it.
	 * 
	 * @param sql
	 *            SQL string
	 * @return result set
	 */
	public synchronized ResultSetEx executeUpdate(final String sql) {
		ResultSetEx rsEx = new ResultSetEx();

		ResultSet rs = null;
		Statement stmt = null;

		try {
			stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			stmt.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);

			rsEx.setResultSet(rs);
			rsEx.setStatement(stmt);

		} catch (SQLRecoverableException e) {
			log.error("SQLRecoverableException");
			StackUtil.logStackTrace(log, e);
			closeConnection();
		} catch (SQLException e) {
			log.error("SQLException:" + e);
			log.error("alias:" + this.databaseAlias + ":" + sql);
			StackUtil.logStackTrace(log, e);
		} catch (Exception e) {
			StackUtil.logStackTrace(log, e);
		}

		return rsEx;
	}

	/**
	 * Execute SQL in batch, all SQL will be submitted in a transaction, if the
	 * feature is supported
	 * 
	 * @param sqlList
	 *            list of the SQL string
	 * @return true if all SQL is executed successfully, or else return false
	 */
	public synchronized boolean executeUpdateBatch(final ArrayList<String> sqlList) {
		boolean bSuccess = true;
		boolean bAutocommit = true;

		Statement stmt = null;

		try {
			// disable auto commit
			bAutocommit = this.connection.getAutoCommit();
			this.connection.setAutoCommit(false);

			stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

			for (int i = 0; i < sqlList.size(); i++) {
				stmt.addBatch(sqlList.get(i));
			}

			stmt.executeBatch();
			connection.commit();
		} catch (SQLRecoverableException e) {
			log.error("SQLRecoverableException");
			StackUtil.logStackTrace(log, e);

			// rollback
			try {
				connection.rollback();
			} catch (Exception ex) {
				StackUtil.logStackTrace(log, e);
			}

			bSuccess = false;

			closeConnection();
		} catch (SQLException e) {
			log.error("SQLException:" + e);
			log.error("alias:" + this.databaseAlias + ":" + sqlList);

			// rollback
			try {
				connection.rollback();
			} catch (Exception ex) {
				StackUtil.logStackTrace(log, e);
			}

			bSuccess = false;
		} catch (Exception e) {
			log.error("alias:" + this.databaseAlias + ":" + sqlList);
			StackUtil.logStackTrace(log, e);

			// rollback
			try {
				connection.rollback();
			} catch (Exception ex) {
				StackUtil.logStackTrace(log, e);
			}

			bSuccess = false;
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (Exception e) {
				StackUtil.logStackTrace(log, e);
			}

			// restore the auto-commit settings
			try {
				this.connection.setAutoCommit(bAutocommit);
			} catch (Exception e) {
				StackUtil.logStackTrace(log, e);
			}
		}

		return bSuccess;
	}

	public synchronized boolean foundRecord(final String sql) {
		ResultSetEx rx = executeQuery(sql);
		boolean bFound = false;
		try {
			bFound = rx.getResultSet().first();
		} catch (SQLException e) {
			StackUtil.logStackTrace(log, e);
		} catch (Exception e) {
			StackUtil.logStackTrace(log, e);
		} finally {
			rx.close();
		}

		return bFound;
	}

	/**
	 * Return the database connection.
	 * 
	 * DO NOT use this interface to close the connection, use closeConnection()
	 * instead
	 * 
	 * @return database connection
	 */
	public synchronized Connection getConnection() {
		return this.connection;
	}

	/**
	 * A helper function to retrieve the field value, it only could get the
	 * value of the first row.
	 * 
	 * @param sql
	 * @param fieldName
	 * @return Integer.MIN_VALUE if no record found
	 */
	public synchronized int getIntValue(final String sql, final String fieldName) {
		ResultSetEx rx = executeQuery(sql);
		int value = Integer.MIN_VALUE;
		try {
			if (rx.getResultSet().next()) {
				value = rx.getResultSet().getInt(fieldName);
			}
		} catch (SQLRecoverableException e) {
			log.error("SQLRecoverableException");
			StackUtil.logStackTrace(log, e);
			closeConnection();
		} catch (SQLException e) {
			StackUtil.logStackTrace(log, e);
		} catch (Exception e) {
			StackUtil.logStackTrace(log, e);
		} finally {
			rx.close();
		}

		return value;
	}

	/**
	 * A helper function to retrieve the field value, it only could get the
	 * value of the first row.
	 * 
	 * @param sql
	 * @param fieldName
	 * @return null if no record found
	 */
	public synchronized String getStringValue(final String sql, final String fieldName) {
		ResultSetEx rx = executeQuery(sql);
		String value = null;
		try {
			if (rx.getResultSet().next()) {
				value = rx.getResultSet().getString(fieldName);
			}
		} catch (SQLRecoverableException e) {
			log.error("SQLRecoverableException");
			StackUtil.logStackTrace(log, e);
			closeConnection();
		} catch (SQLException e) {
			StackUtil.logStackTrace(log, e);
		} catch (Exception e) {
			StackUtil.logStackTrace(log, e);
		} finally {
			rx.close();
		}

		return value;
	}

	public ResultSetEx insertOrUpdate(String sqlInsertOrUpdate) {
		String sql[] = sqlInsertOrUpdate.split("\\^\\^\\^\\^");
		if (getIntValue(sql[0], "R") == 0) {
			return this.executeUpdate(sql[1]);
		} else {
			return this.executeUpdate(sql[2]);
		}
	}

	public synchronized boolean notFoundRecord(final String sql) {
		return !(foundRecord(sql));
	}

	/**
	 * Save the database connection as a new alias, the corresponding database
	 * connection won't be created until the client code calls it.
	 * 
	 * If the new alias exits, the connection will be overwritten/replaced.
	 * 
	 * @param alias
	 *            new database alias
	 */
	public synchronized void saveAliasAs(String alias) {
		// if alias already exists, do nothing.
		if (this.databaseAlias.equals(alias)) {
			log.info("alias exists, noop");
			return;
		}

		DatabaseConfig config = DatabaseConfig.getConfig(databaseAlias);

		// the object will be cached in constructor automatically
		// if the new alias exits, it'll be overwritten/replaced.
		new DatabaseConfig(alias, config.getUser(), config.getPassword(), config.getConnectionString(),
				config.getDriver());
	}

	public final static class FactoryFacade {
		public static void setConfigXML(String config) {
			DatabaseConfig.setConfigXML(config);
		}

		/**
		 * Set the default database alias, the default value is: default
		 * 
		 * @param alias
		 *            database alias name
		 */
		public static synchronized void setDefaultDatabaseAlias(String alias) {
			DatabaseFactory.defaultDatabaseAlias = alias;
		}

		/**
		 * Sleep n seconds to retry if failed to connect to database.
		 * 
		 * @param interval
		 *            sleep interval, unit: second, 0 means increased interval
		 */
		public static synchronized void setRetryInterval(int interval) {
			SleepManager.setSleepInterval(interval);
		}

		/**
		 * Set timeout, when the connection is idle for a long time,
		 * reconnection is required. the timeout value is the minimum one of all
		 * database which are configured in config_database.xml
		 * 
		 * @param timeout
		 *            max timeout
		 */
		public static synchronized void setWaitTimeout(int timeout) {
			DatabaseFactory.waitTimeout = timeout;
		}
	}

	/**
	 * A wrapper for MySQL ResultSet. Ex means Extended. Not thread-safe.
	 * 
	 * @author wcharry
	 * 
	 */
	public final class ResultSetEx {
		private ResultSet resultSet = null;
		private Statement statement = null;
		private long lastId = -1;

		/**
		 * Close the resources, this function must be called by the client code.
		 */
		public void close() {
			try {
				if (resultSet != null)
					resultSet.close();

				if (statement != null)
					statement.close();
			} catch (Exception e) {
				StackUtil.logStackTrace(log, e);
			}
		}

		/**
		 * Only works with MySQL.
		 * 
		 * only get one last Id, for more, use statement instead
		 * 
		 * @return last insert id
		 */
		public long getLastId() {
			// this only works for one-insert-statement, if there's multiple
			// inserts, use stmt.getGeneratedKeys instead.
			ResultSet rsKeys;
			try {
				rsKeys = statement.getGeneratedKeys();

				if (rsKeys.next()) {
					lastId = rsKeys.getLong(1);
				}

				if (rsKeys != null)
					rsKeys.close();
			} catch (SQLRecoverableException e) {
				log.error("SQLRecoverableException");
				StackUtil.logStackTrace(log, e);
				closeConnection();
			} catch (SQLException e) {
				StackUtil.logStackTrace(log, e);
			}

			return this.lastId;
		}

		public ResultSet getResultSet() {
			return resultSet;
		}

		public Statement getStatement() {
			return statement;
		}

		public void setLastId(long id) {
			this.lastId = id;
		}

		public void setResultSet(ResultSet resultSet) {
			this.resultSet = resultSet;
		}

		public void setStatement(Statement statement) {
			this.statement = statement;
		}

		public <T> List<T> toList(Class<T> clazz) {
			Orm orm = new Orm();

			List<T> list = orm.dumpResultSet(resultSet, clazz);

			this.close();

			return list;
		}
	}
}
