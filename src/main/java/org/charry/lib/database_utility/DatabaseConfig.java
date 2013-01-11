package org.charry.lib.database_utility;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A class to load configuration for this factory.
 * 
 * The configuration is located at config/config_database.xml
 * 
 * @author wcharry
 * 
 */
public class DatabaseConfig {
	private static Log log = LogFactory.getLog(DatabaseConfig.class);
	/**
	 * To be more convenient, the database connection config will NOT be
	 * removed, even client close the connection
	 */
	private static Map<String, DatabaseConfig> databaseConfigMap = new HashMap<String, DatabaseConfig>();
	private static String configXML = "config/config_database.xml";
	private String user;
	private String password;
	private String connectionString;
	private String driver;
	private final String alias;

	public static synchronized DatabaseConfig getConfig(String alias) {
		Object obj = databaseConfigMap.get(alias);
		DatabaseConfig config = null;
		if (obj != null)
			config = (DatabaseConfig) obj;
		else
			config = new DatabaseConfig(alias);

		return config;
	}

	public static synchronized void removeConfigCache(String alias) {
		databaseConfigMap.remove(alias);
	}

	public static synchronized void resetConfigCache() {
		databaseConfigMap.clear();
	}

	public static synchronized void setConfigXML(String config) {
		configXML = config;
	}

	/**
	 * Init the database factory.
	 * 
	 * @param alias
	 *            database alias
	 */
	public DatabaseConfig(final String alias) {
		this.alias = alias;

		loadConfig();

		databaseConfigMap.put(alias, this); // cache db config
	}

	public DatabaseConfig(final String alias, final String user, final String password, final String connectionString,
			final String driver) {
		this.alias = alias;
		this.driver = driver;
		this.password = password;
		this.connectionString = connectionString;
		this.user = user;

		databaseConfigMap.put(alias, this); // cache db config
	}

	public synchronized String getAlias() {
		return alias;
	}

	public synchronized String getConnectionString() {
		return connectionString;
	}

	public synchronized String getDriver() {
		return driver;
	}

	public synchronized String getPassword() {
		return password;
	}

	public synchronized String getUser() {
		return user;
	}

	/**
	 * Configuration file sample:
	 * 
	 * <pre>
	 * &lt;config&gt;
	 *         &lt;database&gt;
	 *                 &lt;apple&gt;
	 *                         &lt;username&gt;username&lt;/username&gt;
	 *                         &lt;password&gt;passwd&lt;/password&gt;
	 *                         &lt;url&gt;jdbc:mysql://faceoff/por?autoReconnect=true&lt;/url&gt;
	 *                         &lt;driver&gt;com.mysql.jdbc.Driver&lt;/driver&gt;
	 *                 &lt;/apple&gt;
	 * 
	 *                 &lt;banana&gt;
	 *                         &lt;username&gt;VWAPI&lt;/username&gt;
	 *                         &lt;password&gt;passwd&lt;/password&gt;
	 *                         &lt;url&gt;jdbc:oracle:thin:@SSUZMEuuuuuSBDRVS01:1573:INST935&lt;/url&gt;
	 *                         &lt;driver&gt;oracle.jdbc.driver.OracleDriver&lt;/driver&gt;
	 *                 &lt;/banana&gt;
	 * 
	 *         &lt;/database&gt;
	 * &lt;/config&gt;
	 * </pre>
	 */
	private synchronized void loadConfig() {
		try {
			log.debug("load database config:" + configXML);
			log.debug("database alias:" + alias);
			Configuration config = new XMLConfiguration(configXML);
			this.user = config.getString("database." + alias + ".username");
			log.info("username:" + user);
			this.password = config.getString("database." + alias + ".password");
			this.connectionString = config.getString("database." + alias + ".url");
			log.info("conn string:" + connectionString);
			this.driver = config.getString("database." + alias + ".driver");
		} catch (Exception e) {
			log.error(e);
		}
	}
}
