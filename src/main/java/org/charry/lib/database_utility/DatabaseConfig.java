package org.charry.lib.database_utility;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A class to load configuration for this factory.
 * 
 * The configuration is located at config/config_database.xml
 * 
 * @author charry
 * 
 */
public class DatabaseConfig {
	private static Log log = LogFactory.getLog(DatabaseConfig.class);

	private static String configXML = "config/config_database.xml";
	private String user;
	private String password;
	private String connectionString;
	private String driver;
	private String alias;

	/**
	 * Init the database factory.
	 * 
	 * @param alias
	 *            database alias
	 */
	public DatabaseConfig(final String alias) {
		this.alias = alias;

		loadConfig();
	}

	public DatabaseConfig(final String alias, final String user,
			final String password, final String connectionString,
			final String driver) {
		this.alias = alias;
		this.driver = driver;
		this.password = password;
		this.connectionString = connectionString;
		this.user = user;
	}

	public static void setConfigXML(String config) {
		configXML = config;
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
	private void loadConfig() {
		try {
			log.debug("load database config:" + configXML);
			log.debug("database alias:" + alias);
			Configuration config = new XMLConfiguration(configXML);
			this.user = config.getString("database." + alias + ".username");
			log.info("username:" + user);
			this.password = config.getString("database." + alias + ".password");
			this.connectionString = config.getString("database." + alias
					+ ".url");
			log.info("conn string:" + connectionString);
			this.driver = config.getString("database." + alias + ".driver");
		} catch (Exception e) {
			log.error(e);
		}
	}

	public String getAlias() {
		return alias;
	}

	public String getUser() {
		return user;
	}

	public String getPassword() {
		return password;
	}

	public String getConnectionString() {
		return connectionString;
	}

	public String getDriver() {
		return driver;
	}
}
