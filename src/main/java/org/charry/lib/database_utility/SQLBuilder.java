package org.charry.lib.database_utility;

import java.util.ArrayList;

public class SQLBuilder {
	private String targetTable = "";
	private final ArrayList<String> fieldNameList = new ArrayList<String>();
	private final ArrayList<String> fieldValueList = new ArrayList<String>();
	private String whereCondition = "1=2";
	private String sql = "";
	private SqlType sqlType;

	private SQLBuilder generateInsertSql() {
		String sql = "INSERT INTO " + targetTable + "(";

		for (int i = 0; i < fieldNameList.size(); ++i) {
			if (i < fieldNameList.size() - 1)
				sql += fieldNameList.get(i) + ", ";
			else
				sql += fieldNameList.get(i);
		}
		sql += ") VALUES(";

		for (int i = 0; i < fieldValueList.size(); ++i) {
			if (i < fieldValueList.size() - 1)
				sql += fieldValueList.get(i) + ", ";
			else
				sql += fieldValueList.get(i);
		}
		sql += ")";

		this.sql = sql;

		return this;
	}

	private SQLBuilder generateUpdateSql() {
		String sql = "UPDATE " + targetTable + " SET ";

		for (int i = 0; i < fieldNameList.size(); ++i) {
			sql += fieldNameList.get(i) + "=" + fieldValueList.get(i);

			if (i < fieldNameList.size() - 1)
				sql += ", ";
		}
		sql += " WHERE " + this.whereCondition;

		this.sql = sql;

		return this;
	}

	/**
	 * Insert a record into table.
	 * 
	 * @return A wrapper of ResultSet
	 */
	public SQLBuilder insert() {
		this.sqlType = SqlType.INSERT;

		return this;
	}

	/**
	 * Insert if record doesn't exist, else update
	 * 
	 * @param keys
	 *            keys to determine if record(s) exist
	 * @return SQL string for insert or update
	 */
	public SQLBuilder insertOrUpdate(String... keys) {
		String condition = "";
		for (String key : keys) {
			for (int i = 0; i < fieldNameList.size(); ++i) {
				String fname = fieldNameList.get(i);

				if (key.equals(fname)) {
					condition += key + "=" + fieldValueList.get(i);
					condition += " AND ";
				}
			}
		}

		if (condition.contains("AND"))
			condition = condition.substring(0, condition.length() - 4);

		String sqlQuery = String.format("SELECT COUNT(*) AS R FROM %s WHERE %s", this.targetTable, condition);

		String sqlCombo = sqlQuery;

		this.insert().generateInsertSql();
		sqlCombo += "^^^^" + this.sql;

		this.whereCondition = condition;
		this.update().generateUpdateSql();
		sqlCombo += "^^^^" + this.sql;

		this.sql = sqlCombo;
		this.sqlType = SqlType.NONE;

		return this;
	}

	/**
	 * Set value of one field.
	 * 
	 * @param filedName
	 *            field name
	 * @param value
	 *            field value
	 * @return the DatabaseFactory instance, this could be used as method
	 *         chaining.
	 */
	public synchronized SQLBuilder numeric(String filedName, Object value) {
		this.fieldNameList.add(filedName);
		this.fieldValueList.add("" + value);

		return this;
	}

	/**
	 * Reset the cached field names and field values, etc.
	 * 
	 * @return the DatabaseFactory instance, this could be used as method
	 *         chaining.
	 */
	public synchronized SQLBuilder reset() {
		this.fieldNameList.clear();
		this.fieldValueList.clear();

		this.whereCondition = "1=2";

		return this;
	}

	/**
	 * Save a object
	 * 
	 * @param object
	 *            object
	 * @return ResultSetEx a ResultSetEx object
	 */
	public SQLBuilder saveObject(Object object) {
		Orm orm = new Orm(object);
		String tableName = "";
		if (this.targetTable.equals("") == false)
			tableName = this.targetTable;
		else
			tableName = orm.getTableName();

		String sql = "INSERT INTO " + tableName + orm.getInsertSQL();

		this.sql = sql;
		this.sqlType = SqlType.NONE;

		return this;
	}

	public String sql() {
		switch (sqlType) {
		case INSERT:
			this.generateInsertSql();
			break;

		case UPDATE:
			this.generateUpdateSql();
			break;

		default:
			break;
		}

		return this.sql;
	}

	/**
	 * Set string value of one field
	 * 
	 * @param filedName
	 *            field name
	 * @param value
	 *            field value
	 * @return the DatabaseFactory instance, this could be used as method
	 *         chaining.
	 */
	public synchronized SQLBuilder string(String filedName, Object value) {
		this.fieldNameList.add(filedName);
		this.fieldValueList.add("'" + value + "'");

		return this;
	}

	/**
	 * Set the target table for future operation.
	 * 
	 * @param targetTable
	 *            target table
	 * @return the DatabaseFactory instance, this could be used as method
	 *         chaining.
	 */
	public synchronized SQLBuilder table(String targetTable) {
		this.targetTable = targetTable;

		return this;
	}

	@Override
	public String toString() {
		this.sql();
		return this.sql;
	}

	/**
	 * Update record in table.
	 * 
	 * @return A wrapper of ResultSet
	 */
	public SQLBuilder update() {
		this.sqlType = SqlType.UPDATE;

		return this;
	}

	/**
	 * @param object
	 *            the object which is to be updated
	 * @param where
	 *            the where condition on which the object is updated based
	 */
	public SQLBuilder updateObject(Object object, String where) {
		Orm orm = new Orm(object);

		String tableName = "";
		if (this.targetTable.equals("") == false)
			tableName = this.targetTable;
		else
			tableName = orm.getTableName();

		String sql = "UPDATE " + tableName + orm.getUpdateSQL();
		sql += " WHERE " + where;

		this.sql = sql;
		this.sqlType = SqlType.NONE;

		return this;
	}

	/**
	 * @param object
	 *            Object to be updated
	 * @param where
	 *            where statement to identify a row of record
	 * @param fields
	 *            fields to be updated
	 */
	public SQLBuilder updateObject(Object object, String where, String... fields) {
		Orm orm = new Orm(object);

		String tableName = "";
		if (this.targetTable.equals("") == false)
			tableName = this.targetTable;
		else
			tableName = orm.getTableName();

		String sql = "UPDATE " + tableName + orm.getUpdateSQL(fields);
		sql += " WHERE " + where;

		this.sql = sql;
		this.sqlType = SqlType.NONE;

		return this;
	}

	/**
	 * Set the where condition.
	 * 
	 * @param where
	 *            where condition
	 * @return the DatabaseFactory instance, this could be used as method
	 *         chaining.
	 */
	public synchronized SQLBuilder where(String where) {
		this.whereCondition = where;

		return this;
	}

	private enum SqlType {
		NONE, INSERT, UPDATE
	}
}
