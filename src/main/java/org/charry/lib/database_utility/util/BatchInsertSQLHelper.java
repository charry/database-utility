package org.charry.lib.database_utility.util;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This only works for MySQL
 * 
 * @author charry
 * 
 */
public final class BatchInsertSQLHelper {
	private static Log log = LogFactory.getLog(BatchInsertSQLHelper.class);
	private String targetTable = "";
	private ArrayList<String> fieldNameList = new ArrayList<String>();
	private ArrayList<String> fieldValueList = new ArrayList<String>();

	public BatchInsertSQLHelper setTargetTable(String targetTable) {
		this.targetTable = targetTable;

		return this;
	}

	public BatchInsertSQLHelper setFieldValue(String filedName, Object value) {
		this.fieldNameList.add(filedName);
		this.fieldValueList.add("" + value);

		return this;
	}

	public BatchInsertSQLHelper setFieldStringValue(String filedName,
			Object value) {
		this.fieldNameList.add(filedName);
		this.fieldValueList.add("'" + value + "'");

		return this;
	}

	public BatchInsertSQLHelper resetSQLCache() {
		this.fieldNameList.clear();
		this.fieldValueList.clear();

		return this;
	}

	public String toString() {
		String sql = "INSERT INTO " + targetTable + "(";

		String f = fieldNameList.get(0);
		boolean bReachHeaderEnd = false;
		int headerCount = 0;
		for (int i = 0; i < fieldNameList.size(); ++i) {
			if (i != 0 && fieldNameList.get(i).equals(f)) {
				bReachHeaderEnd = true;
			}

			if (bReachHeaderEnd) {
				headerCount = i;
				break;
			} else
				sql += fieldNameList.get(i) + ", ";
		}

		if (sql.endsWith(", "))
			sql = sql.substring(0, sql.length() - 2);

		sql += ") VALUES(";

		for (int i = 0; i < fieldValueList.size(); ++i) {
			if (i != 0 && (i + 1) % headerCount == 0)
				sql += fieldValueList.get(i) + "), (";
			else
				sql += fieldValueList.get(i) + ", ";
		}
		sql += ")";

		if (sql.endsWith(", ()"))
			sql = sql.substring(0, sql.length() - 4);

		return sql;
	}
}
