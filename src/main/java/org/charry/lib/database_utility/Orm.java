package org.charry.lib.database_utility;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.charry.lib.database_utility.annotation.FieldInfo;
import org.charry.lib.database_utility.annotation.FieldInfo.KType;
import org.charry.lib.database_utility.annotation.TableInfo;
import org.charry.lib.database_utility.util.StackUtil;

public class Orm {
	private static Log log = LogFactory.getLog(Orm.class);
	private Class clazz = null;
	private Object object = null;
	private String tableName = "";

	/**
	 * Database to object.
	 */
	public Orm() {
		// NOOP
	}

	/**
	 * Object to database.
	 * 
	 * @param object
	 */
	public Orm(Object object) {
		this.clazz = object.getClass();
		this.object = object;
		TableInfo tableInfo = (TableInfo) clazz.getAnnotation(TableInfo.class);

		if (tableInfo != null)
			this.tableName = tableInfo.name();
	}

	public <T> List<T> dumpResultSet(ResultSet rs, Class clazz) {
		// get the field mapping
		ArrayList<FieldMap> fieldMap = getFieldMap(rs, clazz);

		List<T> elements = new ArrayList<T>();
		try {
			while (rs.next()) {
				Object newInstance = clazz.newInstance();

				for (int i = 0; i < fieldMap.size(); i++) {
					int columnPosition = fieldMap.get(i).getTableFieldColumn();
					Object value = rs.getObject(columnPosition);

					BeanUtils.copyProperty(newInstance, fieldMap.get(i).getObjectFieldName(), value);
				}

				elements.add((T) newInstance);
			}
		} catch (SQLException e) {
			StackUtil.logStackTrace(log, e);
		} catch (InstantiationException e) {
			StackUtil.logStackTrace(log, e);
		} catch (IllegalAccessException e) {
			StackUtil.logStackTrace(log, e);
		} catch (InvocationTargetException e) {
			StackUtil.logStackTrace(log, e);
		} catch (Exception e) {
			StackUtil.logStackTrace(log, e);
		}

		return elements;
	}

	private String formatFieldName(String fieldName) {
		String name = "";
		// joinTime, programRevisionName
		char x = fieldName.charAt(0);
		for (int i = 0; i < fieldName.length(); ++i) {
			char k = fieldName.charAt(i);

			if (isLowerCase(x) && isUpperCase(k))
				name = name + "_";

			name = name + k;
			x = k;
		}

		return name;
	}

	private ArrayList<FieldMap> getFieldMap(ResultSet rs, Class clazz) {
		ArrayList<FieldMap> fieldMap = new ArrayList<FieldMap>();

		Field[] fields = clazz.getDeclaredFields();

		for (int iColCnt = 0; iColCnt < fields.length; iColCnt++) {
			FieldInfo kAnnotation = fields[iColCnt].getAnnotation(FieldInfo.class);
			String originalFieldName = fields[iColCnt].getName();
			String fieldName = originalFieldName;
			if (kAnnotation != null) {
				if (kAnnotation.ignore() == true)
					continue;
			}

			if (kAnnotation != null && kAnnotation.fieldname().equals("") == false)
				fieldName = kAnnotation.fieldname();

			/**
			 * <pre>
			 * originalFieldName: field name of the object 
			 * fieldName: user-customed field name
			 * </pre>
			 */
			FieldMap item = getFieldMap(rs, originalFieldName, fieldName);

			if (item.getObjectFieldName() != null)
				fieldMap.add(item);
		}

		return fieldMap;
	}

	private FieldMap getFieldMap(ResultSet rs, String originalFieldName, String objectFieldName) {
		FieldMap item = new FieldMap();

		String tableFieldName = formatFieldName(objectFieldName);
		try {
			ResultSetMetaData metaData;
			metaData = rs.getMetaData();

			for (int i = 1; i <= metaData.getColumnCount(); ++i) {
				String fieldName = metaData.getColumnName(i);

				if (tableFieldName.compareToIgnoreCase(fieldName) == 0) {
					item.setTableFieldColumn(i);
					item.setObjectFieldName(originalFieldName);

					break;
				}
			}
		} catch (SQLException e) {
			StackUtil.logStackTrace(log, e);
		}

		return item;
	}

	/**
	 * Get SQL based on all fields.
	 * 
	 * @return SQL for insertion
	 */
	public String getInsertSQL() {
		String sql = " (";
		ArrayList<String> fieldNameList = new ArrayList<String>();
		ArrayList<String> fieldValueList = new ArrayList<String>();

		getKeyValueMap(fieldNameList, fieldValueList);

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

		return sql;
	}

	/**
	 * Get key list and corresponding value list.
	 * 
	 * @param fieldNameList
	 *            field name list
	 * @param fieldValueList
	 *            value list
	 */
	private void getKeyValueMap(ArrayList<String> fieldNameList, ArrayList<String> fieldValueList) {
		Field[] fields = clazz.getDeclaredFields();

		for (int iColCnt = 0; iColCnt < fields.length; iColCnt++) {
			FieldInfo kAnnotation = fields[iColCnt].getAnnotation(FieldInfo.class);
			String fieldName = fields[iColCnt].getName();
			if (kAnnotation != null) {
				if (kAnnotation.ignore() == true)
					continue;
			}

			try {
				Object obj = PropertyUtils.getSimpleProperty(object, fieldName);
				if (kAnnotation == null || kAnnotation.fieldname().equals("")) {
					fieldName = formatFieldName(fieldName);
					fieldName = fieldName.toUpperCase();
				}

				if (kAnnotation != null && kAnnotation.fieldname().equals("") == false)
					fieldName = kAnnotation.fieldname();

				String strValue = "";
				if (obj == null)
					strValue = "NULL";
				else {
					if (kAnnotation == null || kAnnotation.type() == KType.STRING)
						strValue = "'" + obj + "'";
					else
						strValue = "" + obj;
				}
				fieldNameList.add(fieldName);
				fieldValueList.add(strValue);
			} catch (IllegalAccessException e) {
				StackUtil.logStackTrace(log, e);
			} catch (InvocationTargetException e) {
				StackUtil.logStackTrace(log, e);
			} catch (NoSuchMethodException e) {
				StackUtil.logStackTrace(log, e);
			}
		}
	}

	public String getTableName() {
		return this.tableName;
	}

	public String getUpdateSQL() {
		String sql = " SET ";
		ArrayList<String> fieldNameList = new ArrayList<String>();
		ArrayList<String> fieldValueList = new ArrayList<String>();

		getKeyValueMap(fieldNameList, fieldValueList);

		for (int i = 0; i < fieldNameList.size(); ++i) {
			sql += fieldNameList.get(i) + "=" + fieldValueList.get(i) + ",";
		}

		// Remove tailing ,
		sql = sql.substring(0, sql.length() - 1);

		return sql;
	}

	/**
	 * Get SQL based on specified field names
	 * 
	 * @param toBeUpdatedFields
	 * @return SQL for update
	 */
	public String getUpdateSQL(String... toBeUpdatedFields) {
		// convert object name to table field name
		for (int i = 0; i < toBeUpdatedFields.length; ++i) {
			toBeUpdatedFields[i] = formatFieldName(toBeUpdatedFields[i]).toUpperCase();
		}

		String sql = " SET ";
		ArrayList<String> fieldNameList = new ArrayList<String>();
		ArrayList<String> fieldValueList = new ArrayList<String>();

		getKeyValueMap(fieldNameList, fieldValueList);

		for (int i = 0; i < fieldNameList.size(); ++i) {
			for (String f : toBeUpdatedFields) {
				if (fieldNameList.get(i).equals(f)) {
					sql += fieldNameList.get(i) + "=" + fieldValueList.get(i) + ",";

					break;
				}
			}
		}

		// Remove tailing ,
		sql = sql.substring(0, sql.length() - 1);

		return sql;
	}

	private boolean isLowerCase(char ch) {
		return ch >= 'a' && ch <= 'z';
	}

	private boolean isUpperCase(char ch) {
		return ch >= 'A' && ch <= 'Z';
	}

	public class FieldMap {
		private String objectFieldName;
		private int tableFieldColumn;

		public String getObjectFieldName() {
			return objectFieldName;
		}

		public int getTableFieldColumn() {
			return tableFieldColumn;
		}

		public void setObjectFieldName(String objectFieldName) {
			this.objectFieldName = objectFieldName;
		}

		public void setTableFieldColumn(int tableFieldColumn) {
			this.tableFieldColumn = tableFieldColumn;
		}
	}
}
