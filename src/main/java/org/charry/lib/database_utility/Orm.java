package org.charry.lib.database_utility;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

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

	public Orm(Class clazz, Object object) {
		this.clazz = clazz;
		this.object = object;
		TableInfo tableInfo = (TableInfo) clazz.getAnnotation(TableInfo.class);

		if (tableInfo != null)
			this.tableName = tableInfo.name();
	}

	public String getTableName() {
		return this.tableName;
	}

	public String getSQL() {
		String sql = " (";
		ArrayList<String> fieldNameList = new ArrayList<String>();
		ArrayList<String> fieldValueList = new ArrayList<String>();
		Field[] fields = clazz.getDeclaredFields();

		for (int iColCnt = 0; iColCnt < fields.length; iColCnt++) {
			FieldInfo kAnnotation = fields[iColCnt]
					.getAnnotation(FieldInfo.class);
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

				if (kAnnotation != null
						&& kAnnotation.fieldname().equals("") == false)
					fieldName = kAnnotation.fieldname();

				String strValue = "";
				if (obj == null)
					strValue = "NULL";
				else {
					if (kAnnotation == null
							|| kAnnotation.type() == KType.STRING)
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

	boolean isLowerCase(char ch) {
		return ch >= 'a' && ch <= 'z';
	}

	boolean isUpperCase(char ch) {
		return ch >= 'A' && ch <= 'Z';
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
}
