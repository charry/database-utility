package org.charry.lib.database_utility.examples;

import org.charry.lib.database_utility.annotation.FieldInfo;
import org.charry.lib.database_utility.annotation.TableInfo;
import org.charry.lib.database_utility.annotation.FieldInfo.KType;

@TableInfo(name = "USER_INFO")
public class UserInfo {
	@FieldInfo(type = KType.NONSTRING, ignore = true)
	private int id;

	private String user;

	@FieldInfo(fieldname = "PASSWD", type = KType.STRING)
	private String password;

	// @KAnnotation(hide = true)
	@FieldInfo(type = KType.STRING)
	private String joinTime;

	public UserInfo() {
	}

	public String getJoinTime() {
		return joinTime;
	}

	public void setJoinTime(String joinTime) {
		this.joinTime = joinTime;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}
}