package org.charry.lib.database_utility.annotation;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
public @interface FieldInfo {
	public enum KType {
		STRING, NONSTRING
	}

	KType type() default KType.STRING;

	boolean ignore() default false;

	String fieldname() default "";
}
