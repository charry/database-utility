package org.charry.lib.database_utility.annotation;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface TableInfo {
	public String name();
}