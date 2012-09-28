package org.charry.lib.database_utility.util;

import org.apache.commons.logging.Log;

public class StackUtil {
	/**
	 * A function to dump the stack frame.
	 * 
	 * @param log
	 *            log4j instance
	 * @param e
	 *            exception instance
	 */
	public static void logStackTrace(final Log log, final Exception e) {
		log.error(e);

		StackTraceElement[] s = e.getStackTrace();
		for (int i = 0; i < s.length; i++) {
			log.error(s[i]);
		}
	}
}
