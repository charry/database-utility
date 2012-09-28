package org.charry.lib.database_utility.util;

public class SleepManager {
	// unit: second
	private static int sleepInterval = 0;
	private static int current = 0;
	private static int[] intervals = { 5, 10, 20, 30, 60, 120, 300, 600, 1200,
			1800, 3600 };

	public static int getNextSleepInterval() {
		int interval = 0;

		if (sleepInterval != 0) {
			interval = sleepInterval;
		} else {
			interval = intervals[current];
			if (current < intervals.length - 1)
				current++;
		}

		// prevent user from setting a unreasonable sleep interval
		if (interval < 5)
			interval = 5;

		return interval;
	}

	public static void resetSleepTick() {
		current = 0;
	}

	public static void setSleepInterval(int interval) {
		sleepInterval = interval;
	}
}
