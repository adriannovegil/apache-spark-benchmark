/**
 * This file is part of Apache Spark Benchmark.
 *
 * Apache Spark Benchmark is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2, or (at your option) any later
 * version.
 *
 * Apache Spark Benchmark is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; see the file COPYING. If not, see
 * <http://www.gnu.org/licenses/>.
 */
package es.devcircus.apache.spark.benchmark.util;

import java.io.*;
import java.text.*;
import java.util.*;

/**
 *
 * @author Adrian Novegil <adrian.novegil@gmail.com>
 */
public abstract class FormatHelper {

    /**
     * New Line String
     */
    public static String NEW_LINE = System.getProperty("line.separator");

    /**
     * Int Number Formatter
     */
    private static final DecimalFormat INT_FORMAT
            = new DecimalFormat("###,###,###");

    /**
     * Real Number Formatter - for numbers >= 1
     */
    private static final DecimalFormat REAL_FORMAT_1
            = new DecimalFormat("0.0");

    /**
     * Real Number Formatter - for numbers < 1
     */
    private static final DecimalFormat REAL_FORMAT_2
            = new DecimalFormat("0.00");

    /**
     * Real Number Formatter - for numbers < 0.1
     */
    private static final DecimalFormat REAL_FORMAT_3
            = new DecimalFormat("0.000");

    /**
     * Real Number Formatter - for numbers < 0.1
     */
    private static final DecimalFormat REAL_FORMAT_4
            = new DecimalFormat("0.0000");

    /**
     * Date format for result lines
     */
    private static final SimpleDateFormat TIME_FORMAT
            = new SimpleDateFormat("yyyy-MM-dd kk:mm");

    /**
     * Gets unqualified class name (excluding package / wrapper class name).
     *
     * @param cls a Class to get its unqualified class name
     * @return the unqualified class name.
     */
    public static String getShortClassName(Class cls) {
        return getShortClassName(cls.getName());
    }

    /**
     * Gets unqualified class name (excluding package / wrapper class name).
     *
     * @param className a fully qualified class name
     * @return the unqualified class name.
     */
    public static String getShortClassName(String className) {
        int ix = Math.max(
                className.lastIndexOf('.'), className.lastIndexOf('$'));
        return (ix < 0) ? className : className.substring(ix + 1);
    }

    /**
     * Formats a specified numeric (int or real) value.
     *
     * @param value a value to be formatted
     * @return the formatted string.
     */
    public static String formatNumber(double value) {
        if (value >= 100) {
            return formatInt(value);
        } else if (value >= 0.995) {
            return REAL_FORMAT_1.format(value);
        } else if (value >= 0.0995) {
            return REAL_FORMAT_2.format(value);
        } else if (value >= 0.00995) {
            return REAL_FORMAT_3.format(value);
        } else {
            return REAL_FORMAT_4.format(value);
        }
    }

    /**
     * Formats a specified numeric value as int.
     *
     * @param value a value to be formatted
     * @return the formatted string.
     */
    public static String formatInt(double value) {
        return INT_FORMAT.format(value);
    }

    /**
     * Formats a specified time value.
     *
     * @param date represents time
     * @return the formatted string.
     */
    public static String formatTime(Date date) {
        return TIME_FORMAT.format(date);
    }

    /**
     * Formats a specified exception stack trace as a single string. (for
     * written to the results.txt file at the end of a result line)
     *
     * @param e an exception to format its stack trace
     * @return the stack trace as a single sting.
     */
    public static String formatStackTrace(Throwable e) {
        // Convert the stack to a string:
        ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
        try (PrintWriter writer = new PrintWriter(out)) {
            e.printStackTrace(writer);
        }
        String stackTrace = out.toString();
        // Replace new lines with a delimiter:
        String delimiter = "|||";
        stackTrace = stackTrace.replace(FormatHelper.NEW_LINE, delimiter);
        stackTrace = stackTrace.replace("\r\n", delimiter);
        stackTrace = stackTrace.replace("\r", delimiter);
        stackTrace = stackTrace.replace("\n", delimiter);
        // Return the result single line string:
        return stackTrace;
    }
}
