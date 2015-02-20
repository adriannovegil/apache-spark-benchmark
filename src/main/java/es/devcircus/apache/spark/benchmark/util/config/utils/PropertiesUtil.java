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
package es.devcircus.apache.spark.benchmark.util.config.utils;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 *
 * @author Adrian Novegil <adrian.novegil@gmail.com>
 */
public class PropertiesUtil {

    /**
     *
     * @param sourceProperties
     * @param targetProperties
     */
    public static void copyProperties(
            Properties sourceProperties, Properties targetProperties) {
        for (Map.Entry<Object, Object> entry : sourceProperties.entrySet()) {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            targetProperties.setProperty(key, value);
        }
    }

    /**
     *
     * @param map
     * @return
     */
    public static Properties getPropertiesfromMap(Map<String, String> map) {
        Properties properties = new Properties();
        Iterator<Map.Entry<String, String>> itr = map.entrySet().iterator();
        while (itr.hasNext()) {
            Map.Entry<String, String> entry = itr.next();
            String key = entry.getKey();
            String value = entry.getValue();
            if (value != null) {
                properties.setProperty(key, value);
            }
        }
        return properties;
    }

    /**
     *
     * @param properties
     * @return
     */
    public static Properties getPropertiesfromMap(Properties properties) {
        return properties;
    }

    /**
     *
     * @param properties
     * @param map
     */
    public static void clearMapAndLoadFromProperties(
            Properties properties, Map<String, String> map) {
        map.clear();
        Iterator<Map.Entry<Object, Object>> itr
                = properties.entrySet().iterator();
        while (itr.hasNext()) {
            Map.Entry<Object, Object> entry = itr.next();

            map.put((String) entry.getKey(), (String) entry.getValue());
        }
    }

    /**
     *
     * @param properties
     * @param map
     */
    public static void appendPropertiesToMap(
            Properties properties, Map<String, String> map) {
        Iterator<Map.Entry<Object, Object>> itr
                = properties.entrySet().iterator();
        while (itr.hasNext()) {
            Map.Entry<Object, Object> entry = itr.next();
            map.put((String) entry.getKey(), (String) entry.getValue());
        }
    }

    /**
     *
     * @param properties
     * @param prefix
     * @param removePrefix
     * @return
     */
    public static Properties getProperties(
            Properties properties, String prefix, boolean removePrefix) {
        Properties newProperties = new Properties();
        Enumeration<String> enu
                = (Enumeration<String>) properties.propertyNames();
        while (enu.hasMoreElements()) {
            String key = enu.nextElement();
            if (key.startsWith(prefix)) {
                String value = properties.getProperty(key);
                if (removePrefix) {
                    key = key.substring(prefix.length());
                }
                newProperties.setProperty(key, value);
            }
        }
        return newProperties;
    }

    /**
     *
     * @param map
     * @param printWriter
     */
    public static void list(Map<String, String> map, PrintStream printWriter) {
        Properties properties = getPropertiesfromMap(map);
        properties.list(printWriter);
    }

    /**
     *
     * @param map
     * @param printWriter
     */
    public static void list(Map<String, String> map, PrintWriter printWriter) {
        Properties properties = getPropertiesfromMap(map);
        properties.list(printWriter);
    }

    /**
     *
     * @param properties1
     * @param properties2
     */
    public static void merge(Properties properties1, Properties properties2) {
        Enumeration<String> enu
                = (Enumeration<String>) properties2.propertyNames();
        while (enu.hasMoreElements()) {
            String key = enu.nextElement();
            String value = properties2.getProperty(key);
            properties1.setProperty(key, value);
        }
    }

}
