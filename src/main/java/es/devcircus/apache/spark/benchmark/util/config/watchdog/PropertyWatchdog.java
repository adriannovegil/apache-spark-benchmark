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
package es.devcircus.apache.spark.benchmark.util.config.watchdog;

import es.devcircus.apache.spark.benchmark.util.config.ConfigurationManager;

/**
 *
 * @author Adrian Novegil <adrian.novegil@gmail.com>
 */
public class PropertyWatchdog extends FileWatchdog {

    /**
     *
     * @param filename
     */
    public PropertyWatchdog(String filename) {
        super(filename);
    }

    /**
     *
     */
    @Override
    public void doOnChange() {
        ConfigurationManager.loadConfigure(filename);
    }
}
