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
package es.devcircus.apache.spark.benchmark;

import es.devcircus.apache.spark.benchmark.sql.tests.query02.Query02ProgrammaticallyTest;
import java.io.IOException;

/**
 * Main class
 *
 * @author Adrian Novegil Toledo <adrian.novegil@gmail.com>
 */
public class Main {

    /**
     * Timeout in milliseconds to wait for every run
     */
    private static long timeout;

    /**
     * Name of the active persistence unit
     */
    private static String persistenceUnitName = "prueba";

    /**
     *
     * @param args
     */
    public static void main(String[] args) throws IOException, InterruptedException {

        Query02ProgrammaticallyTest test = new Query02ProgrammaticallyTest();
        
        test.prepare();
        test.execute();
        test.commit();
        test.close();
        
    }
}
