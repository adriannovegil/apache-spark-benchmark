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

import es.devcircus.apache.spark.benchmark.sql.tests.query01.Query01ReflectionTest;
import es.devcircus.apache.spark.benchmark.util.config.ConfigurationManager;
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

        ConfigurationManager.loadConfigure("benchmark.properties");
        
        Query01ReflectionTest test01 = new Query01ReflectionTest();        
        test01.prepare();
        test01.execute();
        test01.commit();
        test01.close();
        
//        Query02ReflectionTest test02 = new Query02ReflectionTest();        
//        test02.prepare();
//        test02.execute();
//        test02.commit();
//        test02.close();
        
//        Query03ReflectionTest test03 = new Query03ReflectionTest();        
//        test03.prepare();
//        test03.execute();
//        test03.commit();
//        test03.close();
        
    }
}
