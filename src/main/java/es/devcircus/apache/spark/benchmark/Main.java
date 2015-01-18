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

import es.devcircus.apache.spark.benchmark.sql.tests.query01.Query01HiveTest;
import es.devcircus.apache.spark.benchmark.sql.tests.query01.Query01ProgrammaticallyTest;
import es.devcircus.apache.spark.benchmark.sql.tests.query01.Query01ReflectionTest;
import es.devcircus.apache.spark.benchmark.sql.tests.query02.Query02HiveTest;
import es.devcircus.apache.spark.benchmark.sql.tests.query02.Query02ProgrammaticallyTest;
import es.devcircus.apache.spark.benchmark.sql.tests.query02.Query02ReflectionTest;
import es.devcircus.apache.spark.benchmark.sql.tests.query03.Query03HiveTest;
import es.devcircus.apache.spark.benchmark.sql.tests.query03.Query03ProgrammaticallyTest;
import es.devcircus.apache.spark.benchmark.sql.tests.query03.Query03ReflectionTest;
import es.devcircus.apache.spark.benchmark.sql.tests.query04.Query04HiveTest;
import es.devcircus.apache.spark.benchmark.util.BenchmarkExecutor;
import es.devcircus.apache.spark.benchmark.util.config.ConfigurationManager;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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

    private static Map<String, Boolean> sqlBenchmarResults = new HashMap<String, Boolean>();

    /**
     *
     * @param args
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        // Cargamos la configuracion del benchmark.
        ConfigurationManager.loadConfigure("benchmark.properties");
        // Ejecutamos los Benchmark de SQL
        executeSqlBenchmarks();
        // Una vez hemos terminado la ejecucion de los benchmark, mostramos los
        // resultados por pantalla.
        printResults();
    }

    /**
     *
     */
    private static void executeSqlBenchmarks() {
        // Variable auxiliar con la que gestionamos los resultados de los benchmark.
        Boolean result;
        
        // Bloque de ejecucion de los benchmark.
//        Query01HiveTest query01HiveTest = new Query01HiveTest();
//        result = BenchmarkExecutor.process(query01HiveTest);
//        if (result) {
//            sqlBenchmarResults.put(query01HiveTest.getName(), result);
//        }

//        Query01ProgrammaticallyTest query01ProgrammaticallyTest = new Query01ProgrammaticallyTest();
//        result = BenchmarkExecutor.process(query01ProgrammaticallyTest);
//        if (result) {
//            sqlBenchmarResults.put(query01ProgrammaticallyTest.getName(), result);
//        }

//        Query01ReflectionTest query01ReflectionTest1 = new Query01ReflectionTest();
//        result = BenchmarkExecutor.process(query01ReflectionTest1);
//        if (result) {
//            sqlBenchmarResults.put(query01ReflectionTest1.getName(), result);
//        }

//        Query02HiveTest query02HiveTest = new Query02HiveTest();
//        result = BenchmarkExecutor.process(query02HiveTest);
//        if (result) {
//            sqlBenchmarResults.put(query02HiveTest.getName(), result);
//        }

//        Query02ProgrammaticallyTest query02ProgrammaticallyTest = new Query02ProgrammaticallyTest();
//        result = BenchmarkExecutor.process(query02ProgrammaticallyTest);
//        if (result) {
//            sqlBenchmarResults.put(query02ProgrammaticallyTest.getName(), result);
//        }
        
//        Query02ReflectionTest query02ReflectionTest = new Query02ReflectionTest();
//        result = BenchmarkExecutor.process(query02ReflectionTest);
//        if (result) {
//            sqlBenchmarResults.put(query02ReflectionTest.getName(), result);
//        }
                                
//        Query03HiveTest query03HiveTest = new Query03HiveTest();
//        result = BenchmarkExecutor.process(query03HiveTest);
//        if (result) {
//            sqlBenchmarResults.put(query03HiveTest.getName(), result);
//        }

//        Query03ProgrammaticallyTest query03ProgrammaticallyTest = new Query03ProgrammaticallyTest();
//        result = BenchmarkExecutor.process(query03ProgrammaticallyTest);
//        if (result) {
//            sqlBenchmarResults.put(query03ProgrammaticallyTest.getName(), result);
//        }

//        Query03ReflectionTest query03ReflectionTest = new Query03ReflectionTest();
//        result = BenchmarkExecutor.process(query03ReflectionTest);
//        if (result) {
//            sqlBenchmarResults.put(query03ReflectionTest.getName(), result);
//        }

        Query04HiveTest query04HiveTest = new Query04HiveTest();
        result = BenchmarkExecutor.process(query04HiveTest);
        if (result) {
            sqlBenchmarResults.put(query04HiveTest.getName(), result);
        }

    }

    /**
     * Metodo que muestra por pantalla los resultados obtenidos de los
     * diferentes benchmarks ejecutados.
     */
    private static void printResults() {

        System.out.println("----------------------------------------------------------");
        System.out.println(" Resultado de ejecución de las pruebas SQL                ");
        System.out.println("----------------------------------------------------------");

        for (String key : sqlBenchmarResults.keySet()) {
            System.out.println("  - Test..: " + key + " - Resultato..: " + sqlBenchmarResults.get(key));
        }
    }
}
