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
import es.devcircus.apache.spark.benchmark.util.runner.BenchmarkExecutor;
import es.devcircus.apache.spark.benchmark.util.Launcher;
import es.devcircus.apache.spark.benchmark.util.config.ConfigurationManager;
import es.devcircus.apache.spark.benchmark.util.runner.Runner;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class
 *
 * @author Adrian Novegil Toledo <adrian.novegil@gmail.com>
 */
public class Main {

    // Looger del test
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    /**
     * Timeout in milliseconds to wait for every run
     */
    private static long timeout = 1000 * 300;

    /**
     * Mapa de resultados de la ejecucion de los benchmark.
     */
    private static final Map<String, Boolean> sqlBenchmarResults = new HashMap<>();

    /**
     * Metodo principal
     *
     * @param args
     */
    public static void main(String[] args) {
        new Main().run();
    }

    /**
     * Ejecutamos los test. Inicialmente este metodo solamente lanza la
     * ejecucion de las pruebas SQL. Sin embargo, si se decide anhadir otras
     * pruebas adicionales, como por ejemplo del framework Mlib o de Streaming
     * se haria en este punto.
     */
    private void run() {
        // Ejecutamos los Benchmark de SQL
        runAllActiveSQLCombinations();
    }

    /**
     * Metodo que ejecuta todas las combinaciones de test para el caso de SQL
     */
    private static void runAllActiveSQLCombinations() {
        // Recuperamos el modo de ejecucion. En funcion del valor de esta variable
        // ejecutamos el test de manera directa, normalmente para depuracion, o 
        // en caso contrario, ejecutamos el test en una instancia independiente
        // de la maquina virtual.
        Boolean jvmExecution
                = ConfigurationManager.get("apache.benchmark.config.global.independent.jvm.execution").equals("1");
        // Lista de argumentos
        List<String> argList = null;
        // Array con los jars a incluis necesarios para la ejecución
        String[] jarPaths = new String[0];
        // Bloque de ejecucion de los benchmark.
        // Ejcucion de la query 01 delegando en Hive.
        if (ConfigurationManager.get("apache.benchmark.config.sql.query.01.hive.active").equals("1")) {
            // Recuperamos los valores posibles para el test.
            String crudeTestValues = ConfigurationManager.get("apache.benchmark.config.sql.query.01.hive.test.values");
            String[] crudeTestValuesParts = crudeTestValues.split(",");
            // Ejecutamos el test para cada uno de los valores especificados.
            for (String currentTestValue : crudeTestValuesParts) {
                argList = new ArrayList<>();
                argList.add(currentTestValue);
                if (jvmExecution) {
                    runTest(ConfigurationManager.get("apache.benchmark.config.sql.query.01.hive.class"),
                            Runner.class,
                            argList,
                            jarPaths);
                } else {
                    // Creamos una instancia del test a ejecutar.
                    Query01HiveTest query01HiveTest = new Query01HiveTest();
                    // Seteamos el valor de la prueba que queremos ejecutar.
                    query01HiveTest.setTestValue(currentTestValue);
                    // Ejecutamos y seteamos en el mapa de resultados el nombre del
                    // test y el resultado obtenido.
                    sqlBenchmarResults.put(query01HiveTest.getName(),
                            BenchmarkExecutor.process(query01HiveTest));
                }
            }
        }
        // Ejcucion de la query 01, la definicion del modelo se ha hecho de manera
        // programativa.
        if (ConfigurationManager.get("apache.benchmark.config.sql.query.01.programmatically.active").equals("1")) {
            // Recuperamos los valores posibles para el test.
            String crudeTestValues = ConfigurationManager.get("apache.benchmark.config.sql.query.01.programmatically.test.values");
            String[] crudeTestValuesParts = crudeTestValues.split(",");
            // Ejecutamos el test para cada uno de los valores especificados.
            for (String currentTestValue : crudeTestValuesParts) {
                argList = new ArrayList<>();
                argList.add(currentTestValue);
                if (jvmExecution) {
                    runTest(ConfigurationManager.get("apache.benchmark.config.sql.query.01.programmatically.class"),
                            Runner.class,
                            argList,
                            jarPaths);
                } else {
                    // Creamos una instancia del test a ejecutar.
                    Query01ProgrammaticallyTest query01ProgrammaticallyTest = new Query01ProgrammaticallyTest();
                    // Seteamos el valor de la prueba que queremos ejecutar.
                    query01ProgrammaticallyTest.setTestValue(currentTestValue);
                    // Ejecutamos y seteamos en el mapa de resultados el nombre del
                    // test y el resultado obtenido.
                    sqlBenchmarResults.put(query01ProgrammaticallyTest.getName(),
                            BenchmarkExecutor.process(query01ProgrammaticallyTest));
                }
            }
        }
        // Ejcucion de la query 01, la definicion del modelo se ha hecho usando
        // reflexion.
        if (ConfigurationManager.get("apache.benchmark.config.sql.query.01.reflection.active").equals("1")) {
            // Recuperamos los valores posibles para el test.
            String crudeTestValues = ConfigurationManager.get("apache.benchmark.config.sql.query.01.reflection.test.values");
            String[] crudeTestValuesParts = crudeTestValues.split(",");
            // Ejecutamos el test para cada uno de los valores especificados.
            for (String currentTestValue : crudeTestValuesParts) {
                argList = new ArrayList<>();
                argList.add(currentTestValue);
                if (jvmExecution) {
                    runTest(ConfigurationManager.get("apache.benchmark.config.sql.query.01.reflection.class"),
                            Runner.class,
                            argList,
                            jarPaths);
                } else {
                    // Creamos una instancia del test a ejecutar.
                    Query01ReflectionTest query01ReflectionTest1 = new Query01ReflectionTest();
                    // Seteamos el valor de la prueba que queremos ejecutar.
                    query01ReflectionTest1.setTestValue(currentTestValue);
                    // Ejecutamos y seteamos en el mapa de resultados el nombre del
                    // test y el resultado obtenido.
                    sqlBenchmarResults.put(query01ReflectionTest1.getName(),
                            BenchmarkExecutor.process(query01ReflectionTest1));
                }
            }
        }
        // Ejcucion de la query 02 delegando en Hive.
        if (ConfigurationManager.get("apache.benchmark.config.sql.query.02.hive.active").equals("1")) {
            // Recuperamos los valores posibles para el test.
            String crudeTestValues = ConfigurationManager.get("apache.benchmark.config.sql.query.02.hive.test.values");
            String[] crudeTestValuesParts = crudeTestValues.split(",");
            // Ejecutamos el test para cada uno de los valores especificados.
            for (String currentTestValue : crudeTestValuesParts) {
                argList = new ArrayList<>();
                argList.add(currentTestValue);
                if (jvmExecution) {
                    runTest(ConfigurationManager.get("apache.benchmark.config.sql.query.02.hive.class"),
                            Runner.class,
                            argList,
                            jarPaths);
                } else {
                    // Creamos una instancia del test a ejecutar.
                    Query02HiveTest query02HiveTest = new Query02HiveTest();
                    // Seteamos el valor de la prueba que queremos ejecutar.
                    query02HiveTest.setTestValue(currentTestValue);
                    // Ejecutamos y seteamos en el mapa de resultados el nombre del
                    // test y el resultado obtenido.
                    sqlBenchmarResults.put(query02HiveTest.getName(),
                            BenchmarkExecutor.process(query02HiveTest));
                }
            }
        }
        // Ejcucion de la query 02, la definicion del modelo se ha hecho de manera
        // programativa.
        if (ConfigurationManager.get("apache.benchmark.config.sql.query.02.programmatically.active").equals("1")) {
            // Recuperamos los valores posibles para el test.
            String crudeTestValues = ConfigurationManager.get("apache.benchmark.config.sql.query.02.programmatically.test.values");
            String[] crudeTestValuesParts = crudeTestValues.split(",");
            // Ejecutamos el test para cada uno de los valores especificados.
            for (String currentTestValue : crudeTestValuesParts) {
                argList = new ArrayList<>();
                argList.add(currentTestValue);
                if (jvmExecution) {
                    runTest(ConfigurationManager.get("apache.benchmark.config.sql.query.02.programmatically.class"),
                            Runner.class,
                            argList,
                            jarPaths);
                } else {
                    // Creamos una instancia del test a ejecutar.
                    Query02ProgrammaticallyTest query02ProgrammaticallyTest = new Query02ProgrammaticallyTest();
                    // Seteamos el valor de la prueba que queremos ejecutar.
                    query02ProgrammaticallyTest.setTestValue(currentTestValue);
                    // Ejecutamos y seteamos en el mapa de resultados el nombre del
                    // test y el resultado obtenido.
                    sqlBenchmarResults.put(query02ProgrammaticallyTest.getName(),
                            BenchmarkExecutor.process(query02ProgrammaticallyTest));
                }
            }
        }
        // Ejcucion de la query 02, la definicion del modelo se ha hecho usando
        // reflexion.
        if (ConfigurationManager.get("apache.benchmark.config.sql.query.02.reflection.active").equals("1")) {
            // Recuperamos los valores posibles para el test.
            String crudeTestValues = ConfigurationManager.get("apache.benchmark.config.sql.query.02.reflection.test.values");
            String[] crudeTestValuesParts = crudeTestValues.split(",");
            // Ejecutamos el test para cada uno de los valores especificados.
            for (String currentTestValue : crudeTestValuesParts) {
                argList = new ArrayList<>();
                argList.add(currentTestValue);
                if (jvmExecution) {
                    runTest(ConfigurationManager.get("apache.benchmark.config.sql.query.02.reflection.class"),
                            Runner.class,
                            argList,
                            jarPaths);
                } else {
                    // Creamos una instancia del test a ejecutar.
                    Query02ReflectionTest query02ReflectionTest = new Query02ReflectionTest();
                    // Seteamos el valor de la prueba que queremos ejecutar.
                    query02ReflectionTest.setTestValue(currentTestValue);
                    // Ejecutamos y seteamos en el mapa de resultados el nombre del
                    // test y el resultado obtenido.
                    sqlBenchmarResults.put(query02ReflectionTest.getName(),
                            BenchmarkExecutor.process(query02ReflectionTest));
                }
            }
        }
        // Ejcucion de la query 03 delegando en Hive.
        if (ConfigurationManager.get("apache.benchmark.config.sql.query.03.hive.active").equals("1")) {
            // Recuperamos los valores posibles para el test.
            String crudeTestValues = ConfigurationManager.get("apache.benchmark.config.sql.query.03.hive.test.values");
            String[] crudeTestValuesParts = crudeTestValues.split(",");
            // Ejecutamos el test para cada uno de los valores especificados.
            for (String currentTestValue : crudeTestValuesParts) {
                argList = new ArrayList<>();
                argList.add(currentTestValue);
                if (jvmExecution) {
                    runTest(ConfigurationManager.get("apache.benchmark.config.sql.query.03.hive.class"),
                            Runner.class,
                            argList,
                            jarPaths);
                } else {
                    // Creamos una instancia del test a ejecutar.
                    Query03HiveTest query03HiveTest = new Query03HiveTest();
                    // Seteamos el valor de la prueba que queremos ejecutar.
                    query03HiveTest.setTestValue(currentTestValue);
                    // Ejecutamos y seteamos en el mapa de resultados el nombre del
                    // test y el resultado obtenido.
                    sqlBenchmarResults.put(query03HiveTest.getName(),
                            BenchmarkExecutor.process(query03HiveTest));
                }
            }
        }
        // Ejcucion de la query 03, la definicion del modelo se ha hecho de manera
        // programativa.
        if (ConfigurationManager.get("apache.benchmark.config.sql.query.03.programmatically.active").equals("1")) {
            // Recuperamos los valores posibles para el test.
            String crudeTestValues = ConfigurationManager.get("apache.benchmark.config.sql.query.03.programmatically.test.values");
            String[] crudeTestValuesParts = crudeTestValues.split(",");
            // Ejecutamos el test para cada uno de los valores especificados.
            for (String currentTestValue : crudeTestValuesParts) {
                argList = new ArrayList<>();
                argList.add(currentTestValue);
                if (jvmExecution) {
                    runTest(ConfigurationManager.get("apache.benchmark.config.sql.query.03.programmatically.class"),
                            Runner.class,
                            argList,
                            jarPaths);
                } else {
                    // Creamos una instancia del test a ejecutar.
                    Query03ProgrammaticallyTest query03ProgrammaticallyTest = new Query03ProgrammaticallyTest();
                    // Seteamos el valor de la prueba que queremos ejecutar.
                    query03ProgrammaticallyTest.setTestValue(currentTestValue);
                    // Ejecutamos y seteamos en el mapa de resultados el nombre del
                    // test y el resultado obtenido.
                    sqlBenchmarResults.put(query03ProgrammaticallyTest.getName(),
                            BenchmarkExecutor.process(query03ProgrammaticallyTest));
                }
            }
        }
        // Ejcucion de la query 03, la definicion del modelo se ha hecho usando
        // reflexion.
        if (ConfigurationManager.get("apache.benchmark.config.sql.query.03.reflection.active").equals("1")) {
            // Recuperamos los valores posibles para el test.
            String crudeTestValues = ConfigurationManager.get("apache.benchmark.config.sql.query.03.reflection.test.values");
            String[] crudeTestValuesParts = crudeTestValues.split(",");
            // Ejecutamos el test para cada uno de los valores especificados.
            for (String currentTestValue : crudeTestValuesParts) {
                argList = new ArrayList<>();
                argList.add(currentTestValue);
                if (jvmExecution) {
                    runTest(ConfigurationManager.get("apache.benchmark.config.sql.query.03.reflection.class"),
                            Runner.class,
                            argList,
                            jarPaths);
                } else {
                    // Creamos una instancia del test a ejecutar.
                    Query03ReflectionTest query03ReflectionTest = new Query03ReflectionTest();
                    // Seteamos el valor de la prueba que queremos ejecutar.
                    query03ReflectionTest.setTestValue(currentTestValue);
                    // Ejecutamos y seteamos en el mapa de resultados el nombre del
                    // test y el resultado obtenido.
                    sqlBenchmarResults.put(query03ReflectionTest.getName(),
                            BenchmarkExecutor.process(query03ReflectionTest));
                }
            }
        }
        // Ejcucion de la query 04 delegando en Hive.
        if (ConfigurationManager.get("apache.benchmark.config.sql.query.04.hive.active").equals("1")) {
            if (jvmExecution) {
                runTest(ConfigurationManager.get("apache.benchmark.config.sql.query.04.hive.class"),
                        Runner.class,
                        argList,
                        jarPaths);
            } else {
                // Creamos una instancia del test a ejecutar.
                Query04HiveTest query04HiveTest = new Query04HiveTest();
                // Ejecutamos y seteamos en el mapa de resultados el nombre del
                // test y el resultado obtenido.
                sqlBenchmarResults.put(query04HiveTest.getName(),
                        BenchmarkExecutor.process(query04HiveTest));
            }

        }
        // Finalmente, despues de ejecutar los test, si el modo de ejecucion es
        // directo, mostramos el resultado de la ejecucion por pantalla.
        if (!jvmExecution) {
            printResults();
        }
    }

    /**
     * Metodo que se encarga de lanzar un test concreto.
     */
    private static void runTest(String testClass, Class mainClass, List<String> argList, String[] jarPaths) {
        // Instanciamos una nueva instancia que Launcher que sera quien se encargue
        // de lanzar el test.
        Launcher launcher = new Launcher(testClass, mainClass, argList, jarPaths);
        long startTime = System.currentTimeMillis();
        launcher.start();
        // Wait for the launcher's and its subprocess:
        try {
//            launcher.join(timeout);
            launcher.join();
        } catch (InterruptedException e) {
        }
        // Kill the sub process if still running:
        if (launcher.isAlive()) {
            launcher.kill();
            try {
                launcher.join();
            } catch (InterruptedException e) {
            }
        }
        // Print messages:
        long elapsedTime = (System.currentTimeMillis() - startTime) / 1000;
        System.out.print(launcher.getStdOutMessage());
        System.out.println("Test class executed..: " + testClass + ", completed in " + elapsedTime + " seconds.");
        System.out.println(launcher.getStdErrMessage());
    }

    /**
     * Metodo que muestra por pantalla los resultados obtenidos de los
     * diferentes benchmarks ejecutados.
     */
    private static void printResults() {
        System.out.println();
        System.out.println("----------------------------------------------------------");
        System.out.println(" Resultado de ejecución de las pruebas SQL                ");
        System.out.println("----------------------------------------------------------");
        System.out.println();
        for (String key : sqlBenchmarResults.keySet()) {
            System.out.println("  - Benchmark..: " + key + " - Resultado..: "
                    + (sqlBenchmarResults.get(key) ? "[OK]" : "[FAIL]"));
        }
        System.out.println("");
    }
}
