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
package es.devcircus.apache.spark.benchmark.sql.tests.query01;

import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.hive.api.java.JavaHiveContext;

/**
 * 1. Scan Query
 *
 * <pre>
 * SELECT pageURL, pageRank FROM rankings WHERE pageRank > X
 * </pre>
 *
 * This query scans and filters the dataset and stores the results.
 *
 * This query primarily tests the throughput with which each framework can read
 * and write table data. The best performers are Impala (mem) and Shark (mem)
 * which see excellent throughput by avoiding disk. For on-disk data, Redshift
 * sees the best throughput for two reasons. First, the Redshift clusters have
 * more disks and second, Redshift uses columnar compression which allows it to
 * bypass a field which is not used in the query. Shark and Impala scan at HDFS
 * throughput with fewer disks.
 *
 * Both Shark and Impala outperform Hive by 3-4X due in part to more efficient
 * task launching and scheduling. As the result sets get larger, Impala becomes
 * bottlenecked on the ability to persist the results back to disk. Nonetheless,
 * since the last iteration of the benchmark Impala has improved its performance
 * in materializing these large result-sets to disk.
 *
 * Tez sees about a 40% improvement over Hive in these queries. This is in part
 * due to the container pre-warming and reuse, which cuts down on JVM
 * initialization time.
 *
 * @author Adrian Novegil <adrian.novegil@gmail.com>
 */
public class Query01HiveTest {

    /**
     * Método principal.
     *
     * @param args Argumentos que le pasamos al programa.
     */
    public static void main(String[] args) {

        /**
         * Once you have launched the Spark shell, the next step is to create a
         * SQLContext. A SQLConext wraps the SparkContext, which you used in the
         * previous lesson, and adds functions for working with structured data.
         */
        // Seteamos el nombre del programa. Este nombre se usara en el cluster
        // para su ejecución.
        SparkConf sparkConf = new SparkConf().setAppName("asb:java:sql:query01-hive-test");
        // Creamos un contexto de spark.
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        // Creamos un contexto SQL en el que lanzaremos las querys.
        JavaHiveContext sqlCtx = new JavaHiveContext(ctx);

        // ---------------------------------------------------------------------
        //  Momento SQL.
        // ---------------------------------------------------------------------
        // Si existiese previamente la tabla, nos la cargamos.
        sqlCtx.sql("DROP TABLE IF EXISTS rankings");
        // Creamos la tabla y cargamo slos datos.
        sqlCtx.sql(" CREATE TABLE IF NOT EXISTS rankings (pageURL STRING, pageRank INT,"
                + " avgDuration INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
                + " STORED AS TEXTFILE LOCATION '/media/adrian/data/apache_spark_data/text-deflate/tiny/rankings'");
        // Lanzamos las query sobre los datos.
        JavaSchemaRDD results = sqlCtx.sql("SELECT pageURL, pageRank FROM rankings WHERE pageRank > 10");

        // ---------------------------------------------------------------------
        //  Mostramos el resultado por pantalla.
        // ---------------------------------------------------------------------
        List<String> names = results.map(new Function<Row, String>() {
            @Override
            public String call(Row row) {
                return "pageURL..: " + row.getString(0) + " - pageRank..: " + row.getInt(1);
            }
        }).collect();

        // Sacamos por pantalla los resultados de la query
        for (String name : names) {
            System.out.println(name);
        }

        // Paramos el contexto.
        ctx.stop();
    }
}
