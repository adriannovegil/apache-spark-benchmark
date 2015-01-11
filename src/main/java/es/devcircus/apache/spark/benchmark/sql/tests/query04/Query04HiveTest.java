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
package es.devcircus.apache.spark.benchmark.sql.tests.query04;

import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.hive.api.java.JavaHiveContext;

/**
 * 4. External Script Query
 *
 * <pre>
 * CREATE TABLE url_counts_partial AS
 *     SELECT TRANSFORM (line)
 *         USING "python /root/url_count.py" as (sourcePage, destPage, cnt)
 *     FROM documents;
 * CREATE TABLE url_counts_total AS
 *     SELECT SUM(cnt) AS totalCount, destPage
 *     FROM url_counts_partial
 *     GROUP BY destPage;
 * </pre>
 *
 * This query calls an external Python function which extracts and aggregates
 * URL information from a web crawl dataset. It then aggregates a total count
 * per URL.
 *
 * Impala and Redshift do not currently support calling this type of UDF, so
 * they are omitted from the result set. Impala UDFs must be written in Java or
 * C++, where as this script is written in Python. The performance advantage of
 * Shark (disk) over Hive in this query is less pronounced than in 1, 2, or 3
 * because the shuffle and reduce phases take a relatively small amount of time
 * (this query only shuffles a small amount of data) so the task-launch overhead
 * of Hive is less pronounced. Also note that when the data is in-memory, Shark
 * is bottlenecked by the speed at which it can pipe tuples to the Python
 * process rather than memory throughput. This makes the speedup relative to
 * disk around 5X (rather than 10X or more seen in other queries).
 *
 * @author Adrian Novegil <adrian.novegil@gmail.com>
 */
public class Query04HiveTest {

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
        SparkConf sparkConf = new SparkConf().setAppName("asb:java:sql:query04-hive-test");        
        // Creamos un contexto de spark.
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);        
        // Creamos un contexto SQL en el que lanzaremos las querys.
        JavaHiveContext sqlCtx = new JavaHiveContext(ctx);
        
//        SparkContext sparkContext = new SparkContext(sparkConf);                
//        SQLContext context = new SQLContext(sparkContext);        
//        context.setConf("hive.metastore.warehouse.dir", "/tmp/warehouse");  
        
        
        
        
//        sparkConf.set("hive.metastore.warehouse.dir", "/tmp");

        // ---------------------------------------------------------------------
        //  Momento SQL.
        // ---------------------------------------------------------------------
        
        
//        sqlCtx.hql("SET hive.metastore.warehouse.dir=/tmp");
        
        
        // Si existiese previamente la tabla, nos la cargamos.
        sqlCtx.hql("DROP TABLE IF EXISTS documents");

        // Creamos la tabla y cargamo slos datos.
        sqlCtx.hql("CREATE EXTERNAL TABLE documents (line STRING) "
                + "STORED AS TEXTFILE LOCATION '/media/adrian/data/apache_spark_data/text-deflate/tiny/crawl'");

        sqlCtx.hql("DROP TABLE IF EXISTS url_counts_partial");

        sqlCtx.hql("CREATE TABLE url_counts_partial AS"
                + " SELECT TRANSFORM (line)"
                + " USING 'python /tmp/url_count.py' as (sourcePage,"
                + " destPage, count) from documents");
        
//        sqlCtx.sql("DROP TABLE IF EXISTS url_counts_total");
//
//        sqlCtx.sql(" CREATE TABLE url_counts_total AS"
//                + " SELECT SUM(count) AS totalCount, destpage"
//                + " FROM url_counts_partial GROUP BY destpage");

        // Lanzamos las query sobre los datos.
//        JavaSchemaRDD results = sqlCtx.sql(
//                "DROP TABLE IF EXISTS url_counts_partial;"
//                + " CREATE TABLE url_counts_partial AS"
//                + " SELECT TRANSFORM (line)"
//                + " USING 'python /tmp/url_count.py' as (sourcePage,"
//                + " destPage, count) from documents;"
//                + " DROP TABLE IF EXISTS url_counts_total;"
//                + " CREATE TABLE url_counts_total AS"
//                + " SELECT SUM(count) AS totalCount, destpage"
//                + " FROM url_counts_partial GROUP BY destpage;");
//        JavaSchemaRDD results1 = sqlCtx.sql("DROP TABLE IF EXISTS url_counts_partial");
//        JavaSchemaRDD results2 = sqlCtx.sql(
//                " CREATE TABLE url_counts_partial AS"
//                    + " SELECT TRANSFORM (line)"
//                    + " USING 'python /tmp/url_count.py' as (sourcePage,"
//                        + " destPage, count) from documents");
//        JavaSchemaRDD results3 = sqlCtx.sql(" DROP TABLE IF EXISTS url_counts_total");
//        JavaSchemaRDD results4 = sqlCtx.sql(
//                " CREATE TABLE url_counts_total AS"
//                    + " SELECT SUM(count) AS totalCount, destpage"
//                    + " FROM url_counts_partial GROUP BY destpage");
        // ---------------------------------------------------------------------
        //  Mostramos el resultado por pantalla.
        // ---------------------------------------------------------------------
//        List<String> names = results.map(new Function<Row, String>() {
//            @Override
//            public String call(Row row) {
//                return "SUBSTR..: " + row.getString(0) + " - SUM..: " + row.getDouble(1);
//            }
//        }).collect();
//
//        // Sacamos por pantalla los resultados de la query
//        for (String name : names) {
//            System.out.println(name);
//        }
        
        // Paramos el contexto.
        ctx.stop();
    }
}
