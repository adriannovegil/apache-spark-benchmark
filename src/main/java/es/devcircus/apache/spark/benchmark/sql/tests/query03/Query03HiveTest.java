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
package es.devcircus.apache.spark.benchmark.sql.tests.query03;

import java.text.SimpleDateFormat;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.hive.api.java.JavaHiveContext;

/**
 * 3. Join Query
 *
 * <pre>
 * SELECT sourceIP, totalRevenue, avgPageRank
 * FROM
 *     (SELECT sourceIP,
 *         AVG(pageRank) as avgPageRank,
 *         SUM(adRevenue) as totalRevenue
 *     FROM Rankings AS R, UserVisits AS UV
 *     WHERE R.pageURL = UV.destURL
 *         AND UV.visitDate BETWEEN Date(`1980-01-01') AND Date(`X')
 *     GROUP BY UV.sourceIP)
 * ORDER BY totalRevenue DESC LIMIT 1
 * </pre>
 *
 * This query joins a smaller table to a larger table then sorts the results.
 *
 * When the join is small (3A), all frameworks spend the majority of time
 * scanning the large table and performing date comparisons. For larger joins,
 * the initial scan becomes a less significant fraction of overall response
 * time. For this reason the gap between in-memory and on-disk representations
 * diminishes in query 3C. All frameworks perform partitioned joins to answer
 * this query. CPU (due to hashing join keys) and network IO (due to shuffling
 * data) are the primary bottlenecks. Redshift has an edge in this case because
 * the overall network capacity in the cluster is higher.
 *
 * @author Adrian Novegil <adrian.novegil@gmail.com>
 */
public class Query03HiveTest {

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
        SparkConf sparkConf = new SparkConf().setAppName("asb:java:sql:query03-hive-test");
        // Creamos un contexto de spark.
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        // Creamos un contexto SQL en el que lanzaremos las querys.
        JavaHiveContext sqlCtx = new JavaHiveContext(ctx);

        // ---------------------------------------------------------------------
        //  Momento SQL.
        // ---------------------------------------------------------------------
        // Si existiese previamente la tabla, nos la cargamos.
        sqlCtx.sql("DROP TABLE IF EXISTS rankings");
        sqlCtx.sql("DROP TABLE IF EXISTS uservisits");
        // Creamos la tabla y cargamo slos datos.
        sqlCtx.sql("CREATE TABLE IF NOT EXISTS rankings (pageURL STRING, pageRank INT, avgDuration INT)"
                + " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' "
                + " STORED AS TEXTFILE LOCATION '/media/adrian/data/apache_spark_data/text-deflate/tiny/rankings'");   
        
        sqlCtx.sql("CREATE EXTERNAL TABLE uservisits (sourceIP STRING,destURL STRING,"
                + " visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,"
                + " languageCode STRING,searchWord STRING,duration INT )"
                + " ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
                + " STORED AS TEXTFILE LOCATION '/media/adrian/data/apache_spark_data/text-deflate/tiny/uservisits'");
        // Lanzamos las query sobre los datos.
        JavaSchemaRDD results = sqlCtx.sql(
                "SELECT sourceIP, sum(adRevenue) as totalRevenue, avg(pageRank) as pageRank"
                + " FROM rankings R JOIN"
                    + " (SELECT sourceIP, destURL, adRevenue"
                    + " FROM uservisits UV"
                    + " WHERE CAST(UV.visitDate AS TIMESTAMP) > CAST('2002-01-01 00:00:00.000' AS TIMESTAMP)"
                        + " AND CAST(UV.visitDate AS TIMESTAMP) < CAST('2010-01-01 00:00:00.000' AS TIMESTAMP))"
                        + " NUV ON (R.pageURL = NUV.destURL)"
                    + " GROUP BY sourceIP"
                + " ORDER BY totalRevenue DESC LIMIT 1");
      
        // ---------------------------------------------------------------------
        //  Mostramos el resultado por pantalla.
        // ---------------------------------------------------------------------
        List<String> names = results.map(new Function<Row, String>() {
            @Override
            public String call(Row row) {
                return "ip..: " + row.getString(0) + " - totalRevenue..: "
                        + row.getDouble(1) + " - pageRank..: " + row.getDouble(2);
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
