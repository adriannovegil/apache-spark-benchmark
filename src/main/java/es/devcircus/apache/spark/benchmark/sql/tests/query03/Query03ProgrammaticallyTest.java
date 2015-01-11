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

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.DataType;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.StructField;
import org.apache.spark.sql.api.java.StructType;

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
public class Query03ProgrammaticallyTest {

    private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

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
        SparkConf sparkConf = new SparkConf().setAppName("asb:java:sql:query01-programmatically-test");
        // Creamos un contexto de spark.
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        // Creamos un contexto SQL en el que lanzaremos las querys.
        JavaSQLContext sqlCtx = new JavaSQLContext(ctx);

        // Cargamos los datos desde el fichero de raking.
        JavaRDD<String> rankingData = ctx.textFile("/media/adrian/data/apache_spark_data/text-deflate/tiny/rankings");
        // Cargamos los datos desde el fichero de uservisits.
        JavaRDD<String> uservisitsData = ctx.textFile("/media/adrian/data/apache_spark_data/text-deflate/tiny/uservisits");

        // ---------------------------------------------------------------------
        //  Contamos el numero de resultados cargados.
        // ---------------------------------------------------------------------
        // Contamos los resultados recuperados.
        Long rankingCountResult = rankingData.count();
        Long uservisitsCountResult = uservisitsData.count();
        // Mostramos el resultado del conteo por pantalla.
        System.out.println("Resultado del conteo del RDD de Ranking......: " + rankingCountResult);
        System.out.println("Resultado del conteo del RDD de User Visits..: " + uservisitsCountResult);

        // ---------------------------------------------------------------------
        //  Definimos el modelo de resultado de la consulta mediante programacion
        // ---------------------------------------------------------------------
        // Definimos la lista de atributos.
        List<StructField> rankingFields = new ArrayList<>();
        // Para cada uno de los atributos especificamos el nombre y el tipo de 
        // dato.
        rankingFields.add(DataType.createStructField("pageURL", DataType.StringType, true));
        rankingFields.add(DataType.createStructField("pageRank", DataType.IntegerType, true));
        rankingFields.add(DataType.createStructField("avgDuration", DataType.IntegerType, true));
        // Cremos el esquema de datos a partir de los campos creados.
        StructType rankingSchema = DataType.createStructType(rankingFields);

        // Convertimos las lineas que creamos como String a partir del fichero de
        // texto a instancias de filas. En este punto aun no podemos mapear al
        // esquema concreto.
        JavaRDD<Row> rankingRowRDD = rankingData.map(
                new Function<String, Row>() {
                    @Override
                    public Row call(String record) throws Exception {
                        String[] fields = record.split(",");
                        return Row.create(
                                fields[0],
                                new Integer(fields[1]),
                                new Integer(fields[2]));
                    }
                });

        // Definimos la lista de atributos.
        List<StructField> userVisitsFields = new ArrayList<>();
        // Para cada uno de los atributos especificamos el nombre y el tipo de 
        // dato.
        userVisitsFields.add(DataType.createStructField("sourceIP", DataType.StringType, true));
        userVisitsFields.add(DataType.createStructField("destURL", DataType.StringType, true));
        userVisitsFields.add(DataType.createStructField("visitDate", DataType.TimestampType, true));
        userVisitsFields.add(DataType.createStructField("adRevenue", DataType.FloatType, true));
        userVisitsFields.add(DataType.createStructField("userAgent", DataType.StringType, true));
        userVisitsFields.add(DataType.createStructField("countryCode", DataType.StringType, true));
        userVisitsFields.add(DataType.createStructField("languageCode", DataType.StringType, true));
        userVisitsFields.add(DataType.createStructField("searchWord", DataType.StringType, true));
        userVisitsFields.add(DataType.createStructField("duration", DataType.IntegerType, true));
        // Cremos el esquema de datos a partir de los campos creados.
        StructType userVisitsSchema = DataType.createStructType(userVisitsFields);

        // Convertimos las lineas que creamos como String a partir del fichero de
        // texto a instancias de filas. En este punto aun no podemos mapear al
        // esquema concreto.
        JavaRDD<Row> userVisitsRowRDD = uservisitsData.map(
                new Function<String, Row>() {
                    @Override
                    public Row call(String record) throws Exception {
                        String[] fields = record.split(",");
                        return Row.create(
                                fields[0],
                                fields[1],
                                new Timestamp(formatter.parse(fields[2]).getTime()),
                                new Float(fields[3]),
                                fields[4],
                                fields[5],
                                fields[6],
                                fields[7],
                                new Integer(fields[8]));
                    }
                });

        // ---------------------------------------------------------------------
        //  Creamos el esquema y declaramos la tabla sobre la que vamos a lanzar
        //  la query
        // ---------------------------------------------------------------------
        // Aplicamos el esquema que hemos creado a las lineas que hemos creado en
        // el paso anterior..
        JavaSchemaRDD rankingSchemaRDD = sqlCtx.applySchema(rankingRowRDD, rankingSchema);
        JavaSchemaRDD userVisitsSchemaRDD = sqlCtx.applySchema(userVisitsRowRDD, userVisitsSchema);

        // Registramos la tabla rankings
        rankingSchemaRDD.registerTempTable("rankings");
        userVisitsSchemaRDD.registerTempTable("uservisits");

        // ---------------------------------------------------------------------
        //  Lanzamos la query
        // ---------------------------------------------------------------------
//        JavaSchemaRDD results = sqlCtx.sql(
//                "SELECT sourceIP, destURL, adRevenue, visitDate"
//                + " FROM uservisits UV"
////                + " WHERE UV.visitDate > CAST('1980-01-01 00:00:00.000' AS TIMESTAMP)"
//                + " WHERE UV.visitDate > CAST('2002-01-01 00:00:00.000' AS TIMESTAMP)"
////                + " AND UV.visitDate < CAST('2010-01-01 00:00:00.000' AS TIMESTAMP)");
//                + " AND UV.visitDate < CAST('2002-12-01 00:00:00.000' AS TIMESTAMP)");
//        JavaSchemaRDD results = sqlCtx.sql(
//                "SELECT sourceIP, destURL, adRevenue, visitDate"
//                + " FROM uservisits UV");
//        JavaSchemaRDD results = sqlCtx.sql(
//                "SELECT sourceIP, totalRevenue, avgPageRank"
//                + " FROM"
//                    + " (SELECT sourceIP,"
//                        + " AVG(pageRank) as avgPageRank,"
//                        + " SUM(adRevenue) as totalRevenue"
//                    + " FROM Rankings AS R, UserVisits AS UV"
//                    + " WHERE R.pageURL = UV.destinationURL"
//                        + " AND UV.visitDate"
//                        + " BETWEEN Date('1980-01-01') AND Date('1980-04-01')"
//                    + " GROUP BY UV.sourceIP)"
//                + " ORDER BY totalRevenue DESC LIMIT 1");
        JavaSchemaRDD results = sqlCtx.sql(
                "SELECT sourceIP, sum(adRevenue) as totalRevenue, avg(pageRank) as pageRank"
                + " FROM rankings R JOIN"
                + " (SELECT sourceIP, destURL, adRevenue"
                + " FROM uservisits UV"
                + " WHERE UV.visitDate > CAST('2002-01-01 00:00:00.000' AS TIMESTAMP)"
                + " AND UV.visitDate < CAST('2010-01-01 00:00:00.000' AS TIMESTAMP))"
                + " NUV ON (R.pageURL = NUV.destURL)"
                + " GROUP BY sourceIP"
                + " ORDER BY totalRevenue DESC LIMIT 1");
        // ---------------------------------------------------------------------
        //  Mostramos el resultado por pantalla.
        // ---------------------------------------------------------------------
        List<String> names = results.map(new Function<Row, String>() {
            @Override
            public String call(Row row) {
//                return "sourceIP..: " + row.getString(0) + " - destURL..: "
//                        + row.getString(1) + " - adRevenue..: " + row.getFloat(2)
//                        + " - visitDate..: " + row.get(3);

                return "ip..: " + row.getString(0) + " - totalRevenue..: "
                        + row.getDouble(1) + " - pageRank..: " + row.getDouble(2);

//                return "Hola mundo!!!!!";
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
