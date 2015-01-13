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

import es.devcircus.apache.spark.benchmark.util.Test;
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
public class Query04HiveTest extends Test {

    private SparkConf sparkConf;
    private JavaSparkContext ctx;
    private JavaHiveContext sqlCtx;

    /**
     * Metodo que se encarga de la inicializacion del contexto Spark de
     * ejecucion, variables de entorno, etc.
     *
     * @return True si el metodo se ha ejecutado correctamente, false en caso
     * contrario.
     */
    @Override
    public Boolean prepare() {
        // Intanciamos el objeto de configuracion.
        this.sparkConf = new SparkConf();
        // Indicamos la direccion al nodo master. El valor puede ser la ip del
        // nodo master, o "local" en el caso de que querramos ejecutar la app
        // en modo local.
        this.sparkConf.setMaster("local");
        // Seteamos el nombre del programa. Este nombre se usara en el cluster
        // para su ejecucion.
        this.sparkConf.setAppName("asb:java:sql:query04-hive-test");
        // Seteamos el path a la instalacion de spark
        this.sparkConf.setSparkHome("/opt/spark/bin/spark-submit");
        // Creamos un contexto de spark.
        this.ctx = new JavaSparkContext(this.sparkConf);
        // Creamos un contexto SQL en el que lanzaremos las querys.
        this.sqlCtx = new JavaHiveContext(this.ctx);
        // Retornamos true indicando que el metodo ha terminado correctamente
        return true;
    }

    /**
     * Metodo que ejecuta el core de la prueba que estamos realizando.
     *
     * @return True si el metodo se ha ejecutado correctamente, false en caso
     * contrario.
     */
    @Override
    public Boolean execute() {

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

        // Lanzamos una query para recuperar los datos procesados y verificar
        // el resultado.        
        JavaSchemaRDD results = sqlCtx.hql("SELECT * FROM url_counts_partial");
        
        // Si esta activo el modo de debug llamamos al metodo que muestra los 
        // datos.
        if (true) {
            this.debug(results);
        }
        // Retornamos true indicando que el metodo ha terminado correctamente
        return true;
    }

    /**
     * Metodo que se encarga del procesado de los datos del benchmark,
     * generacion de resultados, etc.
     *
     * @return True si el metodo se ha ejecutado correctamente, false en caso
     * contrario.
     */
    @Override
    public Boolean commit() {
        // Indicamos que todo ha sucedido correctamente.
        return true;
    }

    /**
     * Metodo encargado de finalizar la ejecucion del benchmark, cierre de
     * contextos, etc.
     *
     * @return True si el metodo se ha ejecutado correctamente, false en caso
     * contrario.
     */
    @Override
    public Boolean close() {
        // Paramos el contexto.
        this.ctx.stop();
        // Indicamos que todo ha sucedido correctamente.
        return true;
    }

    /**
     * Metodo interno para mostrar los datos por pantalla. Se usa para verificar
     * que la operacion se ha realizado correctamente.
     *
     * @param results Resultado obtenidos de la operacion realizada.
     */
    private void debug(JavaSchemaRDD results) {
        // Extraemos los resultados a partir del objeto JavaSchemaRDD
        List<String> names = results.map(new Function<Row, String>() {
            @Override
            public String call(Row row) {
                return "01..: " + row.getString(0) + " - 02..: " + row.getString(1) + " - 03..: " + row.getString(2);
            }
        }).collect();
        // Sacamos por pantalla los resultados de la query
        for (String name : names) {
            System.out.println(name);
        }
    }

}
