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
package es.devcircus.apache.spark.benchmark.sql.tests.query02;

import es.devcircus.apache.spark.benchmark.util.config.ConfigurationManager;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.hive.api.java.JavaHiveContext;

/**
 * 2. Aggregation Query
 *
 * <pre>
 * SELECT SUBSTR(sourceIP, 1, X), SUM(adRevenue) FROM uservisits GROUP BY
 * SUBSTR(sourceIP, 1, X)
 * </pre>
 *
 * This query applies string parsing to each input tuple then performs a
 * high-cardinality aggregation.
 *
 * Redshift's columnar storage provides greater benefit than in Query 1 since
 * several columns of the UserVistits table are un-used. While Shark's in-memory
 * tables are also columnar, it is bottlenecked here on the speed at which it
 * evaluates the SUBSTR expression. Since Impala is reading from the OS buffer
 * cache, it must read and decompress entire rows. Unlike Shark, however, Impala
 * evaluates this expression using very efficient compiled code. These two
 * factors offset each other and Impala and Shark achieve roughly the same raw
 * throughput for in memory tables. For larger result sets, Impala again sees
 * high latency due to the speed of materializing output tables.
 *
 * @author Adrian Novegil <adrian.novegil@gmail.com>
 */
public class Query02HiveTest extends Query02Test {

    private static SparkConf sparkConf;
    private static JavaSparkContext ctx;
    private static JavaHiveContext sqlCtx;

    /**
     * Metodo que se encarga de la inicializacion del contexto Spark de
     * ejecucion, variables de entorno, etc.
     *
     * @return True si el metodo se ha ejecutado correctamente, false en caso
     * contrario.
     */
    @Override
    public Boolean config() {
        // Intanciamos el objeto de configuracion.
        sparkConf = new SparkConf();
        // Indicamos la direccion al nodo master. El valor puede ser la ip del
        // nodo master, o "local" en el caso de que querramos ejecutar la app
        // en modo local.
        sparkConf.setMaster(
                ConfigurationManager.get("apache.benchmark.config.global.master"));
        // Seteamos el nombre del programa. Este nombre se usara en el cluster
        // para su ejecucion y en el proyecto para los resultados.
        setName(ConfigurationManager.get("apache.benchmark.config.sql.query.02.hive.name"));
        sparkConf.setAppName(getName());
        // Seteamos el path a la instalacion de spark
        sparkConf.setSparkHome(
                ConfigurationManager.get("apache.benchmark.config.global.spark.home"));
        // Creamos un contexto de spark.
        ctx = new JavaSparkContext(sparkConf);
        // Creamos un contexto SQL en el que lanzaremos las querys.
        sqlCtx = new JavaHiveContext(ctx);
        // Retornamos true indicando que el metodo ha terminado correctamente
        return true;
    }

    /**
     * Metodo que se encarga de ejecutar todas las acciones necesarias para
     * preparar el contexto del benchmark.
     *
     * @return True si el metodo se ha ejecutado correctamente, false en caso
     * contrario.
     */
    @Override
    public Boolean prepare() {
        // Si existiese previamente la tabla, nos la cargamos.
        sqlCtx.hql(this.getDropUservisitsTableQuery());
        // Creamos la tabla y cargamo slos datos.
        sqlCtx.hql(this.getCreateUservisitsTableQuery());
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
        JavaSchemaRDD results = null;
        // Lanzamos las query sobre los datos.
        results = sqlCtx.sql(this.getTopValueSelectQuery(10));
        // Si esta activo el modo de debug llamamos al metodo que muestra los 
        // datos.
        if (VERBOSE_MODE) {
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
        ctx.stop();
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
                return "SUBSTR..: " + row.getString(0) + " - SUM..: " + row.getDouble(1);
            }
        }).collect();
        // Sacamos por pantalla los resultados de la query
        for (String name : names) {
            System.out.println(name);
        }
    }
}
