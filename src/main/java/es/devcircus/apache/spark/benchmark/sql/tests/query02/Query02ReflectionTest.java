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

import es.devcircus.apache.spark.benchmark.sql.model.UserVisit;
import es.devcircus.apache.spark.benchmark.util.Test;
import es.devcircus.apache.spark.benchmark.util.config.ConfigurationManager;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;

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
public class Query02ReflectionTest extends Test {

    private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

    private static SparkConf sparkConf;
    private static JavaSparkContext ctx;
    private static JavaSQLContext sqlCtx;

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
        sparkConf = new SparkConf();
        // Indicamos la direccion al nodo master. El valor puede ser la ip del
        // nodo master, o "local" en el caso de que querramos ejecutar la app
        // en modo local.
        sparkConf.setMaster(
                ConfigurationManager.get("apache.benchmark.config.global.master"));
        // Seteamos el nombre del programa. Este nombre se usara en el cluster
        // para su ejecucion.
        sparkConf.setAppName(
                ConfigurationManager.get("apache.benchmark.config.sql.query.02.reflection.name"));
        // Seteamos el path a la instalacion de spark
        sparkConf.setSparkHome(
                ConfigurationManager.get("apache.benchmark.config.global.spark.home"));
        // Creamos un contexto de spark.
        ctx = new JavaSparkContext(sparkConf);
        // Creamos un contexto SQL en el que lanzaremos las querys.
        sqlCtx = new JavaSQLContext(ctx);
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
        // Cargamos los datos desde el fichero de uservisits.
        JavaRDD<String> uservisitsData = ctx.textFile(BASE_DATA_PATH + "/uservisits");
        // Contamos los resultados recuperados.
        Long countResult = uservisitsData.count();
        // Mostramos el resultado del conteo por pantalla.
        System.out.println("Resultado del conteo del RDD...: " + countResult);
        // ---------------------------------------------------------------------
        //  Mapeamos los datos leidos a objetos del modelo
        // ---------------------------------------------------------------------        
        // Convertimos las lineas que creamos como String a partir del fichero de
        // texto a instancias de modelo. En este punto aun no podemos mapear al
        // esquema concreto.
        JavaRDD<UserVisit> rowRDD = uservisitsData.map(
                new Function<String, UserVisit>() {
                    @Override
                    public UserVisit call(String record) throws Exception {
                        String[] fields = record.split(",");
                        UserVisit userVisit = new UserVisit();
                        userVisit.setSourceIP(fields[0]);
                        userVisit.setDestURL(fields[1]);
                        userVisit.setVisitDate(new Timestamp(formatter.parse(fields[2]).getTime()));
                        userVisit.setAdRevenue(new Float(fields[3]));
                        userVisit.setUserAgent(fields[4]);
                        userVisit.setCountryCode(fields[5]);
                        userVisit.setLanguageCode(record);
                        userVisit.setSearchWord(fields[6]);
                        userVisit.setDuration(new Integer(fields[8]));
                        return userVisit;
                    }
                });
        // ---------------------------------------------------------------------
        //  Creamos el esquema y declaramos la tabla sobre la que vamos a lanzar
        //  la query
        // ---------------------------------------------------------------------
        // Aplicamos el esquema que hemos creado a las lineas que hemos creado en
        // el paso anterior..
        JavaSchemaRDD userVisitsSchemaRDD = sqlCtx.applySchema(rowRDD, UserVisit.class);
        // Registramos la tabla rankings
        userVisitsSchemaRDD.registerTempTable("uservisits");
        JavaSchemaRDD results = null;
        // Repetimos la ejecucion de la query tantas veces como sea necesario.        
        for (int i = 0; i < NUM_TRIALS; i++) {
            //  Lanzamos la query
            results = sqlCtx.sql("SELECT SUBSTR(sourceIP, 1, 10), SUM(adRevenue) FROM uservisits GROUP BY SUBSTR(sourceIP, 1, 10)");
        }
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
