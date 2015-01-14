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

import es.devcircus.apache.spark.benchmark.sql.model.Ranking;
import es.devcircus.apache.spark.benchmark.util.Test;
import es.devcircus.apache.spark.benchmark.util.config.ConfigurationManager;
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
public class Query01ProgrammaticallyTest extends Test {

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
                ConfigurationManager.get("apache.benchmark.config.sql.query.01.programmatically.name"));
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
        // Cargamos los datos desde el fichero de raking.
        JavaRDD<String> rankingData = ctx.textFile(BASE_DATA_PATH + "/rankings");
        // Contamos los resultados recuperados.
        Long countResult = rankingData.count();
        // Mostramos el resultado del conteo por pantalla.
        System.out.println("Resultado del conteo del RDD...: " + countResult);
        // ---------------------------------------------------------------------
        //  Definimos el modelo de resultado de la consulta mediante programacion
        // ---------------------------------------------------------------------
        // Definimos la lista de atributos.
        List<StructField> fields = new ArrayList<>();
        // Para cada uno de los atributos especificamos el nombre y el tipo de 
        // dato.
        fields.add(DataType.createStructField(Ranking.PAGE_URL_KEY, DataType.StringType, true));
        fields.add(DataType.createStructField(Ranking.PAGE_RANK_KEY, DataType.IntegerType, true));
        fields.add(DataType.createStructField(Ranking.AVG_DURATION_KEY, DataType.IntegerType, true));
        // Cremos el esquema de datos a partir de los campos creados.
        StructType schema = DataType.createStructType(fields);
        // Convertimos las lineas que creamos como String a partir del fichero de
        // texto a instancias de filas. En este punto aun no podemos mapear al
        // esquema concreto.
        JavaRDD<Row> rowRDD = rankingData.map(
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
        // ---------------------------------------------------------------------
        //  Creamos el esquema y declaramos la tabla sobre la que vamos a lanzar
        //  la query
        // ---------------------------------------------------------------------
        // Aplicamos el esquema que hemos creado a las lineas que hemos creado en
        // el paso anterior..
        JavaSchemaRDD rankingSchemaRDD = sqlCtx.applySchema(rowRDD, schema);
        // Registramos la tabla rankings
        rankingSchemaRDD.registerTempTable("rankings");
        JavaSchemaRDD results = null;
        // Repetimos la ejecucion de la query tantas veces como sea necesario.        
        for (int i = 0; i < NUM_TRIALS; i++) {
            //  Lanzamos la query        
            results = sqlCtx.sql("SELECT pageURL, pageRank FROM rankings WHERE pageRank > 10");
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
                return "pageURL..: " + row.getString(0) + " - pageRank..: " + row.getInt(1);
            }
        }).collect();
        // Sacamos por pantalla los resultados de la query
        for (String name : names) {
            System.out.println(name);
        }
    }
}
