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

import es.devcircus.apache.spark.benchmark.sql.model.Ranking;
import es.devcircus.apache.spark.benchmark.sql.model.UserVisit;
import es.devcircus.apache.spark.benchmark.util.config.ConfigurationManager;
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
public class Query03ProgrammaticallyTest extends Query03Test {

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
        setName(ConfigurationManager.get("apache.benchmark.config.sql.query.03.programmatically.name"));
        sparkConf.setAppName(getName());
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
     * Metodo que se encarga de ejecutar todas las acciones necesarias para
     * preparar el contexto del benchmark.
     *
     * @return True si el metodo se ha ejecutado correctamente, false en caso
     * contrario.
     */
    @Override
    public Boolean prepare() {
        // Cargamos los datos desde el fichero de raking.
        JavaRDD<String> rankingData = ctx.textFile(BASE_DATA_PATH + "/rankings");
        // Cargamos los datos desde el fichero de uservisits.
        JavaRDD<String> uservisitsData = ctx.textFile(BASE_DATA_PATH + "/uservisits");
        if (VERBOSE_MODE) {
            // Contamos los resultados recuperados.
            Long rankingCountResult = rankingData.count();
            Long uservisitsCountResult = uservisitsData.count();
            // Mostramos el resultado del conteo por pantalla.
            System.out.println("Resultado del conteo del RDD de Ranking......: " + rankingCountResult);
            System.out.println("Resultado del conteo del RDD de User Visits..: " + uservisitsCountResult);
        }
        // Definimos la lista de atributos.
        List<StructField> rankingFields = new ArrayList<>();
        // Para cada uno de los atributos especificamos el nombre y el tipo de 
        // dato.
        rankingFields.add(DataType.createStructField(Ranking.PAGE_URL_KEY, DataType.StringType, true));
        rankingFields.add(DataType.createStructField(Ranking.PAGE_RANK_KEY, DataType.IntegerType, true));
        rankingFields.add(DataType.createStructField(Ranking.AVG_DURATION_KEY, DataType.IntegerType, true));
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
        userVisitsFields.add(DataType.createStructField(UserVisit.SOURCE_IP_KEY, DataType.StringType, true));
        userVisitsFields.add(DataType.createStructField(UserVisit.DEST_URL_KEY, DataType.StringType, true));
        userVisitsFields.add(DataType.createStructField(UserVisit.VISIT_DATE_KEY, DataType.TimestampType, true));
        userVisitsFields.add(DataType.createStructField(UserVisit.AD_REVENUE_KEY, DataType.FloatType, true));
        userVisitsFields.add(DataType.createStructField(UserVisit.USER_AGENTL_KEY, DataType.StringType, true));
        userVisitsFields.add(DataType.createStructField(UserVisit.COUNTRY_CODE_KEY, DataType.StringType, true));
        userVisitsFields.add(DataType.createStructField(UserVisit.LANGUAGE_CODE_KEY, DataType.StringType, true));
        userVisitsFields.add(DataType.createStructField(UserVisit.SEARCH_WORD_KEY, DataType.StringType, true));
        userVisitsFields.add(DataType.createStructField(UserVisit.DURATION_KEY, DataType.IntegerType, true));
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
        // Aplicamos el esquema que hemos creado a las lineas que hemos creado en
        // el paso anterior..
        JavaSchemaRDD rankingSchemaRDD = sqlCtx.applySchema(rankingRowRDD, rankingSchema);
        JavaSchemaRDD userVisitsSchemaRDD = sqlCtx.applySchema(userVisitsRowRDD, userVisitsSchema);
        // Registramos la tabla rankings
        rankingSchemaRDD.registerTempTable("rankings");
        userVisitsSchemaRDD.registerTempTable("uservisits");
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
        Long startTime;
        Long endTime;
        Long tmpRunTime = (long) 0;
        JavaSchemaRDD results = null;
        // Repetimos la ejecucion de la query tantas veces como sea necesario.        
        for (int i = 0; i < NUM_TRIALS; i++) {
            // Medimos el timepo de inicio del experimento.
            startTime = System.currentTimeMillis();
            //  Lanzamos la query
            results = sqlCtx.sql(this.getJoinSelectQuery("2010-01-01 00:00:00.000"));
            // Medimos el tiempo de finalizacion del experimento.
            endTime = System.currentTimeMillis();
            // Sumamos el tiempo de la iteracion actual
            tmpRunTime += endTime - startTime;
        }
        // Calculamos el runTime del experimento actual dividiendo la suma de los
        // tiempos parciales entre el numero de iteraciones.
        tmpRunTime = tmpRunTime / NUM_TRIALS;
        // Seteamos el resultado del tiempo calculado.
        setRunTime(tmpRunTime);
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
                return "ip..: " + row.getString(0) + " - totalRevenue..: "
                        + row.getDouble(1) + " - pageRank..: " + row.getDouble(2);
            }
        }).collect();
        // Sacamos por pantalla los resultados de la query
        for (String name : names) {
            System.out.println(name);
        }
    }
}
