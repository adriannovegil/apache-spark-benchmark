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

import es.devcircus.apache.spark.benchmark.util.config.ConfigurationManager;
import es.devcircus.apache.spark.benchmark.util.sql.SQLTest;

/**
 *
 * @author Adrian Novegil <adrian.novegil@gmail.com>
 */
public abstract class Query04Test extends SQLTest {

    // Path al script python usado en el experimento.
    private static final String urlCountPythonScriptPath;

    // Cargamos la ruta al script python necesario para el test.
    static {
        // Recuperamos el path absoluto hasta el script auxiliar que usamos para
        // procesar los datos.        
        urlCountPythonScriptPath
                = ConfigurationManager.get("apache.benchmark.config.sql.query.04.hive.url.count.python.script.path");
    }

    /**
     * Query que hace el borrado de la tabla documents.
     *
     * @return String que contiene la query que hace el borrado de la tabla
     * documents
     */
    protected String getDropDocumentsTableQuery() {
        // Retornamos la query compuesta.
        return "DROP TABLE IF EXISTS documents";
    }

    /**
     * Query que crea la tabla documents. Crea la tabla y carga los datos en la
     * misma.
     *
     * @return String que contiene la query que crea la tabla documents.
     */
    protected String getCreateDocumentsTableQuery() {
        // Retornamos la query compuesta.
        return "CREATE EXTERNAL TABLE documents (line STRING) "
                + "STORED AS TEXTFILE LOCATION '" + BASE_DATA_PATH + "/crawl'";
    }

    /**
     * Query que hace el borrado de la tabla url_counts_partial.
     *
     * @return String que contiene la query que hace el borrado de la tabla
     * url_counts_partial
     */
    protected String getDropUrlCountsPartialTableQuery() {
        // Retornamos la query compuesta.
        return "DROP TABLE IF EXISTS url_counts_partial";
    }

    /**
     * Query que crea la tabla url_counts_partial. Crea la tabla y carga los
     * datos en la misma.
     *
     * @return String que contiene la query que crea la tabla
     * url_counts_partial.
     */
    protected String getCreateUrlCountsPartialTableQuery() {
        // Retornamos la query compuesta.
        return "CREATE TABLE url_counts_partial AS"
                + " SELECT TRANSFORM (line)"
                + " USING 'python " + urlCountPythonScriptPath + "' as (sourcePage,"
                + " destPage, count) from documents";
    }

    /**
     * Query de prueba que ejecutamos una vez que se ha terminado de realizar el
     * test. Con el resultado de esta query verificamos si se ha ejecutado
     * correctamente o no el test.
     *
     * @return String que contiene la query final a ejecutar.
     */
    protected String getSelectTableQuery() {
        // Retornamos la query compuesta.
        return "SELECT * FROM url_counts_partial";
    }
}
