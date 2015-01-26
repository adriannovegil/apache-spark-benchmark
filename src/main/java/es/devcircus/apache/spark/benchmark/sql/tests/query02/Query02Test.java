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

import es.devcircus.apache.spark.benchmark.util.sql.SQLTest;

/**
 *
 * @author Adrian Novegil <adrian.novegil@gmail.com>
 */
public abstract class Query02Test extends SQLTest {

    /**
     * Query que hace el borrado de la tabla uservisits.
     *
     * @return String que contiene la query que hace el borrado de la tabla
     * uservisits
     */
    protected String getDropUservisitsTableQuery() {
        // Retornamos la query compuesta.
        return "DROP TABLE IF EXISTS uservisits";
    }

    /**
     * Query que crea la tabla uservisits. Crea la tabla y carga los datos en la
     * misma.
     *
     * @return String que contiene la query que crea la tabla uservisits.
     */
    protected String getCreateUservisitsTableQuery() {
        // Retornamos la query compuesta.
        return "CREATE EXTERNAL TABLE uservisits (sourceIP STRING,destURL STRING,"
                + " visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,"
                + " languageCode STRING,searchWord STRING,duration INT )"
                + " ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
                + " STORED AS TEXTFILE LOCATION '" + BASE_DATA_PATH + "/uservisits'";
    }

    /**
     * Query que ejecutamos en el test.
     *
     * @param topValue Valor que queremos asignar a la query.
     * @return String que contiene la query final a ejecutar.
     */
    protected String getTopValueSelectQuery(int topValue) {
        // Retornamos la query compuesta.
        return "SELECT SUBSTR(sourceIP, 1, " + topValue + "), SUM(adRevenue) "
                + "FROM uservisits "
                + "GROUP BY SUBSTR(sourceIP, 1, " + topValue + ")";
    }
}
