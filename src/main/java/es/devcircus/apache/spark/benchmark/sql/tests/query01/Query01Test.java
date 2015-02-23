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

import es.devcircus.apache.spark.benchmark.util.sql.SQLTest;

/**
 *
 * @author Adrian Novegil <adrian.novegil@gmail.com>
 */
public abstract class Query01Test extends SQLTest {
    
    /**
     * Query que hace el borrado de la tabla rankings.
     *
     * @return String que contiene la query que hace el borrado de la tabla
     * rankings
     */
    protected String getDropRankingsTableQuery() {
        // Retornamos la query compuesta.
        return "DROP TABLE IF EXISTS rankings";
    }

    /**
     * Query que crea la tabla rankings. Crea la tabla y carga los datos en la
     * misma.
     *
     * @return String que contiene la query que crea la tabla rankings.
     */
    protected String getCreateRankingsTableQuery() {
        // Retornamos la query compuesta.
        return "CREATE EXTERNAL TABLE IF NOT EXISTS rankings (pageURL STRING, pageRank INT,"
                + " avgDuration INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
                + " STORED AS TEXTFILE LOCATION '" + BASE_DATA_PATH + "/rankings'";
//        return "CREATE TABLE IF NOT EXISTS rankings (pageURL STRING, pageRank INT,"
//                + " avgDuration INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
//                + " STORED AS TEXTFILE LOCATION '" + BASE_DATA_PATH + "/rankings'";
    }

    /**
     * Query que ejecutamos en el test.
     *
     * @param pageRankValue Valor que queremos asignar a la query.
     * @return String que contiene la query final a ejecutar.
     */
    protected String getPageRankValueSelectQuery(int pageRankValue) {
        // Retornamos la query compuesta.
        return "SELECT pageURL, pageRank "
                + "FROM rankings "
                + "WHERE pageRank > " + pageRankValue;
    }
}
