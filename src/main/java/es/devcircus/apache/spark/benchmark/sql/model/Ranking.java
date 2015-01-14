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
package es.devcircus.apache.spark.benchmark.sql.model;

import java.io.Serializable;

/**
 *
 * @author Adrian Novegil <adrian.novegil@gmail.com>
 */
public class Ranking implements Serializable {

    public static final String PAGE_URL_KEY = "pageURL";
    private String pageURL;

    public static final String PAGE_RANK_KEY = "pageRank";
    private Integer pageRank;

    public static final String AVG_DURATION_KEY = "avgDuration";
    private Integer avgDuration;

    /**
     * Constructor por defecto de la clase.
     */
    public Ranking() {
    }

    /**
     * Constructor de la clase. Se le pasan como parametros los valores de los
     * atributos de la instancia que queremos crear.
     *
     * @param pageURL URL de la pagina.
     * @param pageRank Ranking de la pagina.
     * @param avgDuration Tiempo medio.
     */
    public Ranking(String pageURL, Integer pageRank, Integer avgDuration) {
        this.pageURL = pageURL;
        this.pageRank = pageRank;
        this.avgDuration = avgDuration;
    }

    /**
     *
     * @return
     */
    public String getPageURL() {
        return pageURL;
    }

    /**
     *
     * @param pageURL
     */
    public void setPageURL(String pageURL) {
        this.pageURL = pageURL;
    }

    /**
     *
     * @return
     */
    public Integer getPageRank() {
        return pageRank;
    }

    /**
     *
     * @param pageRank
     */
    public void setPageRank(Integer pageRank) {
        this.pageRank = pageRank;
    }

    /**
     *
     * @return
     */
    public Integer getAvgDuration() {
        return avgDuration;
    }

    /**
     *
     * @param avgDuration
     */
    public void setAvgDuration(Integer avgDuration) {
        this.avgDuration = avgDuration;
    }

}
