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

import java.sql.Timestamp;

/**
 *
 * @author Adrian Novegil <adrian.novegil@gmail.com>
 */
public class UserVisit {

    private String sourceIP;
    private String destURL;
    private Timestamp visitDate;
    private Float adRevenue;
    private String userAgent;
    private String countryCode;
    private String languageCode;
    private String searchWord;
    private Integer duration;

    /**
     * Constructor por defecto de la clase.
     */
    public UserVisit() {
    }

    /**
     * Constructor de la clase. Se le pasan como parametros los valores de los
     * atributos de la instancia que queremos crear.
     * @param sourceIP Direccion ip de origen.
     * @param destURL URL de destino.
     * @param visitDate Fecha de la solicitud.
     * @param adRevenue 
     * @param userAgent 
     * @param countryCode Codigo identificativo del pais desde el que se hizo la
     * solicitud.
     * @param languageCode Codigo de idioma.
     * @param searchWord Palabra buscada.
     * @param duration Durancion.
     */
    public UserVisit(String sourceIP, String destURL, Timestamp visitDate,
            Float adRevenue, String userAgent, String countryCode,
            String languageCode, String searchWord, Integer duration) {
        this.sourceIP = sourceIP;
        this.destURL = destURL;
        this.visitDate = visitDate;
        this.adRevenue = adRevenue;
        this.userAgent = userAgent;
        this.countryCode = countryCode;
        this.languageCode = languageCode;
        this.searchWord = searchWord;
        this.duration = duration;
    }

    /**
     *
     * @return
     */
    public String getSourceIP() {
        return sourceIP;
    }

    /**
     *
     * @param sourceIP
     */
    public void setSourceIP(String sourceIP) {
        this.sourceIP = sourceIP;
    }

    /**
     *
     * @return
     */
    public String getDestURL() {
        return destURL;
    }

    /**
     *
     * @param destURL
     */
    public void setDestURL(String destURL) {
        this.destURL = destURL;
    }

    /**
     *
     * @return
     */
    public Timestamp getVisitDate() {
        return visitDate;
    }

    /**
     *
     * @param visitDate
     */
    public void setVisitDate(Timestamp visitDate) {
        this.visitDate = visitDate;
    }

    /**
     *
     * @return
     */
    public Float getAdRevenue() {
        return adRevenue;
    }

    /**
     *
     * @param adRevenue
     */
    public void setAdRevenue(Float adRevenue) {
        this.adRevenue = adRevenue;
    }

    /**
     *
     * @return
     */
    public String getUserAgent() {
        return userAgent;
    }

    /**
     *
     * @param userAgent
     */
    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    /**
     *
     * @return
     */
    public String getCountryCode() {
        return countryCode;
    }

    /**
     *
     * @param countryCode
     */
    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }

    /**
     *
     * @return
     */
    public String getLanguageCode() {
        return languageCode;
    }

    /**
     *
     * @param languageCode
     */
    public void setLanguageCode(String languageCode) {
        this.languageCode = languageCode;
    }

    /**
     *
     * @return
     */
    public String getSearchWord() {
        return searchWord;
    }

    /**
     *
     * @param searchWord
     */
    public void setSearchWord(String searchWord) {
        this.searchWord = searchWord;
    }

    /**
     *
     * @return
     */
    public Integer getDuration() {
        return duration;
    }

    /**
     *
     * @param duration
     */
    public void setDuration(Integer duration) {
        this.duration = duration;
    }

}
