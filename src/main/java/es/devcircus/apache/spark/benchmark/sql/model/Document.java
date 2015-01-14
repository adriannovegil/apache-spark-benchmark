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
public class Document implements Serializable {

    public static final String LINE_KEY = "line";
    private String line;

    /**
     * Constructor por defecto de la clase.
     */
    public Document() {
    }

    /**
     * Constructor de la clase. Se le pasan como parametros los valores de los
     * atributos de la instancia que queremos crear.
     *
     * @param line Linea leida del documento del benchmark.
     */
    public Document(String line) {
        this.line = line;
    }

    /**
     *
     * @return
     */
    public String getLine() {
        return line;
    }

    /**
     *
     * @param line
     */
    public void setLine(String line) {
        this.line = line;
    }

}
