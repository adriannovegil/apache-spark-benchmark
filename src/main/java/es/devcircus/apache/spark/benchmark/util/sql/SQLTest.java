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
package es.devcircus.apache.spark.benchmark.util.sql;

import es.devcircus.apache.spark.benchmark.util.Test;
import es.devcircus.apache.spark.benchmark.util.config.ConfigurationManager;

/**
 *
 * @author Adrian Novegil <adrian.novegil@gmail.com>
 */
public abstract class SQLTest extends Test {

    // Variable que contiene el valor del test
    private String testValue;

    /**
     * Metodo que nos permite recuperar el valor del atributo testValue.
     *
     * @return Valor actual del atributo testValue.
     */
    public String getTestValue() {
        return testValue;
    }

    /**
     * Metodo que nos permite modificar el valor del atributo testValue.
     *
     * @param testValue Nueva valor del atributo testValue.
     */
    public void setTestValue(String testValue) {
        this.testValue = testValue;
    }

    /**
     * Path al directorio base donde se encuentran los datos del experimento.
     */
    protected static String BASE_DATA_PATH = null;

    static {
        // Directorio base de los ficheros del benchmark.
        // Lo componemos a partir del directorio base configurado en el fichero
        // de configuracion del benchmark, el tipo de compresion y el tamanho del
        // experimento.
        BASE_DATA_PATH = ConfigurationManager.get("apache.benchmark.config.sql.global.data.base.dir") + "/"
                + ConfigurationManager.get("apache.benchmark.config.sql.global.data.compression.type") + "/"
                + ConfigurationManager.get("apache.benchmark.config.sql.global.data.size");
    }

    /**
     * Metodo que se encarga de la preparacion de los datos para el experimento.
     *
     * @return True si la preparacion ha finalizado con exito. False en caso
     * contrario.
     */
    public abstract Boolean prepare();

}
