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
package es.devcircus.apache.spark.benchmark.util;

import es.devcircus.apache.spark.benchmark.util.config.ConfigurationManager;
import java.io.Serializable;

/**
 *
 * @author Adrian Novegil <adrian.novegil@gmail.com>
 */
public abstract class Test implements Serializable {

    private String name;
    public static Boolean VERBOSE_MODE = null;
    public static Integer NUM_TRIALS = null;
    
    static {
        // Modo verbose.
        VERBOSE_MODE = (ConfigurationManager.get("apache.benchmark.config.global.verbose").equals("1"));
        // Numero de repeticiones de los experimentos
        NUM_TRIALS = new Integer(ConfigurationManager.get("apache.benchmark.config.global.num.trials"));
    }

    /**
     * Metodo que nos permite recuperar el valor del atributo name.
     *
     * @return Valor del atributo name.
     */
    public String getName() {
        return name;
    }

    /**
     * Metodo que nos permite setear el valor del atributo name.
     *
     * @param name Nuevo valor del atributo name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     *
     * @return
     */
    public abstract Boolean config();

    /**
     *
     * @return
     */
    public abstract Boolean execute();

    /**
     *
     * @return
     */
    public abstract Boolean close();
}
