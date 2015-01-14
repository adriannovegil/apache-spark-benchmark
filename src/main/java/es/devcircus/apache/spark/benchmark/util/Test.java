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

public abstract class Test implements Serializable {

    protected static String BASE_DATA_PATH = null;
    protected static Boolean VERBOSE_MODE = null;
    protected static Integer NUM_TRIALS = null;

    static {
        // Directorio base de los ficheros del benchmark.
        // Lo componemos a partir del directorio base configurado en el fichero
        // de configuracion del benchmark, el tipo de compresion y el tamanho del
        // experimento.
        BASE_DATA_PATH = ConfigurationManager.get("apache.benchmark.config.sql.global.data.base.dir") + "/"
                + ConfigurationManager.get("apache.benchmark.config.sql.global.data.compression.type") + "/"
                + ConfigurationManager.get("apache.benchmark.config.sql.global.data.size");
        // Modo verbose.
        VERBOSE_MODE = (ConfigurationManager.get("apache.benchmark.config.global.verbose").equals("1"));
        // Numero de repeticiones de los experimentos
        NUM_TRIALS = new Integer(ConfigurationManager.get("apache.benchmark.config.global.num.trials"));
    }

    public abstract Boolean prepare();

    public abstract Boolean execute();

    public abstract Boolean commit();

    public abstract Boolean close();
}
