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
package es.devcircus.apache.spark.benchmark.util.runner;

import es.devcircus.apache.spark.benchmark.util.Test;
import es.devcircus.apache.spark.benchmark.util.sql.SQLTest;

/**
 *
 * @author Adrian Novegil <adrian.novegil@gmail.com>
 */
public final class BenchmarkExecutor {

    /**
     * Consturctor por defecto.
     */
    private BenchmarkExecutor() {
    }

    /**
     *
     * @param test
     * @return
     */    
    public static Boolean process(Test test) {
        test.config();
        test.execute();
        test.close();
        return true;
    }

    /**
     *
     * @param test
     * @return
     */
    public static Boolean process(SQLTest test) {
        test.config();
        test.prepare();
        test.execute();
        test.close();
        return true;
    }
}
