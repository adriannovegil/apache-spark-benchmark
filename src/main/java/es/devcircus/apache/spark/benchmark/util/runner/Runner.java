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

import es.devcircus.apache.spark.benchmark.util.FileHelper;
import es.devcircus.apache.spark.benchmark.util.FormatHelper;
import es.devcircus.apache.spark.benchmark.util.Test;
import es.devcircus.apache.spark.benchmark.util.sql.SQLTest;
import java.util.Date;

/**
 *
 * @author Adrian Novegil <adrian.novegil@gmail.com>
 */
public final class Runner {

    /**
     * Runs a specified test on a specified persistence unit.
     *
     * @param args see usage message below
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {
        // Verificamos la lista de argumentos de entrada del test.
        if (args.length < 1 || args.length > 2) {
            System.err.println("Usage: es.devcircus.apache.spark.benchmark.util.runner.Runner " + "<test-class>" + "<test-value>");
            System.exit(1);
        }
        // Recuperamos el primer argumento que se corresonde con la clase del test
        // que queremos ejecutar.
        String mainClass = args[0];
        // Construimos la clase del test que queremos ejecutar.       
        SQLTest test = (SQLTest) Class.forName(mainClass).newInstance();
        if (args.length == 2) {
            // Valor de pruebas, si procede.
            String testValue = args[1];
            // Seteamos el valor del test.
            test.setTestValue(testValue);
        }
        // Ejecutamos el test.
        try {
            new Runner(test).run();
        } catch (Exception e) {
        }
    }

    // El test que queremos ejecutar.
    private final SQLTest test;
    // Tiempo que ha tardado la configuracion.
    private Long configTime;
    // tiempo que ha tardado la preparacion del experimento.
    private Long prepareTime;
    // Tiempo que ha tardado la ejecucion del experimento.
    private Long executeTime;

    /**
     * Construimos la instancia del ejecutor.
     *
     * @param test Instancia de test que queremos ejecutar.
     */
    private Runner(SQLTest test) {
        // Preparamos los atributos necesarios para la ejecución.
        this.test = test;
    }

    /**
     * Ejecuta todos los pasos del test, toma tiempos, toma constancia de
     * resultados, etc.
     */
    private void run() {
        Long startTime;
        Long endTime;
        // Medimos el timepo de inicio del metodo de configuracion.
        startTime = System.nanoTime();
        // Ejecutamos el metodo de configuracion del test.     
        this.test.config();
        // Medimos el tiempo de finalizacion del metodo de configuracion.
        endTime = System.nanoTime();
        // Seteamos el tiempo de configuracion.
        this.configTime = endTime - startTime;
        // Medimos el timepo de inicio del metodo de preapracion.
        startTime = System.nanoTime();
        this.test.prepare();
        // Medimos el tiempo de finalizacion del metodo de preparacion.
        endTime = System.nanoTime();
        // Seteamos el tiempo de preaparacion.
        this.prepareTime = endTime - startTime;
        // Ejcutamos el kernel computacional
        Long tmpRunTime = (long) 0;
        // Repetimos la ejecucion de la query tantas veces como sea necesario.        
        for (int i = 0; i < SQLTest.NUM_TRIALS; i++) {
            // Medimos el timepo de inicio del experimento.
            startTime = System.nanoTime();
            // Ejecutamos el core del benchmark.
            this.test.execute();
            // Medimos el tiempo de finalizacion del experimento.
            endTime = System.nanoTime();
            // Sumamos el tiempo de la iteracion actual
            tmpRunTime += endTime - startTime;
        }
        // Calculamos el runTime del experimento actual dividiendo la suma de los
        // tiempos parciales entre el numero de iteraciones.
        this.executeTime = tmpRunTime / Test.NUM_TRIALS;
        // Cerramos el test.
        this.test.close();
        // Anhadimos en el fichero de resultados los datos de la ejecucion.
        this.reportResult();
    }

    /**
     * Writes result line for a specified action.
     *
     * @param result one of: "started", result number or exception string
     */
    private void reportResult() {
        String TIME_STAMP_STRING = new String("[" + FormatHelper.formatTime(new Date()) + "]");
        // Fecha - Hora - Test - Tiempo configuracion \t - Tiempo carga - Tiempo ejecucion - Tiempo total
        // String builder con el que haremos la concatenacion de los datos de salida.
        StringBuilder sb = new StringBuilder(256);
        // Anhadimos a la salida el nombre del test.        
        sb.append(TIME_STAMP_STRING).append(" Test          : ").append(test.getName()).append("\n");
        // Anhadimos el numero de iteraciones del test
        sb.append(TIME_STAMP_STRING).append(" Trials        : ").append(test.NUM_TRIALS).append('\n');
        // Anhadimos el numero de iteraciones del test
        sb.append(TIME_STAMP_STRING).append(" Test Value    : ").append(test.getTestValue()).append('\n');
        // Tiempo de configuracion
        sb.append(TIME_STAMP_STRING).append(" Times \n");
        sb.append(TIME_STAMP_STRING).append("  - Config     : ").append(this.getSecondsFromNanoSeconds(
                this.configTime)).append("\n");
        // Tiempo de preparacion del test.
        sb.append(TIME_STAMP_STRING).append("  - Prepare    : ").append(this.getSecondsFromNanoSeconds(
                this.prepareTime)).append("\n");
        // Tiempo de ejecucuón del test.
        sb.append(TIME_STAMP_STRING).append("  - Execution  : ").append(this.getSecondsFromNanoSeconds(
                this.executeTime));
        // Escribimos en el fichero de salida la linea de log.
        FileHelper.writeTextLine(sb.toString(), FileHelper.RESULT_FILE);
//        // Fecha - Hora - Test - Tiempo configuracion \t - Tiempo carga - Tiempo ejecucion - Tiempo total
//        // String builder con el que haremos la concatenacion de los datos de salida.
//        StringBuilder sb = new StringBuilder(256);
//        // Anhadimos a la salida la fecha y la hora.
//        sb.append("[").append(FormatHelper.formatTime(new Date())).append("]").append(' ');
//        // Anhadimos a la salida el nombre del test.
//        sb.append(test.getName()).append(",").append("\t");
//        // Anhadimos el numero de iteraciones del test
//        sb.append(" Trials: ").append(test.NUM_TRIALS).append(',').append(" ");
//        // Tiempo de configuracion
//        sb.append(" Times: Config = ").append(this.getSecondsFromNanoSeconds(
//                this.configTime)).append(' ');
//        // Tiempo de preparacion del test.
//        sb.append(" Prepare = ").append(this.getSecondsFromNanoSeconds(
//                this.prepareTime)).append(' ');
//        // Tiempo de ejecucuón del test.
//        sb.append(" Execution = ").append(this.getSecondsFromNanoSeconds(
//                this.executeTime)).append(' ');
//        // Escribimos en el fichero de salida la linea de log.
//        FileHelper.writeTextLine(sb.toString(), FileHelper.RESULT_FILE);
    }

    /**
     *
     * @param nanoSeconds
     * @return
     */
    private double getSecondsFromNanoSeconds(Long nanoSeconds) {
        return (double) nanoSeconds / 1000000000.0;
    }
}
