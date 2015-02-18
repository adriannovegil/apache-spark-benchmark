package es.devcircus.apache.spark.benchmark.util.runner;

import es.devcircus.apache.spark.benchmark.util.FileHelper;
import es.devcircus.apache.spark.benchmark.util.FormatHelper;
import es.devcircus.apache.spark.benchmark.util.sql.SQLTest;
import java.util.Date;

/**
 * Runner of one benchmark test using one persistence unit. Note: Invoked
 * indirectly by Main (using Launcher).
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
        if (args.length != 1) {
            System.err.println("Usage: es.devcircus.apache.spark.benchmark.util.runner.Runner " + "<test-class>");
            System.exit(1);
        }
        // Recuperamos el primer argumento que se corresonde con la clase del test
        // que queremos ejecutar.
        String mainClass = args[0];
        // Construimos la clase del test que queremos ejecutar.       
        SQLTest test = (SQLTest) Class.forName(mainClass).newInstance();
        // Ejecutamos el test.
        try {
            new Runner(test).run();
        } catch (Exception e) {
        }
    }

    /**
     * The test to be run
     */
    private final SQLTest test;

    /**
     * Currently tested action
     */
    private String actionName;

    private Long configTime;
    private Long prepareTime;
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
        startTime = System.currentTimeMillis();
        // Ejecutamos el metodo de configuracion del test.     
        this.test.config();
        // Medimos el tiempo de finalizacion del metodo de configuracion.
        endTime = System.currentTimeMillis();
        // Seteamos el tiempo de configuracion.
        this.configTime = endTime - startTime;
        // Medimos el timepo de inicio del metodo de preapracion.
        startTime = System.currentTimeMillis();
        this.test.prepare();
        // Medimos el tiempo de finalizacion del metodo de preparacion.
        endTime = System.currentTimeMillis();
        // Seteamos el tiempo de preaparacion.
        this.prepareTime = endTime - startTime;
        // Ejcutamos el kernel computacional
        Long tmpRunTime = (long) 0;
        // Repetimos la ejecucion de la query tantas veces como sea necesario.        
        for (int i = 0; i < SQLTest.NUM_TRIALS; i++) {
            // Medimos el timepo de inicio del experimento.
            startTime = System.currentTimeMillis();
            // Ejecutamos el core del benchmark.
            this.test.execute();
            // Medimos el tiempo de finalizacion del experimento.
            endTime = System.currentTimeMillis();
            // Sumamos el tiempo de la iteracion actual
            tmpRunTime += endTime - startTime;
        }
        // Calculamos el runTime del experimento actual dividiendo la suma de los
        // tiempos parciales entre el numero de iteraciones.
        this.executeTime = tmpRunTime / SQLTest.NUM_TRIALS;
        // Finalizamos el test.        
        this.test.commit();
        // Cerramos el test.
        this.test.close();
        // Anhadimos en el fichero de resultados los datos de la ejecucion.
        this.reportResult();
    }

    /**
     * Writes result lines for ALL the actions.
     *
     * @param result "started", result number or exception string
     */
//    private void reportResult(Object result) {
//        reportResult(result, "Persist");
//        reportResult(result, "Retrieve");
//        reportResult(result, "Update");
//        reportResult(result, "Remove");
//        reportResult(result, "Space");
//    }
    /**
     * Writes result line for a specified action.
     *
     * @param result one of: "started", result number or exception string
     * @param actionName the name of the action
     */
    private void reportResult() {

        // Fecha - Hora - Test - Tiempo configuracion \t - Tiempo carga - Tiempo ejecucion - Tiempo total
        // String builder con el que haremos la concatenacion de los datos de salida.
        StringBuilder sb = new StringBuilder(256);
        // Anhadimos a la salida la fecha y la hora.
        sb.append(FormatHelper.formatTime(new Date())).append(' ');
        // Anhadimos a la salida el nombre del test.
        sb.append(test.getName()).append(' ');
        
        // Tiempo de configuracion
        sb.append(this.configTime).append(' ');
        // Tiempo de preparacion del test.
        sb.append(this.prepareTime).append(' ');
        // Tiempo de ejecucuón del test.
        sb.append(this.executeTime).append(' ');        
        
//        sb.append(test.getThreadCount()).append(' ');
//        sb.append(test.getBatchSize()).append(' ');
//        sb.append(totalObjectCount).append(' ');
        // Anhadimos a la salida la informacion que hemos pasado como parametro.
        sb.append(actionName).append(' ');
        
        // Escribimos en el fichero de salida la linea de log.
        FileHelper.writeTextLine(sb.toString(), FileHelper.RESULT_FILE);
    }
}
