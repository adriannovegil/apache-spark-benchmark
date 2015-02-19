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

import java.io.*;
import java.util.*;

/**
 *
 * @author Adrian Novegil <adrian.novegil@gmail.com>
 */
public final class Launcher extends Thread {

    /**
     * All the active sub processes (for cleanup shutdown hook)
     */
    static HashSet<Process> processSet = new HashSet<>();

    /**
     * Paths to be added to the classpath
     */
    private final String[] jarPaths;

    private final String testClass;

    /**
     * Entry point class (with a main method)
     */
    private final Class mainClass;

    /**
     * Program arguments
     */
    private final List<String> argList;

    /**
     * The running sub process
     */
    private Process process;

    /**
     * Process exit code (0 - for success; 1 - for error)
     */
    private int exitCode;

    /**
     * Standard output message lines
     */
    private final List<String> outMsgList = new ArrayList<>(8);

    /**
     * Standard error message lines
     */
    private final List<String> errMsgList = new ArrayList<>(8);

    /**
     * Constructs a Launcher instance.
     *
     * @param testClass Clase del test que queremos ejecutar.
     * @param mainClass the entry point class (with a main method)
     * @param argList list of program arguments
     * @param jarPaths paths to be added to the classpath
     */
    public Launcher(String testClass, Class mainClass, List<String> argList, String[] jarPaths) {
        this.testClass = testClass;
        this.jarPaths = jarPaths;
        this.mainClass = mainClass;
        this.argList = argList;
    }

    /**
     * Runs the JVM as an external process.
     */
    @Override
    public void run() {
        try {
            // Path al home de java.
            File jvmFile = new File(new File(
                    System.getProperty("java.home"), "bin"), "java");

//            // Comando a ejecutar.
//            List<String> cmdList = new ArrayList<>(64);
//            // Componemos el comando a ejecutar.
//            cmdList.add("mvn ");
//            cmdList.add("\"-Dexec.args=");
//            cmdList.add("-classpath %classpath es.devcircus.apache.spark.benchmark.util.runner.Runner ");
//            cmdList.add(testClass + "\" ");
//            cmdList.add("-Dexec.executable=" + jvmFile.getPath());
//            cmdList.add(" org.codehaus.mojo:exec-maven-plugin:1.2.1:exec");
//            String[] cmd = cmdList.toArray(new String[0]);
//            // Lanzamos la ejecucion del comando del test.
//            process = Runtime.getRuntime().exec(cmd, null);

            process = Runtime.getRuntime().exec(
                    "mvn "
                    + "\"-Dexec.args="
                    + "-classpath %classpath es.devcircus.apache.spark.benchmark.util.runner.Runner "
                    + testClass + "\" "
                    + "-Dexec.executable=" + jvmFile.getPath()
                    + " org.codehaus.mojo:exec-maven-plugin:1.2.1:exec", null);
            // Start collecting standard output and error:
            // Estas clases se encargan de recoger la salida estandar y la salida
            // de error del thread en el que vamos a ejecutar el test.
            MessageCollector errorListener
                    = new MessageCollector(process.getErrorStream(), errMsgList);
            errorListener.start();
            MessageCollector outputListener
                    = new MessageCollector(process.getInputStream(), outMsgList);
            outputListener.start();
            // Esperamos a que termine el test.:
            processSet.add(process);
            exitCode = process.waitFor();
            processSet.remove(process);
            process = null;
            errorListener.join();
            outputListener.join();
        } // Handle exceptions:
        catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Kills the subprocess.
     */
    public void kill() {
        if (process != null) {
            process.destroy();
        }
    }

    /**
     * Shutdown Hook for closing open processes.
     */
    static {
        Runtime.getRuntime().addShutdownHook(new Thread("Launcher-Cleanup") {
            @Override
            public void run() {
                for (Process process : Launcher.processSet) {
                    try {
                        process.destroy();
                    } catch (Throwable e) {
                    }
                }
            }
        }
        );
    }

    /**
     * Gets the standard output message.
     *
     * @return the standard output message.
     */
    public String getStdOutMessage() {
        StringBuilder sb = new StringBuilder(1024);
        for (String s : outMsgList) {
            sb.append(s).append(FormatHelper.NEW_LINE);
        }
        return sb.toString();
    }

    /**
     * Gets the standard error message.
     *
     * @return the standard error message.
     */
    public String getStdErrMessage() {
        StringBuilder sb = new StringBuilder(1024);
        for (String s : errMsgList) {
            sb.append(s).append(FormatHelper.NEW_LINE);
        }
        return sb.toString();
    }

    /**
     * The MessageCollector class collects external process message lines.
     */
    final static class MessageCollector extends Thread {

        // Data Members:
        /**
         * Input stream to be read (process standard output or error)
         */
        private final InputStream m_in;

        /**
         * List to be filled with collected message lines
         */
        private final List<String> m_messageList;

        // Construction:
        /**
         * Constructs a MessageCollector instance.
         *
         * @param in the input stream to read message lines from
         * @param messageList to be filled with collected message lines
         */
        MessageCollector(InputStream in, List<String> messageList) {
            this.m_in = in;
            m_messageList = messageList;
        }

        // Running:
        /**
         * Collects message lines from the input stream.
         */
        @Override
        public void run() {
            try {
                BufferedReader reader
                        = new BufferedReader(new InputStreamReader(m_in));
                String line;
                while ((line = reader.readLine()) != null) {
                    m_messageList.add(line);
                }
            } catch (IOException e) {
            }
        }
    }
}
