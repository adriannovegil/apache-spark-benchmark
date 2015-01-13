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
package es.devcircus.apache.spark.benchmark.util.config.watchdog;

import es.devcircus.apache.spark.benchmark.util.config.ConfigurationManager;
import java.io.File;

/**
 *
 * @author Adrian Novegil <adrian.novegil@gmail.com>
 */
public abstract class FileWatchdog extends Thread {

    /**
     * Delay por defecto que aplica el WatchDog entre revisiones del fichero de
     * configuración.
     */
    static final public long DEFAULT_DELAY = 60000;
    /**
     * Nombre del fichero que queremos observar
     */
    protected String filename;
    /**
     * The delay to observe between every check. By default set {@link
     * #DEFAULT_DELAY}.
     */
    private long delay = DEFAULT_DELAY;
    /**
     * Variable que apunta al fichero de configuración.
     */
    private File file;
    /**
     * Fecha de la última modificación. Atributo empleado para ver si el fichero
     * ha sido modificado o no.
     */
    private long lastModif = 0;
    private boolean warnedAlready = false;
    private boolean interrupted = false;

    /**
     * Contructor que instancia
     *
     * @param filename
     */
    protected FileWatchdog(String configFileName) {
        super("FileWatchdog");
//        try {
        this.filename = configFileName;
//            URL resourceUrl = new URL("file:///" + ConfigurationManager.BASE_CONFIG_DIR + "/" + filename);
        //        this.filename = configFileName;
//            try {
//                file = new File(resourceUrl.toURI());
        file = new File(ConfigurationManager.BASE_CONFIG_DIR + "/" + filename);
        setDaemon(true);
        checkAndConfigure();
//            } catch (URISyntaxException ex) {
//                Logger.getLogger(FileWatchdog.class.getName()).log(Level.SEVERE, null, ex);
//            }
//        } catch (MalformedURLException ex) {
//            Logger.getLogger(FileWatchdog.class.getName()).log(Level.SEVERE, null, ex);
//        }
    }

    /**
     * Recupera el valor de delay que tenemos asignado al FileWatchDog
     *
     * @return valor del delay asignado al FileWatchDog
     */
    public long getDelay() {
        return delay;
    }

    /**
     * Asigna el valor de delay que tenemos asignado al FileWatchDog
     *
     * @param delay valor del delay asignado al FileWatchDog
     */
    public void setDelay(long delay) {
        this.delay = delay;
    }

    /**
     * Método abstracto que se ejecuta en el caso de detectar cambios en el
     * fichero.
     */
    abstract protected void doOnChange();

    /**
     * Método que se encarfa de verificar la configuración del fichero en cada
     * momento.
     */
    protected final void checkAndConfigure() {
        boolean fileExists;
        try {
            fileExists = file.exists();
        } catch (SecurityException e) {
            interrupted = true; // there is no point in continuing
            return;
        }
        if (fileExists) {
            long l = file.lastModified(); // this can also throw a SecurityException
            if (l > lastModif) {           // however, if we reached this point this
                lastModif = l;              // is very unlikely.
                doOnChange();
                warnedAlready = false;
            }
        } else {
            if (!warnedAlready) {
                warnedAlready = true;
            }
        }
    }

    /**
     * Thread que se encarga
     */
    @Override
    public void run() {
        while (!interrupted) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                // no interruption expected
            }
            //Revisamos el fichero de propiedades para ver si se ha producido
            //algún cambio.
            checkAndConfigure();
        }
    }
}
