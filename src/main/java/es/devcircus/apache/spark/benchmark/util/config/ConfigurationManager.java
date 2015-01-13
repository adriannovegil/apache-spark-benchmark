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
package es.devcircus.apache.spark.benchmark.util.config;

import es.devcircus.apache.spark.benchmark.util.config.utils.PropertiesUtil;
import es.devcircus.apache.spark.benchmark.util.config.watchdog.FileWatchdog;
import es.devcircus.apache.spark.benchmark.util.config.watchdog.PropertyWatchdog;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.URLConnection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author Adrian Novegil <adrian.novegil@gmail.com>
 */
public class ConfigurationManager {

    private static final String BASE_APP_DIR_KEY = "base.config.app.url";
    private static final String BASE_CONFIG_DIR_KEY = "base.config.dir.name";
    private static final String BASE_CONFIG_FILE = "system.properties";
    public static String BASE_CONFIG_DIR;
    public static String BASE_APP_DIR;
    /**
     * Mapa que contiene la lista de propiedades y su valor.
     */
    private static Map<String, String> _properties = new ConcurrentHashMap<String, String>();

    /**
     * Cargamos el valor de la URL base donde se encuentra el directorio de
     * configuración de nuestra aplicación.
     */
    static {

        Properties props = new Properties();
        InputStream istream = null;

        try {
            //Abrimos un InputStream con la URL del 
            istream = ConfigurationManager.class.getResourceAsStream("/" + BASE_CONFIG_FILE);
            if (istream == null) {
                BASE_APP_DIR = "";
                BASE_CONFIG_DIR = "";
            } else {
                //Cargamos las properties a partir del InputStream
                props.load(istream);
                //Definimos el valor del directorio raiz de configuración
                BASE_CONFIG_DIR = props.getProperty(BASE_APP_DIR_KEY) + "/"
                        + props.getProperty(BASE_CONFIG_DIR_KEY);
                //Cargamos el resto de los parámetros de configuración en memoria.
                ConfigurationManager.loadConfigure(props);
                //Cerramos el InputStream
                istream.close();
            }
        } catch (Exception e) {
            if (e instanceof InterruptedIOException || e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        } finally {
            if (istream != null) {
                try {
                    istream.close();
                } catch (InterruptedIOException ignore) {
                    Thread.currentThread().interrupt();
                } catch (Throwable ignore) {
                }

            }
        }

    }

    /**
     *
     * @param configFilename
     */
    public static void configure(String configFilename) {
        ConfigurationManager.loadConfigure(configFilename);
    }

    /**
     *
     * @param configURL
     */
    public static void configure(java.net.URL configURL) {
        ConfigurationManager.loadConfigure(configURL);
    }

    /**
     *
     * @param inputStream
     */
    public static void configure(InputStream inputStream) {
        ConfigurationManager.loadConfigure(inputStream);
    }

    /**
     *
     * @param configFilename
     */
    public static void configureAndWatch(String configFilename) {
        configureAndWatch(configFilename, FileWatchdog.DEFAULT_DELAY);
    }

    /**
     *
     * @param configFilename
     * @param delay
     */
    public static void configureAndWatch(String configFilename, long delay) {
        //Instanciamos un nuevo objeto PropertyWatchdog que se encargará de la
        //supervisión del fichero
        PropertyWatchdog pdog = new PropertyWatchdog(configFilename);
        //Indicamos el dalay que se debe aplicar a la supervisión.
        pdog.setDelay(delay);
        //Lanzamos el thread.
        pdog.start();
    }

    /**
     *
     * @param properties
     * @param hierarchy
     */
    private static void loadConfigure(Properties properties) {
        //Añadimos las propiedades a la matriz de properties.
        PropertiesUtil.appendPropertiesToMap(properties, _properties);
    }

    /**
     *
     * @param inputStream
     */
    public static void loadConfigure(InputStream inputStream) {
        //Instanciamos un nuevo objeto de properties.
        Properties props = new Properties();
        try {
            props.load(inputStream);
        } catch (IOException e) {
            if (e instanceof InterruptedIOException) {
                Thread.currentThread().interrupt();
            }
            return;
        }
        //Cargamos las propiedades en la matriz.
        loadConfigure(props);
    }

    /**
     *
     * @param configFileName
     */
    public static void loadConfigure(String configFileName) {
        //Instanciamos un nuevo objeto de properties.
        Properties props = new Properties();
        InputStream istream = null;
        try {
            istream = new FileInputStream(BASE_CONFIG_DIR + "/" + configFileName);
            props.load(istream);
            istream.close();
        } catch (Exception e) {
            if (e instanceof InterruptedIOException || e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return;
        } finally {
            if (istream != null) {
                try {
                    istream.close();
                } catch (InterruptedIOException ignore) {
                    Thread.currentThread().interrupt();
                } catch (Throwable ignore) {
                }

            }
        }
        //Cargamos las propiedades en la matriz.
        loadConfigure(props);
    }

    /**
     *
     * @param configURL
     */
    public static void loadConfigure(java.net.URL configURL) {
        //Instanciamos un nuevo objeto de properties.
        Properties props = new Properties();
        InputStream istream = null;
        URLConnection uConn = null;
        try {
            uConn = configURL.openConnection();
            uConn.setUseCaches(false);
            istream = uConn.getInputStream();
            props.load(istream);
        } catch (Exception e) {
            if (e instanceof InterruptedIOException || e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return;
        } finally {
            if (istream != null) {
                try {
                    istream.close();
                } catch (InterruptedIOException ignore) {
                    Thread.currentThread().interrupt();
                } catch (IOException ignore) {
                } catch (RuntimeException ignore) {
                }
            }
        }
        //Cargamos las propiedades en la matriz.
        loadConfigure(props);
    }

    /**
     *
     * @return
     */
    public static Properties getProperties() {
        return PropertiesUtil.getPropertiesfromMap(ConfigurationManager._properties);
    }

    /**
     * Método que nos retorna el valor de un parámetro de configuración pasado
     * como parámetro.
     *
     * @param key parámetro de configuración que queremos recuperar.
     * @return cadena de texto que contiene el valor del parámetro que queremos
     * recuperar.
     */
    public static String get(String key) {
        if (ConfigurationManager._properties.containsKey(key)) {
            String value = ConfigurationManager._properties.get(key);
            return value;
        } else {
            return null;
        }
    }

    /**
     * Método que nos retorna el valor de un parámetro de configuración pasado
     * coo parámetro. Antes de retornarnos el valor, recarga aquellos ficheros
     * que están siendo supervisados.
     *
     * @param key parámetro de configuración que queremos recuperar.
     * @return cadena de texto que contiene el valor del parámetro que queremos
     * recuperar.
     */
    public static String reloadAnGet(String key) {
        String value = ConfigurationManager._properties.get(key);
        if (value == null) {
            value = System.getProperty(key);
        }
        return value;
    }

    /**
     * Método que nos permite actualizar el valor de un parámetro de
     * configuración dentro de la aplicación. Hay que destacar que este método
     * no actualiza el valor en el fichero.
     *
     * @param key parámetro de configuración que queremos recuperar.
     * @param value nuevo valor que queremos asignar.
     * @return true o false en función de si la operación se ha realizado con
     * éxito o no.
     */
    public static Boolean set(String key, String value) {
        //Verificamos si existe la clave que queremos actualizar en el sistema.
        if (ConfigurationManager._properties.containsKey(key)) {
            //Actualizamos el valor de la propiedad en la tabla.
            ConfigurationManager._properties.put(key, value);
            //Retornamos true.
            return true;
        } else {
            //Retornamos false.
            return false;
        }
    }
}
