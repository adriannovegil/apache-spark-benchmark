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

    /**
     * Valor por defecto del fichero de configuración, si no especificamos otro,
     * la carga de los parametros de configuracion se hara con este.
     */
    private static final String BASE_CONFIG_FILE = "benchmark.properties";
    /**
     * Mapa que contiene la lista de propiedades y su valor.
     */
    private static Map<String, String> _properties = null;

    /**
     * Metodo que se encarga de carga los parametros de configuracion que
     * usaremos en el sistema.
     */
    public static void configure() {
        loadConfigure(BASE_CONFIG_FILE);
    }

    /**
     * Metodo que se encarga de carga los parametros de configuracion que
     * usaremos en el sistema.
     *
     * @param configFilename Fichero del que queremos extraer los parametros de
     * configuracion.
     */
    public static void configure(String configFilename) {
        loadConfigure(configFilename);
    }

    /**
     * Metodo que se encarga de carga los parametros de configuracion que
     * usaremos en el sistema.
     *
     * @param configURL URL al fichero de congiuración del que queremos extraer
     * los parametros de configuración.
     */
    public static void configure(java.net.URL configURL) {
        loadConfigure(configURL);
    }

    /**
     * Metodo que se encarga de carga los parametros de configuracion que
     * usaremos en el sistema.
     *
     * @param inputStream InputStream al fichero de configuración del que
     * queremos extraer los parametros de configuración.
     */
    public static void configure(InputStream inputStream) {
        loadConfigure(inputStream);
    }

    /**
     * Método que nos permite recuperar la isntancia de Properties asociada al
     * manager.
     *
     * @return Instancia de Properties que alberga las propiedades del sistema.
     */
    public static Properties getProperties() {
        if (ConfigurationManager._properties == null) {
            configure();
        }
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
        if (ConfigurationManager._properties == null) {
            configure();
        }
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
        if (ConfigurationManager._properties == null) {
            configure();
        }
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
        if (ConfigurationManager._properties == null) {
            configure();
        }
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

    /**
     * Método privado que se encarga de realizar la carga de los parametros de
     * configuración del sistema.
     *
     * @param properties Instancia de properties que queremos cargar en el
     * manager.
     */
    private static void loadConfigure(Properties properties) {
        if (_properties == null) {
            _properties = new ConcurrentHashMap<>();
        }
        //Añadimos las propiedades a la matriz de properties.
        PropertiesUtil.appendPropertiesToMap(properties, _properties);
    }

    /**
     * Método privado que se encarga de realizar la carga de los parametros de
     * configuración del sistema.
     *
     * @param inputStream InputStream al fichero de configuración del que
     * queremos extraer los parametros de configuración.
     */
    private static void loadConfigure(InputStream inputStream) {
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
     * Método privado que se encarga de realizar la carga de los parametros de
     * configuración del sistema.
     *
     * @param configFileName Fichero del que queremos extraer los parametros de
     * configuracion.
     */
    private static void loadConfigure(String configFileName) {
        //Instanciamos un nuevo objeto de properties.
        Properties props = new Properties();
        InputStream istream = null;
        try {
            istream = ConfigurationManager.class.getResourceAsStream("/" + configFileName);
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
     * Método privado que se encarga de realizar la carga de los parametros de
     * configuración del sistema.
     *
     * @param configURL URL al fichero de congiuración del que queremos extraer
     * los parametros de configuración.
     */
    private static void loadConfigure(java.net.URL configURL) {
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
                } catch (IOException | RuntimeException ignore) {
                }
            }
        }
        //Cargamos las propiedades en la matriz.
        loadConfigure(props);
    }
}
