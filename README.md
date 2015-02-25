Apache Spark Benchmark
======================

EL presente proyecto es fruto de un trabajo fin de master qué pretende ser un banco de pruebas del framework Apache Spark. La idea subyacente es la de poder ejecutar el framework en diferentes entornos tanto software como hardware para ver cuál es su comportamiento y comparar los resultados obtenidos con soluciones similares como puede ser Hive, Redshift, etc.

El trabajo está basado en el benchmark de big data realizado por la universidad de Berckley (https://amplab.cs.berkeley.edu/benchmark/). En esencia, las pruebas realizadas son similares con la salvedad de que éstas se han hecho empleando el API Java de Apache Spark, y se han probado algunas configuraciones adicionales. Además, el benchmark está pensado no sólo para ser ejecutado en Amazon EC2, si no en cualquier cluster, tanto en la nube como local.

Ejecución del proyecto
----------------------

Configuración del proyecto
--------------------------

A continuación se relacionan y explican brevemente los parámetros generales de configuración del proyecto.

Propiedades de configuracion globales.
--------------------------------------

Estos parámetros de configuración son comunes y afectan a todos los test desarrollados en el proyecto.

 * **apache.benchmark.config.global.master**: Modo en el que queremos ejecutar el benchmar. Puede tomar el valor "local" si queremos hacer una ejecución local, por ejemplo, si estamos haciendo pruebas en nuestro ordenador y no queremos enviar los trabajos al cluster. O existe la configuración master, en la que lo que le proporcionamos a la librería son los datos de conexión al nodo master del cluster donde queremos ejecutar los trabajos.
 * **apache.benchmark.config.global.spark.home**: Directorio de la instalación de Apache Spark.
 * **apache.benchmark.config.global.num.trials**: Número de veces que queremos que se ejecute el kernel computacional de nuestro test. El programa calcula el tiempo medio entre todas las ejecuciones.
 * **apache.benchmark.config.global.verbose**: En modo verbose el programa de ejecución de las pruebas muestra por pantalla la traza de ejecución de cada uno de los test. Este modo puede combinarse con el modo test.
 * **apache.benchmark.config.global.test.mode**: En el modo test cada uno de los test ejecutar una serie de funciones que permiten verificar que la carga y/o ejecución de los test se ha hecho correctamente. Comentar que el nivel de verificación que se lleva a cabo es muy sencillo.
 * **apache.benchmark.config.global.independent.jvm.execution**: Según el valor que tome esta variable se modifica el modo de ejecución de los test. Si el valor es 1, el thread main lanza cada uno de los test en una instancia independiente de la JVM. Esto se hace así para que la ejecución de un test, o el orden el que se ejecutan estos, no influyan en los resultados obtenidos. Si el valor del parámetro es 0, la ejecución que se hace es directa, es decir, la hace el thread main dentro de la misma instancia de la JVM. Este modo se ha mantenido ya que facilita el desarrollo y depuración de los test.
 * **apache.benchmark.config.global.root.dir**: Path al directorio de trabajo del benchmark. Entre otras operaciones, en este directorio es donde se crea el fichero de resultados del benchmark.

### Propiedades de configuracion de los benchmark SQL

Los parámetros definidos en esta sección afectan solamente a los test de tipo SQL.

 * **apache.benchmark.config.sql.global.data.base.dir**: Path al directorio base donde se encuentran los fichero que contienen los datos que usaremos en los test. 
 * **apache.benchmark.config.sql.global.data.compression.type**: Dentro del directorio base el test presupone que existe un subdirectorio. Cada uno de estos se corresponde con un formato diferente para los ficheros de datos. Por ejemplo text, text-deflate, sequence, sequence-snappy, etc.
 * **apache.benchmark.config.sql.global.data.size**: Dentro del directorio de formato anterior, el test presupone que existe un subdirectorio en función de la dimensión del dataset. Por ejemplo, Tiny, 1node, 5node, etc.
 * **apache.benchmark.config.sql.global.data.ranking.dir.name**: Nombre del directorio donde se encuentran la información del dataset para la tabla ranking.
 * **apache.benchmark.config.sql.global.data.uservisits.dir.name**: Nombre del directorio donde se encuentran la información del dataset para la tabla uservisits.
 * **apache.benchmark.config.sql.global.data.crawl.dir.name**: Nombre del directorio donde se encuentran la información del dataset para la tabla crawl.

### Propiedades comunes de los test SQL.

A continuación se relaciona una muestra de los posibles parámetros de configuración que puede tener un test SQL de tipo general. La lista constituye la relación mínima de parámetros, es decir, cada caso concreto podrá contar con parámetros adicionales según se requiera.

 * **apache.benchmark.config.sql.query.01.programmatically.name**: Nombre del test actual.
 * **apache.benchmark.config.sql.query.01.programmatically.class**: Path a la clase que implementa el test actual.
 * **apache.benchmark.config.sql.query.01.programmatically.test.values**: Las querys de algunos de los test que ejecutamos están parametrizadas, es decir, en cada ejecución podemos usar un valor diferente para la prueba. Este parámetro contiene la lista, separadas por comas, de los posibles valores que queremos probar. El programa ya se encarga de parsear dichos valores y ejecutar el test tantas veces como valores hallamos especificados.
 * **apache.benchmark.config.sql.query.01.programmatically.active**: Variable que permite al usuario activa o desactivar la ejecución del test. 1 hace que el programa ejecute el test, 0 lo ignora.

Listado de test disponibles.
----------------------------

A continuación se describen brevemente los test dispobibles en la suite de pruebas.

### Query 1: Scan Query
```sql
SELECT pageURL, pageRank FROM rankings WHERE pageRank > X
```
This query scans and filters the dataset and stores the results.

This query primarily tests the throughput with which each framework can read and write table data. The best performers are Impala (mem) and Shark (mem) which see excellent throughput by avoiding disk. For on-disk data, Redshift sees the best throughput for two reasons. First, the Redshift clusters have more disks and second, Redshift uses columnar compression which allows it to bypass a field which is not used in the query. Shark and Impala scan at HDFS throughput with fewer disks.

Both Shark and Impala outperform Hive by 3-4X due in part to more efficient task launching and scheduling. As the result sets get larger, Impala becomes bottlenecked on the ability to persist the results back to disk. Nonetheless, since the last iteration of the benchmark Impala has improved its performance in materializing these large result-sets to disk.

Tez sees about a 40% improvement over Hive in these queries. This is in part due to the container pre-warming and reuse, which cuts down on JVM initialization time.

### Query 2: Aggregation Query
```sql
SELECT SUBSTR(sourceIP, 1, X), SUM(adRevenue) FROM uservisits GROUP BY SUBSTR(sourceIP, 1, X)
```
This query applies string parsing to each input tuple then performs a high-cardinality aggregation.
Redshift's columnar storage provides greater benefit than in Query 1 since several columns of the UserVistits table are un-used. While Shark's in-memory tables are also columnar, it is bottlenecked here on the speed at which it evaluates the SUBSTR expression. Since Impala is reading from the OS buffer cache, it must read and decompress entire rows. Unlike Shark, however, Impala evaluates this expression using very efficient compiled code. These two factors offset each other and Impala and Shark achieve roughly the same raw throughput for in memory tables. For larger result sets, Impala again sees high latency due to the speed of materializing output tables.

### Query 3: Join Query
```sql
SELECT sourceIP, totalRevenue, avgPageRank
FROM
  (SELECT sourceIP,
          AVG(pageRank) as avgPageRank,
          SUM(adRevenue) as totalRevenue
    FROM Rankings AS R, UserVisits AS UV
    WHERE R.pageURL = UV.destURL
       AND UV.visitDate BETWEEN Date(`1980-01-01') AND Date(`X')
    GROUP BY UV.sourceIP)
  ORDER BY totalRevenue DESC LIMIT 1
```
This query joins a smaller table to a larger table then sorts the results.
When the join is small (3A), all frameworks spend the majority of time scanning the large table and performing date comparisons. For larger joins, the initial scan becomes a less significant fraction of overall response time. For this reason the gap between in-memory and on-disk representations diminishes in query 3C. All frameworks perform partitioned joins to answer this query. CPU (due to hashing join keys) and network IO (due to shuffling data) are the primary bottlenecks. Redshift has an edge in this case because the overall network capacity in the cluster is higher.

### Query 4: External Script Query
```sql
CREATE TABLE url_counts_partial AS 
  SELECT TRANSFORM (line)
    USING "python /root/url_count.py" as (sourcePage, destPage, cnt) 
  FROM documents;
CREATE TABLE url_counts_total AS 
  SELECT SUM(cnt) AS totalCount, destPage 
  FROM url_counts_partial 
  GROUP BY destPage;
```
This query calls an external Python function which extracts and aggregates URL information from a web crawl dataset. It then aggregates a total count per URL.
Impala and Redshift do not currently support calling this type of UDF, so they are omitted from the result set. Impala UDFs must be written in Java or C++, where as this script is written in Python. The performance advantage of Shark (disk) over Hive in this query is less pronounced than in 1, 2, or 3 because the shuffle and reduce phases take a relatively small amount of time (this query only shuffles a small amount of data) so the task-launch overhead of Hive is less pronounced. Also note that when the data is in-memory, Shark is bottlenecked by the speed at which it can pipe tuples to the Python process rather than memory throughput. This makes the speedup relative to disk around 5X (rather than 10X or more seen in other queries).

Licencia
--------

Apache Spark Benchmark está protegido bajo licencia GNU General Public License, versión 2
