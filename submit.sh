#!/bin/bash

# ------------------------------------------------------------------------------
# Ejecucion prueba caso 1
# ------------------------------------------------------------------------------

# Programmatically
/opt/spark/bin/spark-submit --class es.devcircus.apache.spark.benchmark.sql.tests.query01.Query01ProgrammaticallyTest --master local target/apache-spark-benchmark-0.0.1-SNAPSHOT.jar

# Reflection
#/opt/spark/bin/spark-submit --class es.devcircus.apache.spark.benchmark.sql.tests.query01.Query01ReflectionTest --master local target/apache-spark-benchmark-0.0.1-SNAPSHOT.jar

# Hive
#/opt/spark/bin/spark-submit --class es.devcircus.apache.spark.benchmark.sql.tests.query01.Query01HiveTest --master local target/apache-spark-benchmark-0.0.1-SNAPSHOT.jar

# ------------------------------------------------------------------------------
# Ejecucion prueba caso 2
# ------------------------------------------------------------------------------

# Programmatically
#/opt/spark/bin/spark-submit --class es.devcircus.apache.spark.benchmark.sql.tests.query02.Query02ProgrammaticallyTest --master local target/apache-spark-benchmark-0.0.1-SNAPSHOT.jar

# Reflection
#/opt/spark/bin/spark-submit --class es.devcircus.apache.spark.benchmark.sql.tests.query02.Query02ReflectionTest --master local target/apache-spark-benchmark-0.0.1-SNAPSHOT.jar

# Hive
#/opt/spark/bin/spark-submit --class es.devcircus.apache.spark.benchmark.sql.tests.query02.Query02HiveTest --master local target/apache-spark-benchmark-0.0.1-SNAPSHOT.jar

# ------------------------------------------------------------------------------
# Ejecucion prueba caso 3
# ------------------------------------------------------------------------------

# Programmatically
#/opt/spark/bin/spark-submit --class es.devcircus.apache.spark.benchmark.sql.tests.query03.Query03ProgrammaticallyTest --master local target/apache-spark-benchmark-0.0.1-SNAPSHOT.jar

# Reflection
#/opt/spark/bin/spark-submit --class es.devcircus.apache.spark.benchmark.sql.tests.query03.Query03ReflectionTest --master local target/apache-spark-benchmark-0.0.1-SNAPSHOT.jar

# Hive
#/opt/spark/bin/spark-submit --class es.devcircus.apache.spark.benchmark.sql.tests.query03.Query03HiveTest --master local target/apache-spark-benchmark-0.0.1-SNAPSHOT.jar

# ------------------------------------------------------------------------------
# Ejecucion prueba caso 4
# ------------------------------------------------------------------------------

# Hive
#/opt/spark/bin/spark-submit --class es.devcircus.apache.spark.benchmark.sql.tests.query04.Query04HiveTest --master local target/apache-spark-benchmark-0.0.1-SNAPSHOT.jar



#/opt/spark/bin/spark-submit --class es.devcircus.simplesqlapp.JSimpleSqlApp --master local[2] target/apache-spark-sql-benchmark-0.0.1-SNAPSHOT.jar
#/opt/spark/bin/spark-submit --class es.devcircus.simplesqlapp.JSimpleSqlApp --master local[4] target/apache-spark-sql-benchmark-0.0.1-SNAPSHOT.jar
