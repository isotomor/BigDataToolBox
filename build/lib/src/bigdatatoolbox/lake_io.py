import getpass
import os
# from async_runner import async_runner
from pyspark.sql import SparkSession


def create_spark_session(name=None) -> SparkSession:
    """
    Crea una sesión de Spark con la configuración proporcionada
    """

    builder = SparkSession.builder

    name = name or f"default_{getpass.getuser()}_{os.getpid()}"
    builder = builder.appName(name)
    builder = builder.enableHiveSupport()

    builder = builder.config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar')
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    return spark
