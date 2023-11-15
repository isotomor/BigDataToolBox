class Lake(object):
    """
    Clase encargada de gestionar las llamadas al datalake, escritura y lectura de datos con las diferentes interfaces.
    """

    def __init__(self, config, logger, spark=None):
        self.config = config
        self.logger = logger
        self.spark = spark

    def read_spark_parquet_files(self, storage_account: str, blob_storage: str, file_name: str):
        """
        Función que recibe Un Storage, un fichero en un blob de databricks.
        """
        df = self.spark.read.parquet(f"abfss://{blob_storage}@{storage_account}.dfs.core.windows.net/{file_name}")

        return df

    def read_spark_csv_files(self, storage_account: str, blob_storage: str, file_name: str):
        """
        Función que recibe Un Storage, un fichero en un blob de databricks.
        """
        df = self.spark.read.csv(f"abfss://{blob_storage}@{storage_account}.dfs.core.windows.net/{file_name}")

        return df