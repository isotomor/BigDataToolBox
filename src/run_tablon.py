from src.init_config import launcher
from pyspark.sql.types import *
from pyspark.dbutils import DBUtils


def get_dbutils(spark):
    return DBUtils(spark)


def run_tablon(project_data, **_):
    schema = StructType([
        StructField('CustomerID', IntegerType(), False),
        StructField('FirstName', StringType(), False),
        StructField('LastName', StringType(), False)
    ])

    data = [
        [1000, 'Mathijs', 'Oosterhout-Rijntjes'],
        [1001, 'Joost', 'van Brunswijk'],
        [1002, 'Stan', 'Bokenkamp']
    ]

    customers = project_data.spark.createDataFrame(data, schema)
    customers.show()

    ### Usamos DBUTILS
    dbutils = get_dbutils(spark=project_data.spark)

    # project_data.spark.conf.set(
    #     "fs.azure.account.key.dataarquitectazureml.dfs.core.windows.net",
    #     "3tc2TjcSDZpw1PPPI4ZI9KwQZ+ML9lCU8ekzywO5hlPkcw9GgiWKVu8zsVdhewLPCR2ZC5UPzgxm+AStzPCB6Q=="
    # )
    storaga_account = "cloudmlarquitecture"
    blob_storage = "raw"

    df = project_data.spark.read.csv(f"abfss://raw@{storaga_account}.dfs.core.windows.net")
    df.show()

    return None


if __name__ == "__main__":
    launcher(run_tablon, init_spark=True, use_databricks_spark=True)
