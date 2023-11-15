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

    df = project_data.read_spark_csv_files(storage_account=project_data.config["STORAGE_ACCOUNT"],
                                           blob_storage=project_data.config["RAW_BLOB_STORAGE"],
                                           file_name="products.csv")

    df.show()

    return None


if __name__ == "__main__":
    launcher(run_tablon, init_spark=True, use_databricks_spark=True)
