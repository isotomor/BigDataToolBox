from init_config import launcher
from pyspark.sql.types import *
from pyspark.dbutils import DBUtils


def get_dbutils(spark):
    return DBUtils(spark)

def run_ingest_campaint_mailchimp(project_data, **_):
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
    dbutils = get_dbutils(spark=spark)

    return None


if __name__ == "__main__":
    launcher(run_ingest_campaint_mailchimp, init_spark=True, use_databricks_spark=True)
