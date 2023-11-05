from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.remote(
  host       = f"https://adb-3983330658043373.13.azuredatabricks.net/?o=3983330658043373", # URL de la cuenta de Databricks
  token      = "dapi030a4aa4d3e99fd2e8fe9a381954db2b-2", # Token
  cluster_id = "1105-101252-dmd0gk8p" # Cluster ID
).getOrCreate()