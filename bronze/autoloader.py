from pyspark.sql.functions import *

def ingest_csv(spark, table_name, config):

    base_path = config["paths"]["raw"]
    catalog = config["databricks"]["catalog"]
    schema = config["databricks"]["schema"]

    path = f"{base_path}/{table_name}.csv"

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .option("inferSchema", True)
        .load(path)
        .writeStream
        .option(
            "checkpointLocation",
            f"/Volumes/{catalog}/{schema}/checkpoints/{table_name}"
        )
        .trigger(availableNow=True)
        .toTable(f"{catalog}.{schema}.{table_name}")
    )