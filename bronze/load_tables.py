def load_tables(spark, config):

    catalog = config["databricks"]["catalog"]
    schema = config["databricks"]["schema"]

    tables = config["tables"]

    return {
        k: spark.table(f"{catalog}.{schema}.{v}")
        for k, v in tables.items()
    }