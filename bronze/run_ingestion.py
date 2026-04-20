from autoloader import ingest_csv

def run_all(spark, config):

    tables = config["ingestion"]["tables"]

    for table in tables:
        print(f"Ingesting {table}...")

        ingest_csv(
            spark=spark,
            table_name=table,
            config=config
        )