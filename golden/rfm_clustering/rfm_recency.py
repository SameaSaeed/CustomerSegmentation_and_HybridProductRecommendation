from pyspark.sql import functions as F


def compute_recency(rfm, config):

    ts_col = config["rfm"]["timestamp_col"]

    return rfm.withColumn(
        "recency",
        F.datediff(F.current_timestamp(), F.col(ts_col))
    )