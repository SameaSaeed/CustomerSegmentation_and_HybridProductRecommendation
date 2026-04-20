from pyspark.sql import functions as F


def add_time_features(df, config):

    ts_col = config.get("feature_engineering", {}).get(
        "timestamp_col",
        "order_purchase_timestamp"
    )

    return (
        df
        .withColumn("purchase_year", F.year(F.col(ts_col)))
        .withColumn("purchase_month", F.date_format(F.col(ts_col), "MMM"))
        .withColumn("purchase_yearmonth", F.date_format(F.col(ts_col), "yyyyMM"))
        .withColumn("purchase_dayofweek", F.date_format(F.col(ts_col), "E"))
        .withColumn("month_num", F.month(F.col(ts_col)))
    )