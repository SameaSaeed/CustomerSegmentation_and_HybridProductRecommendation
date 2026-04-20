from pyspark.sql import functions as F


def build_rfm(df, config):
    """
    Config-driven RFM builder (production-safe)
    """

    rfm_cfg = config["rfm"]

    customer_col = rfm_cfg["customer_col"]
    order_col = rfm_cfg["order_col"]
    timestamp_col = rfm_cfg["timestamp_col"]
    monetary_col = rfm_cfg["monetary_col"]

    # ==============================
    # REFERENCE DATE (RECENCY BASELINE)
    # ==============================
    ref_date = df.select(F.max(F.col(timestamp_col))).collect()[0][0]

    # ==============================
    # RFM AGGREGATION
    # ==============================
    rfm = (
        df.groupBy(customer_col)
        .agg(
            F.datediff(F.lit(ref_date), F.max(F.col(timestamp_col))).alias("recency"),
            F.countDistinct(F.col(order_col)).alias("frequency"),
            F.sum(F.coalesce(F.col(monetary_col), F.lit(0.0))).alias("monetary")
        )
    )

    # ==============================
    # SEGMENTATION RULES
    # ==============================
    rfm = rfm.withColumn(
        "segment",
        F.when(F.col("frequency") > 1, "VIP")
         .when(F.col("recency") < 60, "Loyal")
         .otherwise("Normal")
    )

    return rfm