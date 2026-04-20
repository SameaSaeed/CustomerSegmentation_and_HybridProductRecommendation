from pyspark.sql import functions as F


def segment_rfm(rfm, config):

    return (
        rfm.withColumn(
            "segment",
            F.when((F.col("R_score") >= 4) & (F.col("F_score") >= 4), "VIP")
             .when((F.col("F_score") >= 3) & (F.col("R_score") >= 3), "Loyal")
             .when(F.col("R_score") <= 2, "At Risk")
             .otherwise("Potential")
        )
    )