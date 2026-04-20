from pyspark.sql import functions as F


def add_rfm_scores(rfm, config):

    r_cfg = config["rfm"]

    rec_bins = r_cfg["recency_days_bins"]
    freq_bins = r_cfg["frequency_bins"]
    mon_bins = r_cfg["monetary_bins"]

    # safety check (important in pipelines)
    if "recency" not in rfm.columns:
        raise ValueError("recency column missing. Run compute_recency first.")

    return (
        rfm

        # =====================
        # RECENCY SCORE
        # =====================
        .withColumn(
            "R_score",
            F.when(F.col("recency") <= rec_bins[0], 5)
             .when(F.col("recency") <= rec_bins[1], 4)
             .when(F.col("recency") <= rec_bins[2], 3)
             .when(F.col("recency") <= rec_bins[3], 2)
             .otherwise(1)
        )

        # =====================
        # FREQUENCY SCORE
        # =====================
        .withColumn(
            "F_score",
            F.when(F.col("frequency") <= freq_bins[0], 1)
             .when(F.col("frequency") <= freq_bins[1], 2)
             .when(F.col("frequency") <= freq_bins[2], 3)
             .when(F.col("frequency") <= freq_bins[3], 4)
             .otherwise(5)
        )

        # =====================
        # MONETARY SCORE
        # =====================
        .withColumn(
            "M_score",
            F.when(F.col("monetary") <= mon_bins[0], 1)
             .when(F.col("monetary") <= mon_bins[1], 2)
             .when(F.col("monetary") <= mon_bins[2], 3)
             .when(F.col("monetary") <= mon_bins[3], 4)
             .otherwise(5)
        )

        # =====================
        # FINAL SCORE
        # =====================
        .withColumn(
            "RFM_score",
            F.col("R_score") + F.col("F_score") + F.col("M_score")
        )
    )