from pyspark.sql import functions as F


# ==============================
# DUPLICATE CHECK
# ==============================
def duplicate_count(df):

    # null-safe row signature
    df_hashed = df.withColumn(
        "_row_hash",
        F.sha2(
            F.concat_ws(
                "||",
                *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in df.columns]
            ),
            256
        )
    )

    dup_df = (
        df_hashed.groupBy("_row_hash")
        .count()
        .filter(F.col("count") > 1)
    )

    result = dup_df.select(
        F.sum(F.col("count") - 1).alias("duplicate_rows")
    ).collect()[0][0]

    return result if result else 0


# ==============================
# MISSING % (WIDE FORMAT)
# ==============================
def missing_percentage(df):

    total = df.count()

    return df.select([
        (F.count(F.when(F.col(c).isNull(), 1)) / total).alias(c)
        for c in df.columns
    ])


# ==============================
# MISSING % (LONG FORMAT)
# ==============================
def missing_percentage_long(df):

    total = df.count()

    return df.select([
        F.lit(c).alias("column"),
        (F.count(F.when(F.col(c).isNull(), 1)) / total).alias("missing_pct")
        for c in df.columns
    ])