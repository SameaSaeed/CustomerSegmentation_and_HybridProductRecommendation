# ==============================
# IMPORTS
# ==============================
from pyspark.sql import SparkSession
from pyspark import StorageLevel

from data.ingestion import load_tables
from processing.joins import build_base_df
from processing.cleaning import clean_orders, cast_dates, handle_nulls
from processing.feature_engineering import add_time_features
from processing.validation import duplicate_count, missing_percentage

from rfm.rfm_builder import build_rfm
from rfm.rfm_scoring import compute_recency, add_rfm_scores
from rfm.rfm_segmentation import segment_rfm

from clustering.kmeans import train_kmeans

from utils.config_loader import load_config
from utils.logging import get_logger


# ==============================
# CONFIG + LOGGER
# ==============================
CONFIG = load_config()
logger = get_logger()


# ==============================
# BRONZE → SILVER PIPELINE
# ==============================
def run_pipeline(spark):

    logger.info("Loading bronze tables...")

    data = load_tables(spark, CONFIG)

    logger.info("Building base dataframe...")
    df = build_base_df(
        data["orders"],
        data["customers"],
        data["order_items"]
    )

    logger.info("Applying silver transformations...")

    df = clean_orders(df)
    df = cast_dates(df, CONFIG["cleaning"]["date_columns"])
    df = handle_nulls(df)
    df = add_time_features(df)

    df.persist(StorageLevel.MEMORY_AND_DISK)

    logger.info("Silver layer ready")

    return df


# ==============================
# VALIDATION
# ==============================
def validate(df):
    logger.info("Running validation...")

    dup = duplicate_count(df)
    logger.info(f"Duplicate rows: {dup}")

    missing_percentage(df).show()


# ==============================
# SAVE HELPERS (BRONZE/SILVER/GOLD aware)
# ==============================
def save_table(df, layer, name):

    catalog = CONFIG["databricks"]["catalog"]
    schema = CONFIG["databricks"]["schema"]

    full_name = f"{catalog}.{schema}.{layer}_{name}"

    logger.info(f"Saving: {full_name}")

    df.write.mode("overwrite").saveAsTable(full_name)


# ==============================
# RFM PIPELINE (GOLD)
# ==============================
def build_rfm_layer(df):

    logger.info("Building RFM layer...")

    rfm = build_rfm(df, CONFIG)
    rfm = compute_recency(rfm, CONFIG)
    rfm = add_rfm_scores(rfm, CONFIG)
    rfm = segment_rfm(rfm, CONFIG)

    return rfm


# ==============================
# MAIN PIPELINE
# ==============================
def main(spark):

    try:
        logger.info("Pipeline started")

        # --------------------------
        # SILVER
        # --------------------------
        df = run_pipeline(spark)

        if CONFIG["run"]["validation"]:
            validate(df)

        save_table(df, "silver", CONFIG["tables"]["silver"]["clean"])

        df.unpersist()

        # --------------------------
        # GOLD - RFM
        # --------------------------
        rfm = build_rfm_layer(df)

        save_table(rfm, "gold", CONFIG["tables"]["gold"]["rfm"])

        # --------------------------
        # CLUSTERING
        # --------------------------
        if CONFIG["run"]["clustering"]:
            clusters = train_kmeans(rfm, CONFIG)
            save_table(clusters, "gold", CONFIG["tables"]["gold"]["clusters"])

        logger.info("Pipeline completed successfully ✅")

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise

    finally:
        logger.info("Pipeline finished")


# ==============================
# ENTRY POINT
# ==============================
if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    main(spark)