from pyspark.sql import functions as F


def load_data(spark, config):
    """
    Gold-layer recommender data loader
    Fully config-driven (clean + testable)
    """

    catalog = config["databricks"]["catalog"]
    schema = config["databricks"]["schema"]

    tables = config["tables"]["bronze"]

    # ==============================
    # LOAD BRONZE TABLES
    # ==============================
    orders = spark.table(f"{catalog}.{schema}.{tables['orders']}")
    items = spark.table(f"{catalog}.{schema}.{tables['items']}")
    reviews = spark.table(f"{catalog}.{schema}.{tables['reviews']}")
    products = spark.table(f"{catalog}.{schema}.{tables['products']}")
    category = spark.table(f"{catalog}.{schema}.{tables['category']}")

    # ==============================
    # BUILD BASE RECOMMENDER DATA
    # ==============================
    rec_data = (
        orders
        .join(items, "order_id", "inner")
        .join(reviews, "order_id", "inner")
        .select(
            F.col("customer_id"),
            F.col("product_id"),
            F.col("order_id"),
            F.col(config["features"]["timestamp_col"]).cast("timestamp").alias("timestamp"),
            F.col(config["rfm"]["monetary_col"]).cast("double").alias("rating")
        )
        .dropna()
    )

    # ==============================
    # OPTIONAL FILTERING
    # ==============================
    filters = config.get("cleaning", {}).get("filters", {})

    status_filter = filters.get("order_status")

    if status_filter and "order_status" in orders.columns:
        rec_data = rec_data.join(
            orders.select("order_id", "order_status"),
            "order_id",
            "inner"
        ).filter(F.col("order_status") == status_filter)

    return rec_data, products, category