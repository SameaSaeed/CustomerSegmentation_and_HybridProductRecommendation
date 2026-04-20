from pyspark.sql import functions as F


def clean_orders(df, config):

    cleaning = config.get("cleaning", {})

    # drop columns
    drop_cols = cleaning.get("drop_cols", [])
    if drop_cols:
        df = df.drop(*drop_cols)

    # filter logic
    filters = cleaning.get("filters", {})
    if "order_status" in filters:
        df = df.filter(F.col("order_status") == filters["order_status"])
        df = df.drop("order_status")

    return df


def cast_dates(df, config):

    cols = config.get("cleaning", {}).get("date_columns", [])

    for c in cols:
        df = df.withColumn(c, F.to_timestamp(F.col(c)))

    return df


def handle_nulls(df, config):

    subset = config.get("cleaning", {}).get("required_fields", [])

    if subset:
        df = df.dropna(subset=subset)

    return df