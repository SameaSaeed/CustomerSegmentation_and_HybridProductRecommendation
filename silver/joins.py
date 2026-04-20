from pyspark.sql import DataFrame

def build_base_df(orders, customers, order_items):

    order_cust = orders.join(
        customers,
        on="customer_id",
        how="left"
    )

    df = order_cust.join(
        order_items,
        on="order_id",
        how="left"
    )

    return df