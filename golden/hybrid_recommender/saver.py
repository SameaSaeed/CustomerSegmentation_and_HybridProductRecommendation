def save_table(df, catalog, schema, name):
    df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.{name}")