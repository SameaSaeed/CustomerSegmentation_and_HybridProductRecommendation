from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer

def build_sample(rec_data, limit=5000, ratio=0.1):
    return rec_data.sample(False, ratio, seed=42).limit(limit)

def build_indexers(df):
    user_indexer = StringIndexer(inputCol="customer_id", outputCol="user_idx", handleInvalid="skip")
    item_indexer = StringIndexer(inputCol="product_id", outputCol="item_idx", handleInvalid="skip")

    pipeline = Pipeline(stages=[user_indexer, item_indexer])
    model = pipeline.fit(df)
    return model, model.transform(df)