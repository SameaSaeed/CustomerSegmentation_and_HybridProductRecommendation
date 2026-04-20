from pyspark.ml.recommendation import ALS

def train_als(df, rank, max_iter, reg):
    als = ALS(
        userCol="user_idx",
        itemCol="item_idx",
        ratingCol="review_score",
        rank=rank,
        maxIter=max_iter,
        regParam=reg,
        coldStartStrategy="nan"
    )
    return als.fit(df)