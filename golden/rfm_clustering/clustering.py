from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans


def train_kmeans(rfm, config):

    k = config["clustering"]["k"]
    seed = config["clustering"]["seed"]

    assembler = VectorAssembler(
        inputCols=["recency", "frequency", "monetary"],
        outputCol="features"
    )

    df_vec = assembler.transform(rfm)

    scaler = StandardScaler(
        inputCol="features",
        outputCol="scaled_features",
        withMean=True,
        withStd=True
    )

    scaler_model = scaler.fit(df_vec)
    df_scaled = scaler_model.transform(df_vec)

    kmeans = KMeans(
        featuresCol="scaled_features",
        k=k,
        seed=seed
    )

    model = kmeans.fit(df_scaled)

    clustered = model.transform(df_scaled)

    return clustered, scaler_model, model