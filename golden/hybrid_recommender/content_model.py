from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.sql import functions as F

def build_product_features(products, pro_cat):
    prod_feat = products.join(pro_cat, "product_category_name", "left").fillna("unknown")

    cont_pipe = Pipeline(stages=[
        StringIndexer(
            inputCol="product_category_name_english",
            outputCol="c_idx",
            handleInvalid="skip"
        ),
        OneHotEncoder(inputCol="c_idx", outputCol="c_vec")
    ]).fit(prod_feat)

    return cont_pipe.transform(prod_feat)