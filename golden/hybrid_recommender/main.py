from src.utils.config import load_config
from data_loader import load_data
from preprocessing import build_sample, build_indexers
from rfm_clustering.rfm_builder import build_rfm
from als_model import train_als
from saver import save_table
from model_exporter import export_recommender_model
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))


def run(spark):

    # ==============================
    # LOAD CONFIG (SINGLE SOURCE OF TRUTH)
    # ==============================
    config = load_config()

    databricks_cfg = config["databricks"]
    catalog = databricks_cfg["catalog"]
    schema = databricks_cfg["schema"]

    sample_ratio = config["run"].get("sample_ratio", 0.1)
    sample_limit = config["run"].get("sample_limit", 5000)

    als_cfg = config["als"]

    # ==============================
    # DATA LAYER
    # ==============================
    rec_data, products, pro_cat = load_data(spark, config)

    df_sample = build_sample(rec_data, sample_limit, sample_ratio)

    index_model, df_indexed = build_indexers(df_sample)

    # ==============================
    # FEATURE / RFM LAYER
    # ==============================
    rfm_df = build_rfm(rec_data)

    # ==============================
    # MODEL TRAINING (ALS)
    # ==============================
    als_model = train_als(
        df_indexed,
        als_cfg["rank"],
        als_cfg["max_iter"],
        als_cfg["reg_param"]
    )

    preds = als_model.transform(df_indexed)

    # ==============================
    # SAVE PREDICTIONS (GOLD TABLE)
    # ==============================
    save_table(preds, catalog, schema, "als_predictions")

    # ==============================
    # HYBRID ARTIFACTS (FOR EXPORT)
    # ==============================
    product_vectors = None  # if generated in content pipeline
    user_profiles = None    # if generated in content pipeline

    # ==============================
    # MODEL EXPORT (NEW STEP)
    # ==============================
    if config["model_export"]["registry"]["enable"] is False:
        export_recommender_model(
            als_model=als_model,
            product_vectors=product_vectors,
            user_profiles=user_profiles,
            rfm=rfm_df,
            config=config
        )

    print("✅ Full pipeline completed successfully")


if __name__ == "__main__":
    run(spark)