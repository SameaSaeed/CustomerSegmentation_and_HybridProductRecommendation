import os
import pickle


def export_recommender_model(
    als_model,
    product_vectors,
    user_profiles,
    rfm,
    config
):
    """
    Fully config-driven hybrid recommender exporter
    """

    # ==============================
    # CONFIG SECTIONS
    # ==============================
    export_cfg = config["model_export"]
    storage_cfg = config["storage"]

    # ==============================
    # BASE EXPORT PATH
    # ==============================
    base_path = export_cfg["output"]["base_path"]

    bundle_name = export_cfg["filenames"]["bundle_name"]
    extension = export_cfg["filenames"]["extension"]

    versioning = export_cfg["output"]["versioning"]
    version_strategy = export_cfg["output"]["version_strategy"]

    # ==============================
    # VERSION HANDLING
    # ==============================
    version_suffix = ""

    if versioning:
        if version_strategy == "timestamp":
            from datetime import datetime
            version_suffix = datetime.now().strftime("_%Y%m%d_%H%M%S")
        elif version_strategy == "uuid":
            import uuid
            version_suffix = f"_{uuid.uuid4().hex[:8]}"
        elif version_strategy == "incremental":
            version_suffix = "_v1"  # placeholder (can be enhanced later)

    # ==============================
    # EXPORT DIRECTORY
    # ==============================
    export_path = f"{base_path}/hybrid_recommender"
    os.makedirs(export_path, exist_ok=True)

    # ==============================
    # OPTIONAL ARTIFACT CONTROL
    # ==============================
    artifacts_cfg = export_cfg.get("artifacts", {})

    model_bundle = {}

    # ALS factors
    if artifacts_cfg.get("user_factors", True):
        model_bundle["user_factors"] = als_model.userFactors.toPandas()

    if artifacts_cfg.get("item_factors", True):
        model_bundle["item_factors"] = als_model.itemFactors.toPandas()

    # Core metadata
    model_bundle["rank"] = als_model.rank

    # Content / hybrid artifacts
    if artifacts_cfg.get("product_vectors", True):
        model_bundle["product_vectors"] = product_vectors.toPandas()

    if artifacts_cfg.get("user_profiles", True):
        model_bundle["user_profiles"] = user_profiles.toPandas()

    if artifacts_cfg.get("rfm_segments", True):
        model_bundle["rfm_segments"] = rfm.toPandas()

    # ==============================
    # FILE NAME BUILD
    # ==============================
    file_name = f"{bundle_name}{version_suffix}.{extension}"
    file_path = f"{export_path}/{file_name}"

    # ==============================
    # SAVE
    # ==============================
    with open(file_path, "wb") as f:
        pickle.dump(model_bundle, f)

    print(f"✅ Model exported successfully to: {file_path}")

    return file_path