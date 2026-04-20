def build_rfm_pipeline(df, config):

    from rfm.rfm_builder import build_rfm
    from rfm.rfm_scoring import compute_recency, add_rfm_scores
    from rfm.rfm_segmentation import segment_rfm
    from clustering.kmeans import train_kmeans

    # 1. RFM base
    rfm = build_rfm(df, config)

    # 2. Recency
    rfm = compute_recency(rfm, config)

    # 3. Scoring
    rfm = add_rfm_scores(rfm, config)

    # 4. Segmentation
    rfm = segment_rfm(rfm, config)

    # 5. Clustering
    clustered, scaler_model, kmeans_model = train_kmeans(rfm, config)

    return clustered, scaler_model, kmeans_model