# 🧠 Customer Segmentation & Hybrid Recommender System

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10+-blue?style=flat-square&logo=python"/>
  <img src="https://img.shields.io/badge/PySpark-Big%20Data-orange?style=flat-square&logo=apachespark"/>
  <img src="https://img.shields.io/badge/Databricks-ML%20Pipeline-red?style=flat-square&logo=databricks"/>
  <img src="https://img.shields.io/badge/Machine%20Learning-Recommender-green?style=flat-square"/>
  <img src="https://img.shields.io/badge/Status-Production-brightgreen?style=flat-square"/>
</p>

------------------------------------------------------------------------

## 🚀 Overview

End-to-end customer segmentation + hybrid recommender system using PySpark + collaborative + content-based filtering.

| Component                    | Used for                |
| ---------------------------- | ----------------------- |
| ALS                          | Collaborative filtering |
| Cosine similarity + features | Content-based filtering |


------------------------------------------------------------------------

## 🧱 Architecture

Raw Data → Loader → Data Analysis → Feature Engineering → RFM → Clustering → ALS → Content → Hybrid → Ranking → Export

------------------------------------------------------------------------

## 🔍 Key Features

-   RFM segmentation (VIP / Loyal / Need to focus/ Hibernating)
-   ALS collaborative filtering
-   Content-based similarity
-   Hybrid scoring engine
-   Config-driven pipeline
-   Exportable model bundle 

------------------------------------------------------------------------

## 📦 Output

-   Top-N recommendations per user
-   Hybrid score ranking
-   Serialized model bundle

------------------------------------------------------------------------

## 🔮 Future Work

-   MLflow tracking
-   Real-time API (FastAPI)
-   Feature store integration
-   Streaming ingestion
-   LLM explanations
