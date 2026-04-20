# 🧠 Customer Segmentation & Hybrid Recommender System

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10+-blue?style=flat-square&logo=python"/>
  <img src="https://img.shields.io/badge/PySpark-Big%20Data-orange?style=flat-square&logo=apachespark"/>
  <img src="https://img.shields.io/badge/Databricks-ML%20Pipeline-red?style=flat-square&logo=databricks"/>
  <img src="https://img.shields.io/badge/Machine%20Learning-Recommender-green?style=flat-square"/>
  <img src="https://img.shields.io/badge/Status-Development-brightgreen?style=flat-square"/>
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

Purchase_trend_2017_2018
<img width="1645" height="515" alt="Purchase_trend_2017_2018" src="https://github.com/user-attachments/assets/b097de82-3a5f-4b72-bffa-c731a0f840d0" />

Purchase_by_month
<img width="1645" height="479" alt="Purchase_by_month" src="https://github.com/user-attachments/assets/84da6dea-e964-4f8b-97c8-2cb1be1377e0" />

Purchase_by_week_days
<img width="1654" height="479" alt="Purchase_by_week_days" src="https://github.com/user-attachments/assets/257c5005-bfe7-4efd-ac6a-e3f78e4c92cb" />

Purchase_by_period_of_days
<img width="1654" height="479" alt="Purchase_by_period_of_days" src="https://github.com/user-attachments/assets/b6dcd630-5b51-4eb8-b661-c29da10db0b7" />

Purchase_by_citites
<img width="1013" height="556" alt="Purchase_by_citites" src="https://github.com/user-attachments/assets/6b4bdc67-31ea-40ec-88bb-0eb80c72760a" />

Customers_by_states
<img width="856" height="556" alt="Customers_by_states" src="https://github.com/user-attachments/assets/58582d27-5a5e-4506-8921-0717ca5978b4" />

customer_segments
<img width="1023" height="701" alt="customer_segments" src="https://github.com/user-attachments/assets/3d3407ce-3085-415d-aaa6-2b0c2322b496" />

RFM
<img width="581" height="468" alt="FRM" src="https://github.com/user-attachments/assets/e4856353-3d47-4fc5-a079-88100253e93e" />

RFM_by_customer
<img width="487" height="311" alt="rfm_by_customer" src="https://github.com/user-attachments/assets/9f6a9cee-8dab-43d5-b024-147dd3610ec5" />


RFM_clusters
<img width="1461" height="498" alt="rfm_clusters" src="https://github.com/user-attachments/assets/1461a46e-02f3-4c06-a684-9525ff2fa93a" />

<img width="221" height="232" alt="kmeans" src="https://github.com/user-attachments/assets/76c5acea-d6e5-41a4-849b-7df6cd5b83f2" />

------------------------------------------------------------------------

Recommendations
<img width="950" height="412" alt="recommendation_app" src="https://github.com/user-attachments/assets/86c475d6-b7e6-4d58-a1e8-4a68565c24ba" />

------------------------------------------------------------------------

## 🔮 Future Work

-   MLflow tracking
-   Real-time API (FastAPI)
-   Feature store integration
-   Streaming ingestion
-   LLM explanations
