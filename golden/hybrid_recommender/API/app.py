import streamlit as st
import pickle
import numpy as np
import pandas as pd
import sys
from types import ModuleType

# 1. THE BYPASS 
class UniversalDummy:
    def __init__(self, *args, **kwargs): pass
    def __call__(self, *args, **kwargs): return self
    @classmethod
    def __getattr__(cls, name): return UniversalDummy

def create_dummy_module(name):
    m = ModuleType(name)
    m.__file__ = f"{name}.py" 
    sys.modules[name] = m
    m.__getattr__ = lambda attr: UniversalDummy
    return m

# Register all possible Spark paths before loading the pickle
for mod in [
    "pyspark", "pyspark.sql", "pyspark.sql.types", 
    "pyspark.sql.metrics", "pyspark.ml", "pyspark.ml.linalg"
]:
    create_dummy_module(mod)

# 2. LOAD DATA 
@st.cache_resource
def load_model():
    with open("hybrid_recommender_full.pkl", "rb") as f:
        return pickle.load(f)

m = load_model()

# 3. UI LOGIC
st.title("🛒 Product Recommender")

uf = m['user_factors']
id_col = 'id' if 'id' in uf.columns else 'customer_id'

# Show a sample ID in the input box so you don't get 'Not Found' errors
sample_id = str(uf[id_col].iloc[0]) 
user_id = st.text_input("Enter Customer ID", sample_id)

if st.button("Generate Recommendations"):
    target_id = user_id.strip().lower()
    
    # Cleaning the lookup
    uf_clean = uf.copy()
    uf_clean[id_col] = uf_clean[id_col].astype(str).str.strip().str.lower()
    user_data = uf_clean[uf_clean[id_col] == target_id]
    
    if not user_data.empty:
        # Access vector and flatten (handles Spark wrapped arrays)
        u_vec = np.array(user_data['features'].iloc[0]).flatten()
        
        i_df = m['item_factors']
        i_matrix = np.stack(i_df['features'].values)
        
        scores = np.dot(i_matrix, u_vec)
        
        results = pd.DataFrame({
            'product_id': i_df['id'],
            'final_score': scores
        }).sort_values(by='final_score', ascending=False).head(10)
        
        st.success(f"Recommendations for {target_id[:8]}")
        st.dataframe(results, use_container_width=True)
    else:
        st.error(f"ID not found. This model only contains {len(uf)} users.")
