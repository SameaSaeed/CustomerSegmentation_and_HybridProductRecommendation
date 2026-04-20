from pyspark.sql import functions as F, Window
from pyspark.sql.types import DoubleType
import numpy as np

def cosine(u, i):
    if not u or not i:
        return 0.0
    u, i = np.array(u), np.array(i)
    denom = (np.linalg.norm(u) * np.linalg.norm(i))
    return float(np.dot(u, i) / denom) if denom != 0 else 0.0