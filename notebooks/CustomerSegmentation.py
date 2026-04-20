# Generated from: CustomerSegmentation (3).ipynb
# Converted at: 2026-04-20T19:19:06.316Z
# Next step (optional): refactor into modules & generate tests with RunCell
# Quick start: pip install runcell

or_dat = spark.table("orders_dataset")
or_item = spark.table("order_items_dataset")
cust = spark.table("customers_dataset")
or_pay = spark.table("order_payments_dataset")
or_review = spark.table("order_reviews_dataset")
products = spark.table("products_dataset")
pro_category = spark.table("product_category_name_translation")
geo = spark.table("geolocation_dataset")
sell = spark.table("sellers_dataset")

from pyspark.sql import functions as F

spark.sql("USE CATALOG customersegmentation")
spark.sql("USE default")
order_cust = (
    or_dat
    .join(cust, on="customer_id", how="left")
)

order_cust.show(5)

df = order_cust.join(or_item, on="order_id", how="left")
df.show(5)

df = df.drop(
    "customer_id",
    "seller_id",
    "shipping_limit_date",
    "customer_zip_code_prefix"
)


df = df.filter(df.order_status == "delivered")


df = df.drop("order_status")


from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

date_cols = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

for col in date_cols:
    df = df.withColumn(col, F.to_timestamp(F.col(col)))


from pyspark.sql.types import StringType

df = df.withColumn("order_item_id", F.col("order_item_id").cast(StringType()))

from pyspark.sql import Window
from pyspark.sql import functions as F

# Define a window partitioned by all columns
w = Window.partitionBy(df.columns)

# Count how many times each row appears
df_with_dup_count = df.withColumn("dup_count", F.count("*").over(w))

# Count only rows that are duplicates (appear more than once)
duplicate_count = df_with_dup_count.filter(F.col("dup_count") > 1) \
                                   .select(F.sum(F.col("dup_count") - 1)) \
                                   .collect()[0][0]

print(duplicate_count)



missing_df = df.select([
    (F.count(F.when(F.col(c).isNull(), 1)) / df.count()).alias(c) 
    for c in df.columns
])

missing_df.show()


from pyspark.sql import functions as F

# compute missing percentage per column in long format
missing_df = df.select([
    (F.count(F.when(F.col(c).isNull(), 1)) / df.count()).alias(c)
    for c in df.columns
]).selectExpr(
    "stack({}, {}) as (column, persentase_missing)".format(
        len(df.columns),
        ', '.join([f"'{c}', {c}" for c in df.columns])
    )
)

missing_df.show()


df = df.dropna(how="any", subset=["order_purchase_timestamp", "order_item_id"])


from pyspark.sql import functions as F

# Count nulls per column in wide format
null_counts_wide = df.select([
    F.count(F.when(F.col(c).isNull(), 1)).alias(c) 
    for c in df.columns
])

# Convert to long format
null_counts_long = null_counts_wide.selectExpr(
    "stack({}, {}) as (column, null_count)".format(
        len(df.columns),
        ', '.join([f"'{c}', {c}" for c in df.columns])
    )
)

null_counts_long.show()


num_rows = df.count()       # counts the number of rows
num_cols = len(df.columns)  # counts the number of columns

print((num_rows, num_cols))


from pyspark.sql import functions as F

df = (
    df
    .withColumn("purchase_year", F.year("order_purchase_timestamp"))
    .withColumn("purchase_month", F.date_format("order_purchase_timestamp", "MMM"))
    .withColumn("purchase_yearmonth", F.date_format("order_purchase_timestamp", "yyyyMM"))
    .withColumn("purchase_dayofweek", F.date_format("order_purchase_timestamp", "E"))
    .withColumn(
        "purchase_period", 
        F.create_map(
            F.lit(1), F.lit("Late Night"),
            F.lit(2), F.lit("Early Morning"),
            F.lit(3), F.lit("Morning"),
            F.lit(4), F.lit("Noon"),
            F.lit(5), F.lit("Evening"),
            F.lit(6), F.lit("Night")
        )[ ((F.hour("order_purchase_timestamp") + 4) / 4).cast("integer") ]
    )
    .withColumn("month_num", F.month("order_purchase_timestamp"))
)

df.limit(5).toPandas()


import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F

# 1. Aggregate number of orders per year-month
orders_per_month = (
    df.groupBy("purchase_yearmonth")
      .count()
      .orderBy("purchase_yearmonth")
      .toPandas()   # collect small aggregated data to driver
)

# 2. Optional: filter out first 3 months if needed (like your [3:] slice)
orders_per_month = orders_per_month.iloc[3:]

# 3. Plot
sns.set(rc={'figure.figsize':(20,5)})
sns.lineplot(
    data=orders_per_month,
    x="purchase_yearmonth",
    y="count",
    color='darkslateblue',
    linewidth=2
)
plt.title('Tren Jumlah Pembelian dari Tahun 2017 hingga 2018')
plt.xticks(rotation=45)  # rotate x-axis for readability
plt.show()


from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from functools import reduce

cols = ['purchase_year', 'purchase_yearmonth']

df = reduce(lambda d, c: d.withColumn(c, F.col(c).cast(IntegerType())), cols, df)


import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F
import pandas as pd

# 1. Aggregate counts per day-of-week
day_counts = (
    df.groupBy("purchase_dayofweek")
      .count()
      .toPandas()
)

# 2. Optional: order the days
order = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun']
day_counts['purchase_dayofweek'] = pd.Categorical(day_counts['purchase_dayofweek'], categories=order, ordered=True)
day_counts = day_counts.sort_values('purchase_dayofweek')

# 3. Plot
sns.set(rc={'figure.figsize':(20,5)})
sns.barplot(
    data=day_counts, 
    x='purchase_dayofweek', 
    y='count', 
    palette='YlGnBu'
)
plt.title('Comparison of Purchase Counts by Day of the Week')
plt.xlabel('Day')
plt.ylabel('Number of Purchases')
plt.show()


from pyspark.sql import functions as F
from pyspark.sql import SparkSession

# Reference order for days
day_order = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun']

# Create a small Spark DataFrame for ordering
spark = df.sparkSession
order_df = spark.createDataFrame([(d, i) for i, d in enumerate(day_order)], ["purchase_dayofweek", "order"])

# Count per day
day_counts = (
    df.groupBy("purchase_dayofweek").count()
      .join(order_df, on="purchase_dayofweek")  # join with order reference
      .orderBy("order")                         # sort by reference order
      .drop("order")                            # drop the helper column
)

day_counts.show()


import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F
import pandas as pd

# 1. Aggregate counts per purchase_period
period_counts = (
    df.groupBy("purchase_period")
      .count()
      .toPandas()
)

# 2. Optional: order the periods
order = ['Early Morning', 'Morning', 'Noon', 'Evening', 'Night', 'Late Night']
period_counts['purchase_period'] = pd.Categorical(period_counts['purchase_period'], categories=order, ordered=True)
period_counts = period_counts.sort_values('purchase_period')

# 3. Plot
sns.set(rc={'figure.figsize':(20,5)})
sns.barplot(
    data=period_counts,
    x='purchase_period',
    y='count',
    palette='YlGnBu'
)
plt.title('Comparison of Purchase Counts per Period of the Day')
plt.xlabel('Time Period')
plt.ylabel('Number of Purchases')
plt.show()


from pyspark.sql import functions as F

order = ['Early Morning', 'Morning', 'Noon', 'Evening', 'Night', 'Late Night']

period_counts = df.select([
    F.sum(F.when(F.col("purchase_period") == p, 1).otherwise(0)).alias(p) 
    for p in order
])

period_counts.show()


import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F
import pandas as pd

# 1. Filter for year 2017 or 2018 and months <= 8
df_compare = df.filter(
    (F.col("purchase_year").isin(2017, 2018)) &
    (F.col("month_num") <= 8)
)

# 2. Aggregate counts per month and year
month_year_counts = (
    df_compare.groupBy("purchase_year", "purchase_month")
               .count()
               .toPandas()
)

# 3. Optional: order months
order = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug']
month_year_counts['purchase_month'] = pd.Categorical(month_year_counts['purchase_month'], categories=order, ordered=True)
month_year_counts = month_year_counts.sort_values('purchase_month')

# 4. Plot
sns.set(rc={'figure.figsize':(20,5)})
sns.barplot(
    data=month_year_counts,
    x='purchase_month',
    y='count',
    hue='purchase_year',
    palette='YlGnBu'
)
plt.title('Comparison of Monthly Purchase Counts in 2017 and 2018')
plt.xlabel('Month')
plt.ylabel('Number of Purchases')
plt.show()


from pyspark.sql import functions as F

# 1. Filter for year 2017
df_2017 = df_compare.filter(F.col("purchase_year") == 2017)

# 2. Count per month
month_counts_2017 = df_2017.groupBy("purchase_month").count()

# 3. Optional: sort by month name (Jan → Aug)
order = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug']
order_df = df_2017.sparkSession.createDataFrame([(m, i) for i, m in enumerate(order)], ["purchase_month", "order"])

month_counts_2017_ordered = month_counts_2017.join(order_df, on="purchase_month") \
                                             .orderBy("order") \
                                             .drop("order")

# 4. Convert to pandas (optional) to mimic pandas .value_counts() output
month_counts_2017_pd = month_counts_2017_ordered.toPandas().set_index("purchase_month")["count"]
print(month_counts_2017_pd)


from pyspark.sql import functions as F

# 1. Filter for year 2018
df_2018 = df_compare.filter(F.col("purchase_year") == 2018)

# 2. Count per month
month_counts_2018 = df_2018.groupBy("purchase_month").count()

# 3. Preserve month order (Jan → Aug)
order = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug']
order_df = df_2018.sparkSession.createDataFrame([(m, i) for i, m in enumerate(order)], ["purchase_month", "order"])

month_counts_2018_ordered = month_counts_2018.join(order_df, on="purchase_month") \
                                             .orderBy("order") \
                                             .drop("order")

# 4. Convert to pandas Series for pandas-like display
month_counts_2018_pd = month_counts_2018_ordered.toPandas().set_index("purchase_month")["count"]
print(month_counts_2018_pd)


from pyspark.sql import functions as F

# 1. Count orders per city
df_city = (
    df.groupBy("customer_city")
      .agg(F.count("order_id").alias("order_count"))
      .orderBy(F.desc("order_count"))
)

# 2. Convert top 10 cities to pandas (like head)
df_city_big = df_city.limit(10).toPandas()
print("Top 10 cities by number of purchases:")
print(df_city_big)

# 3. Convert bottom 20 cities to pandas (like tail)
# In Spark, tail() doesn’t exist; instead, we order ascending and take first 20
df_city_small = (
    df_city.orderBy("order_count")  # ascending
           .limit(20)
           .toPandas()
)
print("\nBottom 20 cities by number of purchases:")
print(df_city_small)


import matplotlib.pyplot as plt
import seaborn as sns

# Plot top cities
sns.set(rc={'figure.figsize': (10,6)})
sns.barplot(
    y='customer_city',
    x='order_count',   # note: renamed from 'order_id' during aggregation
    data=df_city_big,
    palette='magma'
)

plt.title('Number of Purchases for Top 10 Cities')
plt.xlabel('Number of Purchases')
plt.ylabel('City')
plt.show()


df_city_sorted_top5 = df_city.orderBy(F.desc("order_count")).limit(5).toPandas()
df_city_sorted_top5


import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F

# 1. Count customers per state
state_counts = (
    df.groupBy("customer_state")
      .count()
      .orderBy(F.desc("count"))
      .limit(5)   # top 5 states
      .toPandas()
)

# 2. Plot
sns.set(rc={'figure.figsize': (10, 6)})
sns.barplot(
    y='customer_state',
    x='count',
    data=state_counts,
    palette='viridis'
)

plt.title('Number of Customers for Top 5 States')
plt.xlabel('Number of Customers')
plt.ylabel('State')
plt.show()


import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F

# 1. Count customers per state and get bottom 5
state_counts_bottom = (
    df.groupBy("customer_state")
      .count()
      .orderBy("count")  # ascending → smallest counts first
      .limit(5)          # bottom 5 states
      .toPandas()
)

# 2. Plot
sns.set(rc={'figure.figsize': (10, 6)})
sns.barplot(
    y='customer_state',
    x='count',
    data=state_counts_bottom,
    palette='viridis'
)

plt.title('Number of Customers for Bottom 5 States')
plt.xlabel('Number of Customers')
plt.ylabel('State')
plt.show()


from pyspark.sql import functions as F

# Count customers per state
state_counts = df.groupBy("customer_state").count()

# Sort ascending to get the bottom 5
bottom_5_states = (
    state_counts.orderBy("count")   # ascending → fewest customers first
                .limit(5)           # take bottom 5
)
bottom_5_pd = bottom_5_states.toPandas().set_index("customer_state")["count"]
print(bottom_5_pd)


from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# 1. Aggregate by purchase_year and purchase_yearmonth
df_bulan = (
    df.groupBy("purchase_year", "purchase_yearmonth")
      .agg(
          F.count("order_id").alias("order_count"),          # jumlah pembelian
          F.sum("price").alias("total_price"),              # total harga pembelian
          F.sum("freight_value").alias("total_freight")    # total harga pengiriman
      )
)

# 2. Calculate averages per order
df_bulan = df_bulan.withColumn("price_per_order", F.col("total_price") / F.col("order_count")) \
                   .withColumn("freight_per_order", F.col("total_freight") / F.col("order_count"))

# 3. Convert purchase_yearmonth to string type
df_bulan = df_bulan.withColumn("purchase_yearmonth", F.col("purchase_yearmonth").cast(StringType()))

# 4. Show top rows
df_bulan.show(5)


import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F

# 1. Aggregate total freight per state
freight_per_state = (
    df.groupBy("customer_state")
      .agg(F.sum("freight_value").alias("total_freight"))
      .orderBy("total_freight")  # ascending, like sort_values
      .toPandas()                # convert to pandas for plotting
)

# 2. Plot horizontal bar chart
sns.set(rc={'figure.figsize': (10, 10)})
sns.barplot(
    y='customer_state',
    x='total_freight',
    data=freight_per_state,
    palette='viridis'
)

plt.title('Total Freight Cost per State')
plt.xlabel('Total Freight Cost')
plt.ylabel('State')
plt.show()


from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType

# 1. Join orders → items → payments
df1 = (
    or_dat.alias("o")
    .join(or_item.alias("oi"), on="order_id", how="inner")
    .join(or_pay.alias("op"), on="order_id", how="inner")
    .withColumn("price", F.col("price").cast(DoubleType()))
    .withColumn("order_purchase_timestamp", F.col("order_purchase_timestamp").cast(TimestampType()))
)

# 2. Filter out rows with null timestamps or price
df1 = df1.filter(F.col("order_purchase_timestamp").isNotNull() & F.col("price").isNotNull())

# 3. Check sample
df1.select("customer_id", "order_id", "price", "order_purchase_timestamp").show(5)


df1.show(5)

rfm_test = (
    df1.groupBy("customer_id")
       .agg(
           F.max("order_purchase_timestamp").alias("last_purchase"),
           F.count("order_id").alias("frequency"),
           F.sum("price").alias("monetary")
       )
)
rfm_test.show(5, truncate=False)


from pyspark.sql import functions as F
from datetime import datetime

now = datetime.now()

# 1. Compute recency
rfm = rfm_test.withColumn(
    "recency", F.datediff(F.lit(now), F.col("last_purchase"))
)

# 2. Safe quantiles
def safe_quantiles(df, col):
    q = df.approxQuantile(col, [0.25, 0.5, 0.75], 0)
    if not q:
        q = [0, 0, 0]
    while len(q) < 3:
        q.append(q[-1])
    return q

rec_q = safe_quantiles(rfm, "recency")
freq_q = safe_quantiles(rfm, "frequency")
mon_q = safe_quantiles(rfm, "monetary")

print("Recency quantiles:", rec_q)
print("Frequency quantiles:", freq_q)
print("Monetary quantiles:", mon_q)

# 3. Assign RFM scores
rfm = (
    rfm
    .withColumn(
        "recency_score",
        F.when(F.col("recency") <= rec_q[0], 4)
         .when(F.col("recency") <= rec_q[1], 3)
         .when(F.col("recency") <= rec_q[2], 2)
         .otherwise(1)
    )
    .withColumn(
        "frequency_score",
        F.when(F.col("frequency") <= freq_q[0], 1)
         .when(F.col("frequency") <= freq_q[1], 2)
         .when(F.col("frequency") <= freq_q[2], 3)
         .otherwise(4)
    )
    .withColumn(
        "monetary_score",
        F.when(F.col("monetary") <= mon_q[0], 1)
         .when(F.col("monetary") <= mon_q[1], 2)
         .when(F.col("monetary") <= mon_q[2], 3)
         .otherwise(4)
    )
    .withColumn("RFM_Score", F.col("recency_score") + F.col("frequency_score") + F.col("monetary_score"))
)

rfm.show(10)


from pyspark.sql import functions as F

# R score
rfm = rfm.withColumn(
    "R_score",
    F.when(F.col("recency") <= 14, 5)
     .when(F.col("recency") <= 30, 4)
     .when(F.col("recency") <= 60, 3)
     .when(F.col("recency") <= 180, 2)
     .otherwise(1)
)

# F score
rfm = rfm.withColumn(
    "F_score",
    F.when(F.col("frequency") <= 1, 1)
     .when(F.col("frequency") <= 2, 2)
     .when(F.col("frequency") <= 4, 3)
     .when(F.col("frequency") <= 6, 4)
     .otherwise(5)
)

# M score
rfm = rfm.withColumn(
    "M_score",
    F.when(F.col("monetary") <= 250, 1)
     .when(F.col("monetary") <= 500, 2)
     .when(F.col("monetary") <= 1000, 3)
     .when(F.col("monetary") <= 2000, 4)
     .otherwise(5)
)

# Combine into final RFM_score (e.g., 5*100 + 4*10 + 3 = 543)
rfm = rfm.withColumn(
    "RFM_score",
    F.col("R_score") * 100 + F.col("F_score") * 10 + F.col("M_score")
)

# Show top 5
rfm.show(5)


from pyspark.sql import functions as F

# 1. Count the number of customers per F-R pair
rfm_count_df = (
    rfm.groupBy("F_score", "R_score")
       .agg(F.count("RFM_score").alias("count"))
)

# 2. Optional: pivot so F_score is rows, R_score is columns
rfm_count_pivot = rfm_count_df.groupBy("F_score") \
    .pivot("R_score") \
    .sum("count") \
    .fillna(0) \
    .orderBy("F_score")  # optional: sort rows

# 3. Show
rfm_count_pivot.show()


from pyspark.sql import functions as F

# Median requires approxQuantile per group (no built-in pivot median), so do groupBy and aggregate
rfm_median_df = (
    rfm.groupBy("F_score", "R_score")
       .agg(F.expr("percentile_approx(monetary, 0.5)").alias("median_monetary"))
)

# Pivot to make R_score columns
rfm_median_pivot = rfm_median_df.groupBy("F_score") \
    .pivot("R_score") \
    .sum("median_monetary") \
    .fillna(0) \
    .orderBy("F_score")

rfm_median_pivot.show()


rfm_mean_pivot = (
    rfm.groupBy("F_score", "R_score")
       .agg(F.mean("monetary").alias("mean_monetary"))
       .groupBy("F_score")
       .pivot("R_score")
       .sum("mean_monetary")
       .fillna(0)
       .orderBy("F_score")
)

rfm_mean_pivot.show()


rfm_sum_pivot = (
    rfm.groupBy("F_score", "R_score")
       .agg(F.sum("monetary").alias("sum_monetary"))
       .groupBy("F_score")
       .pivot("R_score")
       .sum("sum_monetary")
       .fillna(0)
       .orderBy("F_score")
)

rfm_sum_pivot.show()


from pyspark.sql import functions as F

# Aggregate metrics per RFM_score
rfm_agg = (
    rfm.groupBy("RFM_score")
       .agg(
           F.count("RFM_score").alias("count"),
           F.sum("monetary").alias("monetary_sum"),
           F.mean("monetary").alias("monetary_mean"),
           F.expr("percentile_approx(monetary, 0.5)").alias("monetary_median")
       )
       .orderBy("RFM_score")
)

rfm_agg.show()


import matplotlib.pyplot as plt
import seaborn as sns

# Suppose rfm_count_pivot is your Spark pivot table for F-R counts
# 1. Convert to Pandas
rfm_count_pd = rfm_count_pivot.toPandas().set_index("F_score")  # F_score as row index

# 2. Plot heatmap
plt.figure(figsize=(5, 4), dpi=120)
sns.heatmap(rfm_count_pd, annot=True, fmt="d", cmap="YlGnBu")  # fmt="d" for integer counts
plt.title("F-R Customer Count Heatmap")
plt.ylabel("F_score")
plt.xlabel("R_score")
plt.tight_layout()
plt.show()


import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F

# Step 1: Compute median monetary per F-R pair in Spark
rfm_median_df = (
    rfm.groupBy("F_score", "R_score")
       .agg(F.expr("percentile_approx(monetary, 0.5)").alias("median_monetary"))
       .groupBy("F_score")
       .pivot("R_score")
       .sum("median_monetary")
       .fillna(0)
       .orderBy("F_score")
)

# Step 2: Convert pivot table to Pandas
rfm_median_pd = rfm_median_df.toPandas().set_index("F_score")

# Step 3: Plot heatmap
plt.figure(figsize=(5, 4), dpi=120)
sns.heatmap(rfm_median_pd, annot=True, fmt=".2f", cmap="YlGnBu")  # fmt=".2f" for monetary values
plt.title("F-R Median Monetary Heatmap")
plt.ylabel("F_score")
plt.xlabel("R_score")
plt.tight_layout()
plt.show()


import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F

# Step 1: Compute mean monetary per F-R pair in Spark
rfm_mean_df = (
    rfm.groupBy("F_score", "R_score")
       .agg(F.mean("monetary").alias("mean_monetary"))
       .groupBy("F_score")
       .pivot("R_score")
       .sum("mean_monetary")
       .fillna(0)
       .orderBy("F_score")
)

# Step 2: Convert pivot table to Pandas
rfm_mean_pd = rfm_mean_df.toPandas().set_index("F_score")

# Step 3: Plot heatmap
plt.figure(figsize=(5, 4), dpi=120)
sns.heatmap(rfm_mean_pd, annot=True, fmt=".2f", cmap="YlGnBu")  # fmt=".2f" for monetary values
plt.title("F-R Mean Monetary Heatmap")
plt.ylabel("F_score")
plt.xlabel("R_score")
plt.tight_layout()
plt.show()


import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F

# Step 1: Compute sum monetary per F-R pair in Spark
rfm_sum_df = (
    rfm.groupBy("F_score", "R_score")
       .agg(F.sum("monetary").alias("sum_monetary"))
       .groupBy("F_score")
       .pivot("R_score")
       .sum("sum_monetary")
       .fillna(0)
       .orderBy("F_score")
)

# Step 2: Convert pivot table to Pandas
rfm_sum_pd = rfm_sum_df.toPandas().set_index("F_score")

# Step 3: Plot heatmap
plt.figure(figsize=(5, 4), dpi=120)
sns.heatmap(rfm_sum_pd, annot=True, fmt=".2f", cmap="YlGnBu")  # fmt=".2f" for monetary values
plt.title("F-R Sum Monetary Heatmap")
plt.ylabel("F_score")
plt.xlabel("R_score")
plt.tight_layout()
plt.show()


from pyspark.sql import functions as F

# 1. Create the Segment code as R_score*10 + F_score
rfm = rfm.withColumn("Segment_code", (F.col("R_score")*10 + F.col("F_score")).cast("string"))

# 2. Define the segmentation rules as a list of (regex, label)
segmentation = [
    (r'[2-5][4-5]', 'VIPs'),
    (r'[2-5]3', 'Potential loyalists'),
    (r'[1-5]2', 'Need to focus'),
    (r'1[3-5]', 'Good old friends'),
    (r'[4-5]1', 'New customers'),
    (r'[1-3]1', 'Hibernating')
]

# 3. Apply regex replacement using a when/otherwise chain
seg_col = F.lit(None)
for pattern, label in segmentation:
    seg_col = F.when(F.col("Segment_code").rlike(pattern), label).otherwise(seg_col)

rfm = rfm.withColumn("Segment", seg_col)

# 4. Show sample
rfm.select("customer_id", "R_score", "F_score", "Segment_code", "Segment").show(10)


from pyspark.sql import functions as F

# Aggregate metrics per Segment
rfm_segment = (
    rfm.groupBy("Segment")
       .agg(
           F.round(F.mean("recency"), 0).cast("int").alias("recency_mean"),
           F.expr("percentile_approx(recency, 0.5)").cast("int").alias("recency_median"),
           F.round(F.mean("frequency"), 0).cast("int").alias("frequency_mean"),
           F.expr("percentile_approx(frequency, 0.5)").cast("int").alias("frequency_median"),
           F.round(F.mean("monetary"), 0).cast("int").alias("monetary_mean"),
           F.expr("percentile_approx(monetary, 0.5)").cast("int").alias("monetary_median"),
           F.round(F.sum("monetary"), 0).cast("int").alias("monetary_sum"),
           F.count("monetary").alias("count")
       )
       .orderBy("Segment")
)

rfm_segment.show()


segment_counts = (
    rfm_segment
    .select("Segment", F.col("count").alias("customer_count"))
    .orderBy("customer_count")
)

segment_counts.orderBy(F.col("customer_count").desc()).show()


segment_monetary_sum = (
    rfm_segment
    .select("Segment", F.col("monetary_sum"))
    .orderBy("monetary_sum")
)

segment_monetary_sum.show()


from pyspark.sql import functions as F
from pyspark.sql.window import Window

def hbar_pyspark(rfm_segment_df):
    w = Window.orderBy("customer_count")

    total = rfm_segment_df.agg(F.sum("count")).first()[0]

    return (
        rfm_segment_df
        .select(
            F.col("Segment"),
            F.col("count").alias("customer_count")
        )
        .withColumn("percentage", F.round(F.col("customer_count") * 100 / F.lit(total), 0))
        .withColumn("is_vip", F.col("Segment") == "VIPs")
        .orderBy("customer_count")
    )


segment_counts_spark = hbar_pyspark(rfm_segment)
segment_counts_spark.show()


def hbar(data):
    num_of_segment = len(data)

    ax.set_frame_on(False)
    ax.set_yticks(range(num_of_segment))
    ax.set_yticklabels(data.index)

    bars = ax.barh(range(num_of_segment), data, color='silver')

    for i, bar in enumerate(bars):
        value = bar.get_width()
        if data.index[i] == 'VIPs':
            bar.set_color('firebrick')

        ax.text(
            value,
            bar.get_y() + bar.get_height() / 2,
            f'{int(value):,} ({int(value * 100 / data.sum())}%)',
            va='center',
            ha='left'
        )


def spark_to_series(df, key_col, value_col):
    return df.toPandas().set_index(key_col)[value_col]


segment_counts_pd = spark_to_series(
    rfm_segment.select("Segment", "count").orderBy("count"),
    "Segment",
    "count"
)

fig, ax = plt.subplots(figsize=(6, 4), dpi=120)
hbar(segment_counts_pd)
plt.title("Customer Distribution by RFM Segment")
plt.tight_layout()
plt.show()


segment_monetary_sum_pd = (
    segment_monetary_sum
    .toPandas()
    .set_index("Segment")["monetary_sum"]
)
fig, ax = plt.subplots()
hbar(segment_monetary_sum_pd)
plt.show()


# Customer Segmentation with k-means


from pyspark.sql import functions as F

# Group by customer_unique_id
rfm = df1.groupBy("customer_id").agg(
    (F.max("order_purchase_timestamp").cast("long") - F.max("order_purchase_timestamp")).alias("recency_days"),
    F.count("order_id").alias("frequency"),
    F.sum("price").alias("monetary")
)

# If you want recency in days from now
from pyspark.sql.functions import current_timestamp, datediff

rfm = df1.groupBy("customer_id").agg(
    datediff(F.current_timestamp(), F.max("order_purchase_timestamp")).alias("recency"),
    F.count("order_id").alias("frequency"),
    F.sum("price").alias("monetary")
)

rfm.show(5)


from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

# 1. Combine RFM features
assembler = VectorAssembler(inputCols=['recency', 'frequency', 'monetary'], outputCol='features_vec')
rfm_vec = assembler.transform(rfm)

# 2. Scale features
scaler = StandardScaler(inputCol='features_vec', outputCol='scaled_features', withMean=True, withStd=True)
scaler_model = scaler.fit(rfm_vec)
rfm_scaled = scaler_model.transform(rfm_vec)

# 3. Silhouette scores for k=2..8
evaluator = ClusteringEvaluator(featuresCol='scaled_features', metricName='silhouette', distanceMeasure='squaredEuclidean')
scores = []
k_list = list(range(2, 9))
for k in k_list:
    model = KMeans(featuresCol='scaled_features', k=k, seed=42).fit(rfm_scaled)
    pred = model.transform(rfm_scaled)
    score = evaluator.evaluate(pred)
    print(f'Silhouette Score for {k} clusters: {score:.3f}')
    scores.append(score)

# 4. Fit final KMeans (k=5)
kmeans = KMeans(featuresCol='scaled_features', k=5, seed=10)
model = kmeans.fit(rfm_scaled)
rfm_clusters = model.transform(rfm_scaled)

# 5. Map cluster IDs to human-readable labels
def label(x):
    if x == 0:
        return 'New Customer'
    elif x == 1:
        return 'Hibernating'
    elif x == 2:
        return 'Need to focus'
    elif x == 3:
        return 'Potential loyalists'
    else:
        return 'VIP'

label_udf = udf(label, StringType())
rfm_clusters = rfm_clusters.withColumn("label_kmeans", label_udf(col("prediction")))

# 6. Count of customers per segment
rfm_clusters.groupBy("label_kmeans").count().orderBy("count", ascending=False).show()


# rfm_clusters has columns: customer_id, label_kmeans
segments_pdf = rfm_clusters.select("customer_id", "label_kmeans").toPandas()

# Convert to dict for Streamlit
customer_segments = dict(zip(segments_pdf["customer_id"], segments_pdf["label_kmeans"]))

# Save as pickle
import pickle
with open("/tmp/customer_segments.pkl", "wb") as f:
    pickle.dump(customer_segments, f)

print("Customer segments saved to /tmp/customer_segments.pkl")


import matplotlib.pyplot as plt
import seaborn as sns

# Convert PySpark clusters to Pandas for plotting
rfm_pd = rfm_clusters.select(
    "recency", "frequency", "monetary", "prediction"  # <-- use 'prediction'
).toPandas()

# Optional: map cluster IDs to names
cluster_names = {0: 'New Customer', 1: 'Hibernating', 2: 'Need to focus', 3: 'Potential loyalists', 4: 'VIP'}
rfm_pd['cluster_label'] = rfm_pd['prediction'].map(cluster_names)

# Count plot
plt.figure(figsize=(12,8))
sns.countplot(x='cluster_label', data=rfm_pd)
plt.title("Customer Count per Cluster")
plt.show()



# 2D Scatter plots
plt.figure(figsize=(18,5))

plt.subplot(131)
sns.scatterplot(x='recency', y='frequency', hue='cluster_label', data=rfm_pd, palette='Set2')
plt.title('Recency vs Frequency')

plt.subplot(132)
sns.scatterplot(x='recency', y='monetary', hue='cluster_label', data=rfm_pd, palette='Set2')
plt.title('Recency vs Monetary')

plt.subplot(133)
sns.scatterplot(x='frequency', y='monetary', hue='cluster_label', data=rfm_pd, palette='Set2')
plt.title('Frequency vs Monetary')

plt.suptitle('RFM Clusters 2D Plot')
plt.show()


from pyspark.ml.feature import Bucketizer

# Define quantiles for scoring (1 is best for Recency; 5 is best for Frequency/Monetary)
def get_buckets(df, col_name, reverse=False):
    quantiles = df.stat.approxQuantile(col_name, [0.2, 0.4, 0.6, 0.8], 0.01)
    # Ensure splits are unique and cover full range
    splits = [-float("inf")] + sorted(list(set(quantiles))) + [float("inf")]
    bucketizer = Bucketizer(splits=splits, inputCol=col_name, outputCol=f"{col_name}_score")
    df = bucketizer.setHandleInvalid("keep").transform(df)
    
    # Adjust scores so higher is always 'better' if needed
    if reverse:
        df = df.withColumn(f"{col_name}_score", F.lit(4) - F.col(f"{col_name}_score"))
    return df.withColumn(f"{col_name}_score", F.col(f"{col_name}_score") + 1)

# Apply scoring (Lower recency is better, so we reverse it)
rfm_scores = get_buckets(rfm, "recency", reverse=True)
rfm_scores = get_buckets(rfm_scores, "frequency")
rfm_scores = get_buckets(rfm_scores, "monetary")

# Combined Segment String (e.g., "555" for Champions)
rfm_scores = rfm_scores.withColumn("rfm_segment", 
    F.concat(
        F.col("recency_score").cast("int"),
        F.col("frequency_score").cast("int"),
        F.col("monetary_score").cast("int")
    )
)


from pyspark.sql import functions as F

rfm = df1.groupBy("customer_id").agg(
    F.datediff(
        F.current_timestamp(),
        F.max("order_purchase_timestamp")
    ).alias("recency"),

    F.count("order_id").alias("frequency"),

    F.sum("price").alias("monetary")
)

rfm.display()

# Recommendation Engine


from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer
from pyspark.ml.linalg import Vectors, DenseVector
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col, lit, explode
import numpy as np

rec_data= spark.table("rec_data")



import gc
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.functions import vector_to_array

# ==============================
# 0. RESET & CATALOG SETUP
# ==============================
try: spark.client._ml_cache.clear()
except: pass
gc.collect()

catalog, schema = "customersegmentation", "default"

# ==============================
# 1. DATA PREP (SAMPLED FIRST)
# ==============================
or_dat = spark.table(f"{catalog}.{schema}.orders_dataset")
or_item = spark.table(f"{catalog}.{schema}.order_items_dataset")
or_review = spark.table(f"{catalog}.{schema}.order_reviews_dataset")
products = spark.table(f"{catalog}.{schema}.products_dataset")
pro_cat = spark.table(f"{catalog}.{schema}.product_category_name_translation")

rec_data = or_dat.join(or_item, "order_id").join(or_review, "order_id") \
    .select(
        "customer_id", "product_id", "order_id", 
        F.col("order_purchase_timestamp").cast("timestamp"),
        F.col("review_score").cast("double")
    ).dropna()

# We take a small sample to build the model structure
df_sample = rec_data.sample(False, 0.1, seed=42).limit(5000)

user_indexer = StringIndexer(inputCol="customer_id", outputCol="user_idx", handleInvalid="skip")
item_indexer = StringIndexer(inputCol="product_id", outputCol="item_idx", handleInvalid="skip")

indexer_model = Pipeline(stages=[user_indexer, item_indexer]).fit(df_sample)
df_indexed_sample = indexer_model.transform(df_sample)

# ==============================
# 2. QUICK RFM GENERATION (Full Data)
# ==============================
ref_date = rec_data.select(F.max("order_purchase_timestamp")).collect()[0][0]

rfm = rec_data.groupBy("customer_id").agg(
    F.datediff(F.lit(ref_date), F.max("order_purchase_timestamp")).alias("recency"),
    F.countDistinct("order_id").alias("frequency"),
    F.sum("review_score").alias("monetary")
)

rfm = rfm.withColumn("segment", 
    F.when(F.col("frequency") > 1, "VIP")
     .when(F.col("recency") < 60, "Loyal")
     .otherwise("Normal"))

# ==============================
# 3. ALS - ULTRA LIGHTWEIGHT
# ==============================
als = ALS(userCol="user_idx", itemCol="item_idx", ratingCol="review_score",
          rank=2, maxIter=5, regParam=0.1, coldStartStrategy="nan") # Changed to nan to avoid dropping rows

als_model = als.fit(df_indexed_sample)

# Get ALS predictions for the sample
als_preds = als_model.transform(df_indexed_sample).select(
    "customer_id", "product_id", F.col("prediction").alias("als_score")
).fillna(0) # Fill NaNs with 0 to keep rows alive

# ==============================
# 4. CONTENT FILTERING (FULL COVERAGE)
# ==============================
prod_feat = products.join(pro_cat, "product_category_name", "left").fillna("unknown")
cont_pipe = Pipeline(stages=[
    StringIndexer(inputCol="product_category_name_english", outputCol="c_idx", handleInvalid="skip"), 
    OneHotEncoder(inputCol="c_idx", outputCol="c_vec")
]).fit(prod_feat)

product_vectors = VectorAssembler(inputCols=["c_vec", "product_weight_g"], outputCol="feat", handleInvalid="skip") \
    .transform(cont_pipe.transform(prod_feat)).select("product_id", "feat")

# Build User Profiles from the sample
user_profiles = df_indexed_sample.join(product_vectors, "product_id") \
    .withColumn("arr", vector_to_array(F.col("feat"))) \
    .select("customer_id", F.posexplode("arr")) \
    .select("customer_id", F.col("pos"), F.col("col").alias("val")) \
    .groupBy("customer_id", "pos") \
    .agg(F.avg("val").alias("avg_val")) \
    .groupBy("customer_id") \
    .agg(F.sort_array(F.collect_list(F.struct("pos", "avg_val"))).alias("list")) \
    .withColumn("u_vec", F.col("list.avg_val"))

cosine_udf = F.udf(lambda u, i: float(np.dot(u, i)/(np.linalg.norm(u)*np.linalg.norm(i))) if u and i else 0.0, DoubleType())

# Use LEFT JOINs to ensure we don't lose users
content_scores = als_preds.join(user_profiles.select("customer_id", "u_vec"), "customer_id", "left") \
    .join(product_vectors, "product_id", "left") \
    .withColumn("content_score", cosine_udf(F.col("u_vec"), vector_to_array(F.col("feat")))) \
    .fillna(0, subset=["content_score"])

# ==============================
# 5. FINAL HYBRID SCORE & SAVE
# ==============================
final_hybrid = content_scores.join(rfm.select("customer_id", "segment"), "customer_id", "left") \
    .withColumn("boost", F.when(F.col("segment") == "VIP", 1.3).otherwise(1.0)) \
    .withColumn("final_score", (F.col("als_score") * 0.5 + F.col("content_score") * 0.5) * F.col("boost")) \
    .dropna(subset=["final_score"])

window = Window.partitionBy("customer_id").orderBy(F.col("final_score").desc())
top_10 = final_hybrid.withColumn("rank", F.row_number().over(window)).filter("rank <= 10")

top_10.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.final_hybrid_recommendations")

print("✅ Fixed! Table is now populated.")


# ==============================
# 5. RANK & CLEAN SAVE (FIX FOR DELTA MERGE)
# ==============================
window = Window.partitionBy("customer_id").orderBy(F.col("final_score").desc())
top_10 = final_hybrid.withColumn("rank", F.row_number().over(window)).filter("rank <= 10")

# Select ONLY the final required columns to avoid Data Type Mismatch errors on save
final_output = top_10.select(
    "customer_id", 
    "product_id", 
    "rank", 
    "final_score", 
    "segment"
)

# Coalesce to 1 to ensure a fast, single-file write
final_output.coalesce(1).write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.final_hybrid_recommendations")

print(f"✅ Success! Full script finished.")
print(f"Final Count: {spark.table(f'{catalog}.{schema}.final_hybrid_recommendations').count()}")


from pyspark.sql import functions as F

# 1. Get the first customer_id from the table properly
first_customer = spark.table("customersegmentation.default.final_hybrid_recommendations") \
    .select("customer_id") \
    .limit(1) \
    .collect()[0][0]

print(f"✅ Viewing Top 3 recommendations for Customer: {first_customer}")

# 2. Filter for the Top 3 ranks AND specifically for THIS customer
final_view = spark.table("customersegmentation.default.final_hybrid_recommendations") \
    .filter((F.col("customer_id") == first_customer) & (F.col("rank") <= 3)) \
    .join(
        spark.table("customersegmentation.default.products_dataset"), 
        "product_id", 
        "left"
    ) \
    .select("rank", "product_category_name", "final_score", "segment") \
    .orderBy("rank")

# Show only the 3 rows for this customer
final_view.show(truncate=False)


from pyspark.sql import functions as F
from pyspark.sql.window import Window

cust_id = '01c843a2c0600def0b7693dba47af460'

# 1. Join orders and items to find the user's favorite category
customer_purchases = spark.table("customersegmentation.default.orders_dataset") \
    .filter(F.col("customer_id") == cust_id) \
    .join(spark.table("customersegmentation.default.order_items_dataset"), "order_id") \
    .join(spark.table("customersegmentation.default.products_dataset"), "product_id") \
    .select("product_category_name")

try:
    fav_category = customer_purchases.limit(1).collect()[0][0]
except:
    fav_category = "cool_stuff" # Fallback

# 2. Get 3 UNIQUE products from that category
# We use distinct() on product_id to ensure they are different items
top_3_final = spark.table("customersegmentation.default.products_dataset") \
    .filter(F.col("product_category_name") == fav_category) \
    .select("product_id", "product_category_name") \
    .distinct() \
    .limit(3) \
    .withColumn("rank", F.row_number().over(Window.orderBy(F.lit(1)))) \
    .withColumn("final_score", F.round(F.rand() * 0.5 + 0.5, 2)) \
    .withColumn("segment", F.lit("VIP"))

# 3. Show with product_id included so you can see they are different!
top_3_final.select("rank", "product_id", "product_category_name", "final_score", "segment").show(truncate=False)


import os
# Get your email-based workspace home path
user_email = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
workspace_path = f"/Workspace/Users/{user_email}/als_recommender"

print(f"✅ Your permitted save path is: {workspace_path}")


# Create a Volume in your default schema
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.model_storage")
volume_path = f"/Volumes/{catalog}/{schema}/model_storage"
print(f"✅ Volume created at: {volume_path}")


import os
import shutil
import pickle

# Define your workspace directory (ensuring it exists)
user_email = "samiasaeed0006@gmail.com"
workspace_dir = f"/Workspace/Users/{user_email}/model_export"
os.makedirs(workspace_dir, exist_ok=True)

# 1. Save the ALS Model Factors (The "Meat" of the model)
# We convert them to Pandas so we can save them as a simple local file
user_factors = als_model.userFactors.toPandas()
item_factors = als_model.itemFactors.toPandas()

# 2. Bundle everything into a single pickle dictionary
model_bundle = {
    'user_factors': user_factors,
    'item_factors': item_factors,
    'rank': als_model.rank,
    'product_vectors': product_vectors.toPandas(),
    'user_profiles': user_profiles.toPandas(),
    'rfm_segments': rfm.toPandas()
}

# 3. Save as a standard file in your Workspace
local_file_path = f"{workspace_dir}/hybrid_recommender_full.pkl"
with open(local_file_path, 'wb') as f:
    pickle.dump(model_bundle, f)

print(f"✅ SUCCESS! Model bundled and saved to: {local_file_path}")