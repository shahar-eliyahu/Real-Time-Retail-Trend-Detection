import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import *
from pyspark.ml.functions import vector_to_array
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.streaming.state import GroupStateTimeout
import numpy as np
import pandas as pd


# -----------------------------
# 1) Spark Session 
# -----------------------------
spark = SparkSession.builder \
    .appName("RetailTrendStreaming_Stateful") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# -----------------------------
# 2) Load Pre-trained ML Model
# -----------------------------
stage1_model = RandomForestClassificationModel.load("trend_rf_model")

# Thresholds and Constants
STAGE1_THRESHOLD = 0.25
EPS = 1e-6
RATIO_CAP = 20.0
STD_FLOOR = 0.1

# -----------------------------
# 3) Schemas
# -----------------------------
schema = StructType([
    StructField("ProductID",   StringType()),
    StructField("Quantity",    DoubleType()),
    StructField("UnitPrice",   DoubleType()),
    StructField("Revenue",     DoubleType()),
    StructField("CustomerID",  StringType()),
    StructField("Country",     StringType()),
    StructField("IsReturn",    StringType()),
    StructField("InvoiceDate", StringType()),
    StructField("Y",           IntegerType()),
])

state_schema = StructType([
    StructField("tx_history",       ArrayType(DoubleType())),
    StructField("qty_history",      ArrayType(DoubleType())),
    StructField("price_history",    ArrayType(DoubleType())),
    StructField("customer_history", ArrayType(DoubleType())),
    StructField("country_history",  ArrayType(DoubleType())),
    StructField("last_day",         StringType()),
])

output_schema = StructType([
    StructField("ProductID",              StringType()),
    StructField("Day",                    StringType()),
    StructField("Y",                      IntegerType()),
    StructField("tx_cnt",                 DoubleType()),
    StructField("qty_sum",                DoubleType()),
    StructField("unitprice_mean",         DoubleType()),
    StructField("customers_cnt",          DoubleType()),
    StructField("countries_cnt",          DoubleType()),
    StructField("rm_3",                   DoubleType()),
    StructField("rm_7",                   DoubleType()),
    StructField("rs_14_safe",             DoubleType()),
    StructField("lag_1",                  DoubleType()),
    StructField("delta_1",                DoubleType()),
    StructField("growth_1",               DoubleType()),
    StructField("sum_3",                  DoubleType()),
    StructField("slope_7",                DoubleType()),
    StructField("up_days_last_7",         DoubleType()),
    StructField("z_14",                   DoubleType()),
    StructField("trend_signal_7_14",      DoubleType()),
    StructField("rm7_vs_rm30",            DoubleType()),
    StructField("consec_up_3",            DoubleType()),
    StructField("slope_acceleration",     DoubleType()),
    StructField("weekday",                DoubleType()),
    StructField("month",                  DoubleType()),
    StructField("week_of_year",           DoubleType()),
    StructField("rq_30",                  DoubleType()),
    StructField("rq_14",                  DoubleType()),
    StructField("dist_to_p90",            DoubleType()),
    StructField("pct_above_rm",           DoubleType()),
    StructField("days_above_mean",        DoubleType()),
    StructField("price_lag_1",            DoubleType()),
    StructField("price_rm_7",             DoubleType()),
    StructField("price_ratio_7",          DoubleType()),
    StructField("qty_lag_1",              DoubleType()),
    StructField("qty_rm_7",               DoubleType()),
    StructField("qty_rm_14",              DoubleType()),
    StructField("qty_growth_1",           DoubleType()),
    StructField("qty_trend_signal",       DoubleType()),
    StructField("customers_delta_1",      DoubleType()),
    StructField("customers_per_tx",       DoubleType()),
    StructField("customer_burst_signal",  DoubleType()),
    StructField("customer_trend_signal",  DoubleType()),
    StructField("customers_rm14_vs_prev", DoubleType()),
    StructField("countries_rm_14",        DoubleType()),
    StructField("countries_per_tx",       DoubleType()),
    StructField("vr_14",                  DoubleType()),
    StructField("tx_dow_avg",             DoubleType()),
    StructField("tx_vs_dow",              DoubleType()),
])

feature_cols = [f.name for f in output_schema.fields if f.name not in ("ProductID", "Day", "Y")]

# -----------------------------
# 4) Feature Computation Helper
# -----------------------------
def cap(x): return min(float(x), RATIO_CAP)

def compute_features(p_id, d_str, tx, qty, pr, cust, countr, tx_h, qty_h, pr_h, cust_h, countr_h, y):
    dt = pd.to_datetime(d_str)

    def safe_mean(arr, n):
        w = arr[-n:] if len(arr) >= n else arr
        return float(np.mean(w)) if len(w) > 0 else 0.0
    def safe_std(arr, n):
        w = arr[-n:] if len(arr) >= n else arr
        return float(np.std(w)) if len(w) > 1 else STD_FLOOR
    def safe_p90(arr, n):
        w = arr[-n:] if len(arr) >= n else arr
        return float(np.percentile(w, 90)) if len(w) > 0 else 0.0

    rm_14 = safe_mean(tx_h, 14); rm_7 = safe_mean(tx_h, 7)
    rm_30 = safe_mean(tx_h, 30); rs_14_s = max(safe_std(tx_h, 14), STD_FLOOR)
    lag_1 = tx_h[-1] if tx_h else 0.0

    return {
        "ProductID": p_id, "Day": d_str, "Y": int(y),
        "tx_cnt": tx, "qty_sum": qty, "unitprice_mean": pr,
        "customers_cnt": cust, "countries_cnt": countr,
        "rm_3": safe_mean(tx_h, 3), "rm_7": rm_7, "rs_14_safe": rs_14_s,
        "lag_1": lag_1, "delta_1": tx - lag_1, "growth_1": cap(tx / (lag_1 + EPS)),
        "sum_3": float(np.sum(tx_h[-3:]) if tx_h else 0.0), "slope_7": rm_7 - rm_14,
        "up_days_last_7": float(sum([1.0 for i in range(1, len(tx_h)) if tx_h[i] > tx_h[i-1]][-7:])),
        "z_14": (tx - rm_14) / rs_14_s, "trend_signal_7_14": cap(rm_7 / (rm_14 + EPS)),
        "rm7_vs_rm30": cap(rm_7 / (rm_30 + EPS)), "consec_up_3": 0.0,
        "slope_acceleration": 0.0, "weekday": float(dt.dayofweek + 1), "month": float(dt.month),
        "week_of_year": float(dt.isocalendar()[1]), "rq_30": safe_p90(tx_h, 30), "rq_14": safe_p90(tx_h, 14),
        "dist_to_p90": cap(tx / (safe_p90(tx_h, 30) + EPS)), "pct_above_rm": (tx - rm_14) / (rm_14 + EPS),
        "days_above_mean": float(sum(1 for v in tx_h[-7:] if v > rm_14)), "price_lag_1": pr_h[-1] if pr_h else pr,
        "price_rm_7": safe_mean(pr_h, 7), "price_ratio_7": cap(pr / (safe_mean(pr_h, 7) + EPS)),
        "qty_lag_1": qty_h[-1] if qty_h else qty, "qty_rm_7": safe_mean(qty_h, 7), "qty_rm_14": safe_mean(qty_h, 14),
        "qty_growth_1": cap(qty / (qty_h[-1] + EPS) if qty_h else 1.0),
        "qty_trend_signal": cap(safe_mean(qty_h, 7) / (safe_mean(qty_h, 14) + EPS)),
        "customers_delta_1": cust - (cust_h[-1] if cust_h else cust), "customers_per_tx": cust / (tx + EPS),
        "customer_burst_signal": cap(cust / (safe_mean(cust_h, 14) + EPS)),
        "customer_trend_signal": cap(safe_mean(cust_h, 7) / (safe_mean(cust_h, 14) + EPS)),
        "customers_rm14_vs_prev": 1.0, "countries_rm_14": safe_mean(countr_h, 14),  # FIX: country_h -> countr_h
        "countries_per_tx": countr / (tx + EPS), "vr_14": cap(tx / (rm_14 + EPS)),
        "tx_dow_avg": rm_7, "tx_vs_dow": cap(tx / (rm_7 + EPS)),
    }

# -----------------------------
# 5) Stateful Streaming Implementation
# -----------------------------
def stateful_map_function(product_id, events, state):
    MAX_HISTORY = 60

    p_id = str(product_id[0]) if isinstance(product_id, (tuple, list)) else str(product_id)

    s = state.get if state.exists else ([], [], [], [], [], "")
    tx_h, qty_h, pr_h, cust_h, countr_h = list(s[0]), list(s[1]), list(s[2]), list(s[3]), list(s[4])

    events_df = pd.concat(list(events), ignore_index=True)
    events_df["Day"] = events_df["InvoiceDate"].str[:10]

    results = []

    for day_str, day_df in events_df.sort_values("InvoiceDate").groupby("Day"):
        tx, qty = float(len(day_df)), float(day_df["Quantity"].sum())
        pr, cust = float(day_df["UnitPrice"].mean()), float(day_df["CustomerID"].nunique())
        countr, y = float(day_df["Country"].nunique()), int(day_df["Y"].max())

        results.append(compute_features(p_id, day_str, tx, qty, pr, cust, countr, tx_h, qty_h, pr_h, cust_h, countr_h, y))

        tx_h.append(tx); tx_h = tx_h[-MAX_HISTORY:]
        qty_h.append(qty); qty_h = qty_h[-MAX_HISTORY:]
        pr_h.append(pr); pr_h = pr_h[-MAX_HISTORY:]
        cust_h.append(cust); cust_h = cust_h[-MAX_HISTORY:]
        countr_h.append(countr); countr_h = countr_h[-MAX_HISTORY:]  # FIX: country_h -> countr_h

    state.update((tx_h, qty_h, pr_h, cust_h, countr_h, day_str))
    yield pd.DataFrame(results)

# -----------------------------
# 6) Streaming Pipeline Execution
# -----------------------------
raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "retail_stream") \
    .option("startingOffsets", "earliest") \
    .load()

parsed = raw.select(from_json(col("value").cast("string"), schema).alias("d")) \
    .select("d.*") \
    .withColumn("ts", to_timestamp("InvoiceDate")) \
    .withWatermark("ts", "1 day")

feature_stream = parsed.groupBy("ProductID").applyInPandasWithState(
    stateful_map_function,
    output_schema,
    state_schema,
    "append",
    GroupStateTimeout.NoTimeout
)

vector_stream = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features",
    handleInvalid="skip"
).transform(feature_stream)

alerts = stage1_model.transform(vector_stream) \
    .withColumn("p_trend", vector_to_array(col("probability"))[1]) \
    .filter(col("p_trend") >= STAGE1_THRESHOLD)

# -----------------------------
# 7) Final Query with Checkpointing
# -----------------------------
query = alerts.select("Day", "ProductID", "Y") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "checkpoint/retail_streaming") \
    .trigger(processingTime="10 seconds") \
    .start()

print("Stateful Streaming pipeline started. Monitoring 'retail_stream' for trends")
query.awaitTermination()
