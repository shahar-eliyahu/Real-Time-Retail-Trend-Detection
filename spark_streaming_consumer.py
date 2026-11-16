import os
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, to_json, struct, round as _round
from pyspark.sql.types import *
from pyspark.ml.functions import vector_to_array
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.streaming.state import GroupStateTimeout


# -----------------------------
# Configuration
# -----------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "retail_stream")
SCORES_TOPIC = os.getenv("SCORES_TOPIC", "trend_scores")  # every scored product-day, read by the dashboard
MODEL_PATH = os.getenv("MODEL_PATH", "trend_rf_model")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "checkpoint/retail_streaming")

# Thresholds and constants (identical to the training notebook)
STAGE1_THRESHOLD = 0.25
EPS = 1e-6
RATIO_CAP = 20.0
STD_FLOOR = 0.1

# Longest look-back used by any feature:
#   customers_rm_prev60 -> rows -60..-31, tx_dow_avg -> last 30 rows of the same weekday
MAX_HISTORY = 400
# Training dropped rows whose look-back windows were still empty
# (customers_rm_prev60 needs at least 31 previous active days)
MIN_HISTORY = 31


# -----------------------------
# Schemas
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

# Per-product state:
#   *_history  -> one entry per completed active day (same rows the training windows ran over)
#   pend_*     -> the day currently being accumulated; a day can span several micro-batches,
#                 so it is only turned into features once a later day arrives
state_schema = StructType([
    StructField("tx_history",       ArrayType(DoubleType())),
    StructField("qty_history",      ArrayType(DoubleType())),
    StructField("price_history",    ArrayType(DoubleType())),
    StructField("customer_history", ArrayType(DoubleType())),
    StructField("country_history",  ArrayType(DoubleType())),
    StructField("weekday_history",  ArrayType(DoubleType())),
    StructField("pend_day",         StringType()),
    StructField("pend_tx",          DoubleType()),
    StructField("pend_qty",         DoubleType()),
    StructField("pend_price_sum",   DoubleType()),
    StructField("pend_customers",   ArrayType(StringType())),
    StructField("pend_countries",   ArrayType(StringType())),
    StructField("pend_y",           IntegerType()),
])

feature_cols = [
    "tx_cnt", "qty_sum", "unitprice_mean", "customers_cnt", "countries_cnt",
    "rm_3", "rm_7", "rs_14_safe",
    "lag_1", "delta_1", "growth_1", "sum_3",
    "slope_7", "up_days_last_7", "z_14",
    "trend_signal_7_14",
    "rm7_vs_rm30",
    "consec_up_3",
    "slope_acceleration",
    "weekday", "month", "week_of_year",
    "rq_30", "rq_14", "dist_to_p90", "pct_above_rm", "days_above_mean",
    "price_lag_1", "price_rm_7", "price_ratio_7",
    "qty_lag_1", "qty_rm_7", "qty_rm_14", "qty_growth_1", "qty_trend_signal",
    "customers_delta_1", "customers_per_tx",
    "customer_burst_signal", "customer_trend_signal",
    "customers_rm14_vs_prev",
    "countries_rm_14", "countries_per_tx",
    "vr_14",
    "tx_dow_avg", "tx_vs_dow",
]

output_schema = StructType(
    [StructField("ProductID", StringType()),
     StructField("Day", StringType()),
     StructField("Y", IntegerType())]
    + [StructField(c, DoubleType()) for c in feature_cols]
)


# -----------------------------
# Feature computation
# Mirrors the Spark window features of the training notebook:
# every window is rowsBetween(-n, -1) over the product's previous active days.
# -----------------------------
def cap(x):
    return min(float(x), RATIO_CAP)


def mean_last(arr, n):
    w = arr[-n:]
    return float(np.mean(w)) if len(w) > 0 else None


def mean_before(arr, i, n):
    """Mean of the n rows before row i (Spark: avg over rowsBetween(-n, -1) at row i)."""
    w = arr[max(0, i - n):i]
    return float(np.mean(w)) if len(w) > 0 else None


def p90_last(arr, n):
    # Spark percentile_approx returns an actual element of the window, never an interpolation
    return float(np.percentile(arr[-n:], 90, method="inverted_cdf"))


def spark_weekday(dt):
    # Spark dayofweek: Sunday=1 ... Saturday=7  (pandas dayofweek: Monday=0 ... Sunday=6)
    return float((dt.dayofweek + 1) % 7 + 1)


def compute_features(p_id, d_str, y, tx, qty, pr, cust, countr, tx_h, qty_h, pr_h, cust_h, countr_h, wd_h):
    n = len(tx_h)
    dt = pd.Timestamp(d_str)
    weekday = spark_weekday(dt)

    rm_3, rm_7 = mean_last(tx_h, 3), mean_last(tx_h, 7)
    rm_14, rm_30 = mean_last(tx_h, 14), mean_last(tx_h, 30)

    w14 = tx_h[-14:]
    rs_14 = float(np.std(w14, ddof=1)) if len(w14) > 1 else None  # Spark stddev = sample stddev
    rs_14_safe = STD_FLOOR if rs_14 is None or rs_14 < STD_FLOOR else rs_14

    lag_1, lag_2, lag_3 = tx_h[-1], tx_h[-2], tx_h[-3]
    delta_1 = tx - lag_1
    slope_7 = rm_7 - rm_14

    # is_up of a past row i = its own delta_1 > 0 (row 0 has no lag -> 0)
    up_days_last_7 = float(sum(1 for i in range(max(0, n - 7), n) if i > 0 and tx_h[i] > tx_h[i - 1]))

    consec_up_3 = float((tx > lag_1) and (lag_1 > lag_2) and (lag_2 > lag_3))

    # slope_7 as it was 3 rows ago
    j = n - 3
    lag_slope_3 = mean_before(tx_h, j, 7) - mean_before(tx_h, j, 14)

    # is_above_mean of a past row i = tx_i > rm_14 computed at row i
    days_above_mean = 0.0
    for i in range(max(0, n - 7), n):
        rm14_i = mean_before(tx_h, i, 14)
        if rm14_i is not None and tx_h[i] > rm14_i:
            days_above_mean += 1.0

    rq_30, rq_14 = p90_last(tx_h, 30), p90_last(tx_h, 14)

    price_rm_7 = mean_last(pr_h, 7)
    qty_lag_1 = qty_h[-1]
    qty_rm_7, qty_rm_14 = mean_last(qty_h, 7), mean_last(qty_h, 14)

    customers_rm_7, customers_rm_14 = mean_last(cust_h, 7), mean_last(cust_h, 14)
    customers_rm_prev60 = float(np.mean(cust_h[max(0, n - 60):n - 30]))

    same_dow = [t for t, w in zip(tx_h, wd_h) if w == weekday][-30:]
    tx_dow_avg = float(np.mean(same_dow))

    return {
        "ProductID": p_id, "Day": d_str, "Y": int(y),
        "tx_cnt": tx, "qty_sum": qty, "unitprice_mean": pr,
        "customers_cnt": cust, "countries_cnt": countr,
        "rm_3": rm_3, "rm_7": rm_7, "rs_14_safe": rs_14_safe,
        "lag_1": lag_1, "delta_1": delta_1, "growth_1": cap(tx / (lag_1 + EPS)),
        "sum_3": float(np.sum(tx_h[-3:])),
        "slope_7": slope_7, "up_days_last_7": up_days_last_7,
        "z_14": (tx - rm_14) / rs_14_safe,
        "trend_signal_7_14": cap(rm_7 / (rm_14 + EPS)),
        "rm7_vs_rm30": cap(rm_7 / (rm_30 + EPS)),
        "consec_up_3": consec_up_3,
        "slope_acceleration": slope_7 - lag_slope_3,
        "weekday": weekday, "month": float(dt.month), "week_of_year": float(dt.isocalendar()[1]),
        "rq_30": rq_30, "rq_14": rq_14,
        "dist_to_p90": cap(tx / (rq_30 + EPS)),
        "pct_above_rm": (tx - rm_14) / (rm_14 + EPS),
        "days_above_mean": days_above_mean,
        "price_lag_1": pr_h[-1], "price_rm_7": price_rm_7,
        "price_ratio_7": cap(pr / (price_rm_7 + EPS)),
        "qty_lag_1": qty_lag_1, "qty_rm_7": qty_rm_7, "qty_rm_14": qty_rm_14,
        "qty_growth_1": cap(qty / (qty_lag_1 + EPS)),
        "qty_trend_signal": cap(qty_rm_7 / (qty_rm_14 + EPS)),
        "customers_delta_1": cust - cust_h[-1],
        "customers_per_tx": cust / (tx + EPS),
        "customer_burst_signal": cap(cust / (customers_rm_14 + EPS)),
        "customer_trend_signal": cap(customers_rm_7 / (customers_rm_14 + EPS)),
        "customers_rm14_vs_prev": cap(customers_rm_14 / (customers_rm_prev60 + EPS)),
        "countries_rm_14": mean_last(countr_h, 14),
        "countries_per_tx": countr / (tx + EPS),
        "vr_14": cap(tx / (rm_14 + EPS)),
        "tx_dow_avg": tx_dow_avg, "tx_vs_dow": cap(tx / (tx_dow_avg + EPS)),
    }


# -----------------------------
# Stateful streaming logic
# -----------------------------
def stateful_map_function(product_id, events, state):
    p_id = str(product_id[0]) if isinstance(product_id, (tuple, list)) else str(product_id)

    if state.exists:
        s = state.get
        tx_h, qty_h, pr_h, cust_h, countr_h, wd_h = (list(x) for x in s[:6])
        pend_day, pend_tx, pend_qty, pend_price_sum = s[6], s[7], s[8], s[9]
        pend_customers, pend_countries, pend_y = set(s[10]), set(s[11]), s[12]
    else:
        tx_h, qty_h, pr_h, cust_h, countr_h, wd_h = [], [], [], [], [], []
        pend_day, pend_tx, pend_qty, pend_price_sum = None, 0.0, 0.0, 0.0
        pend_customers, pend_countries, pend_y = set(), set(), 0

    events_df = pd.concat(list(events), ignore_index=True)
    events_df["Day"] = events_df["InvoiceDate"].str[:10]

    results = []

    for day_str, day_df in events_df.sort_values("InvoiceDate").groupby("Day", sort=True):
        if pend_day is not None and day_str < pend_day:
            continue  # late event for a day that is already closed

        if pend_day is not None and day_str > pend_day:
            # The pending day is complete -> compute its features, then push it into history
            tx, qty = pend_tx, pend_qty
            pr = pend_price_sum / pend_tx
            cust, countr = float(len(pend_customers)), float(len(pend_countries))

            has_dow = spark_weekday(pd.Timestamp(pend_day)) in wd_h[-MAX_HISTORY:]
            if len(tx_h) >= MIN_HISTORY and has_dow:
                results.append(compute_features(p_id, pend_day, pend_y, tx, qty, pr, cust, countr,
                                                tx_h, qty_h, pr_h, cust_h, countr_h, wd_h))

            tx_h.append(tx); qty_h.append(qty); pr_h.append(pr)
            cust_h.append(cust); countr_h.append(countr)
            wd_h.append(spark_weekday(pd.Timestamp(pend_day)))
            tx_h, qty_h, pr_h = tx_h[-MAX_HISTORY:], qty_h[-MAX_HISTORY:], pr_h[-MAX_HISTORY:]
            cust_h, countr_h, wd_h = cust_h[-MAX_HISTORY:], countr_h[-MAX_HISTORY:], wd_h[-MAX_HISTORY:]

            pend_day = None

        if pend_day is None:
            pend_day, pend_tx, pend_qty, pend_price_sum = day_str, 0.0, 0.0, 0.0
            pend_customers, pend_countries, pend_y = set(), set(), 0

        # Accumulate this batch's events into the (still open) day
        pend_tx += float(len(day_df))
        pend_qty += float(day_df["Quantity"].sum())
        pend_price_sum += float(day_df["UnitPrice"].sum())
        pend_customers.update(day_df["CustomerID"].astype(str))
        pend_countries.update(day_df["Country"].astype(str))
        pend_y = max(pend_y, int(day_df["Y"].max()))

    state.update((tx_h, qty_h, pr_h, cust_h, countr_h, wd_h,
                  pend_day, pend_tx, pend_qty, pend_price_sum,
                  sorted(pend_customers), sorted(pend_countries), pend_y))

    yield pd.DataFrame(results, columns=[f.name for f in output_schema.fields])


# -----------------------------
# Pipeline
# -----------------------------
def build_feature_stream(parsed):
    return parsed.groupBy("ProductID").applyInPandasWithState(
        stateful_map_function,
        output_schema,
        state_schema,
        "append",
        GroupStateTimeout.NoTimeout,
    )


def publish_batch(batch_df, batch_id):
    """Send every scored product-day to the dashboard topic and print the alerts to the console."""
    batch_df = batch_df.persist()
    if batch_df.isEmpty():
        batch_df.unpersist()
        return

    batch_df.select(
        col("ProductID").alias("key"),
        to_json(struct("Day", "ProductID", "p_trend", "Y", "tx_cnt")).alias("value"),
    ).write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("topic", SCORES_TOPIC) \
        .save()

    alerts = batch_df.filter(col("p_trend") >= STAGE1_THRESHOLD).orderBy("Day", "ProductID")
    print(f"-------------------------------------------\nBatch: {batch_id} | scored: {batch_df.count()} | alerts: {alerts.count()}", flush=True)
    alerts.select("Day", "ProductID", "p_trend", "Y").show(50, truncate=False)
    batch_df.unpersist()


def main():
    spark = SparkSession.builder \
        .appName("RetailTrendStreaming_Stateful") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g")) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    stage1_model = RandomForestClassificationModel.load(MODEL_PATH)

    raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    parsed = raw.select(from_json(col("value").cast("string"), schema).alias("d")) \
        .select("d.*") \
        .withColumn("ts", to_timestamp("InvoiceDate")) \
        .withWatermark("ts", "1 day")

    feature_stream = build_feature_stream(parsed)

    vector_stream = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features",
        handleInvalid="skip",
    ).transform(feature_stream)

    scored = stage1_model.transform(vector_stream) \
        .withColumn("p_trend", _round(vector_to_array(col("probability"))[1], 3)) \
        .select("Day", "ProductID", "p_trend", "Y", "tx_cnt")

    query = scored.writeStream \
        .outputMode("append") \
        .foreachBatch(publish_batch) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .trigger(processingTime="10 seconds") \
        .start()

    print(f"Stateful streaming pipeline started. Monitoring '{KAFKA_TOPIC}' on {KAFKA_BOOTSTRAP} for trends", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
