import json
import time
import random
import pandas as pd
from kafka import KafkaProducer

# Load and preprocess data
df = pd.read_csv("cleaned_retail_data_with_Y.csv")
df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
df = df.sort_values("InvoiceDate")
df["InvoiceDate"] = df["InvoiceDate"].astype(str)
df["ProductID"] = df["ProductID"].astype(str).str.strip()
df = df.fillna(0)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "retail_stream"
print(f"Streaming {len(df)} rows to '{topic}' with bursty simulation...")

row_iter = df.iterrows()
stop_streaming = False

try:
    while not stop_streaming:
        # Simulate bursty arrival
        burst_size = random.randint(50,100)
        
        for _ in range(burst_size):
            try:
                _, row = next(row_iter)
                producer.send(topic, value=row.to_dict())
            except StopIteration:
                stop_streaming = True
                break
        
        # Random delay between bursts
        time.sleep(random.uniform(0.1, 0.4))

except KeyboardInterrupt:
    print("Streaming interrupted.")

producer.flush()
print("Done.")

