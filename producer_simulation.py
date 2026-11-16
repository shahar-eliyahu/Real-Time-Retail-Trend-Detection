import os
import json
import time
import random
import threading
import pandas as pd
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "retail_stream")
CONTROL_TOPIC = os.getenv("CONTROL_TOPIC", "stream_control")   # commands from the dashboard
STATUS_TOPIC = os.getenv("STATUS_TOPIC", "stream_status")      # producer state, shown in the dashboard
DATA_PATH = os.getenv("DATA_PATH") or (
    "cleaned_retail_data_with_Y.csv" if os.path.exists("cleaned_retail_data_with_Y.csv")
    else "cleaned_retail_data_with_Y.csv.gz"   # compressed copy kept in the repository
)
AUTO_START = os.getenv("AUTO_START", "true").lower() == "true"  # docker compose sets false: wait for Start
SPEEDS = (1, 5, 20)

# Load and preprocess data
df = pd.read_csv(DATA_PATH)
df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
df = df.sort_values("InvoiceDate", kind="stable")
df["InvoiceDate"] = df["InvoiceDate"].astype(str)
df["ProductID"] = df["ProductID"].astype(str).str.strip()
df = df.fillna(0)
records = df.to_dict(orient="records")


def retry(make, retries=60, delay=2):
    for attempt in range(1, retries + 1):
        try:
            return make()
        except NoBrokersAvailable:
            print(f"Kafka not ready at {KAFKA_BOOTSTRAP} (attempt {attempt}/{retries}), retrying...", flush=True)
            time.sleep(delay)
    raise SystemExit(f"Could not connect to Kafka at {KAFKA_BOOTSTRAP}")


producer = retry(lambda: KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
))

# Shared simulation state (updated by the control thread, read by the streaming loop)
lock = threading.Lock()
sim = {"state": "running" if AUTO_START else "waiting", "speed": 1, "sent": 0}


def listen_for_commands():
    consumer = retry(lambda: KafkaConsumer(
        CONTROL_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    ))
    for msg in consumer:
        cmd = msg.value
        with lock:
            action = cmd.get("action")
            if action in ("start", "resume") and sim["state"] in ("waiting", "paused"):
                sim["state"] = "running"
            elif action == "pause" and sim["state"] == "running":
                sim["state"] = "paused"
            elif action == "speed" and cmd.get("speed") in SPEEDS:
                sim["speed"] = cmd["speed"]
            print(f"Command: {cmd} -> state={sim['state']} speed={sim['speed']}x", flush=True)


def publish_status():
    while True:
        with lock:
            status = dict(sim, total=len(records),
                          sim_date=records[sim["sent"] - 1]["InvoiceDate"][:10] if sim["sent"] else None)
        producer.send(STATUS_TOPIC, value=status)
        time.sleep(1)


threading.Thread(target=listen_for_commands, daemon=True).start()
threading.Thread(target=publish_status, daemon=True).start()

print(f"Ready to stream {len(records)} rows to '{KAFKA_TOPIC}'"
      + ("" if AUTO_START else " - waiting for Start from the dashboard"), flush=True)

try:
    while True:
        with lock:
            state, speed, sent = sim["state"], sim["speed"], sim["sent"]
        if state != "running":
            time.sleep(0.2)
            continue

        # Simulate bursty arrival
        burst = records[sent:sent + random.randint(50, 100)]
        for row in burst:
            producer.send(KAFKA_TOPIC, value=row)
        sent += len(burst)

        with lock:
            sim["sent"] = sent
            if sent >= len(records):
                sim["state"] = "finished"
        if sent >= len(records):
            break

        if sent % 20000 < len(burst):
            print(f"Sent {sent}/{len(records)} rows (up to {burst[-1]['InvoiceDate'][:10]}) at {speed}x", flush=True)

        # Random delay between bursts, shortened by the speed multiplier
        time.sleep(random.uniform(0.1, 0.4) / speed)

except KeyboardInterrupt:
    producer.flush()
    raise SystemExit("Streaming interrupted.")

producer.flush()
print("Done. All rows streamed.", flush=True)

if not AUTO_START:
    # running under the dashboard: stay up so it keeps seeing the "finished" status
    while True:
        time.sleep(60)
