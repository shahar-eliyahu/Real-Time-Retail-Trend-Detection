import os
import json
import time
import asyncio
import threading
from collections import deque
from contextlib import asynccontextmanager
from datetime import date
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Body
from fastapi.responses import FileResponse, JSONResponse
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TX_TOPIC = os.getenv("KAFKA_TOPIC", "retail_stream")
SCORES_TOPIC = os.getenv("SCORES_TOPIC", "trend_scores")
CONTROL_TOPIC = os.getenv("CONTROL_TOPIC", "stream_control")
STATUS_TOPIC = os.getenv("STATUS_TOPIC", "stream_status")
NAMES_PATH = os.getenv("NAMES_PATH", "product_trend_labels.csv")

THRESHOLD = 0.25   # same alert threshold as the streaming model
ALPHA = 0.6        # weight of today's p_trend in the trend score
DECAY = 0.85       # score multiplier per simulated day without a new score
TOP_N = 15
NEW_DAYS = 2       # a product keeps the "new" badge for this many simulated days
HISTORY = 120      # scored days kept per product for the detail chart
FEED_SIZE = 40

STATIC = Path(__file__).parent / "static"


def load_names(path):
    try:
        df = pd.read_csv(path, dtype={"ProductID": str})
        return dict(zip(df["ProductID"].str.strip(), df["Description"].fillna("").str.strip()))
    except FileNotFoundError:
        return {}


def days_between(a, b):
    return (date.fromisoformat(b) - date.fromisoformat(a)).days


class TrendBoard:
    """Live state behind the dashboard: trend scores, streaks, rankings, the feed and throughput."""

    def __init__(self, names):
        self.names = names
        self.lock = threading.Lock()
        self.products = {}
        self.feed = deque(maxlen=FEED_SIZE)
        self.sim_date = None        # latest transaction date seen on the raw stream
        self.score_date = None      # latest scored day (reference for score decay)
        self.total_tx = 0
        self.total_scored = 0
        self._tick_count = 0
        self.rate_history = deque([0] * 60, maxlen=60)
        self.tp = self.fp = self.fn = self.tn = 0
        self.first_top_day = {}
        self.last_ranks = {}
        self.prev_ranks = {}
        self.rank_day = None
        self.producer_status = None
        self.producer_seen = 0.0

    def on_status(self, status):
        with self.lock:
            self.producer_status = status
            self.producer_seen = time.time()

    # ---------- ingestion ----------
    def on_transaction(self, invoice_date):
        with self.lock:
            self.total_tx += 1
            self._tick_count += 1
            d = str(invoice_date)[:10]
            if self.sim_date is None or d > self.sim_date:
                self.sim_date = d

    def on_score(self, rec):
        pid, day = str(rec["ProductID"]), str(rec["Day"])
        p, y, tx = float(rec["p_trend"]), int(rec["Y"]), float(rec.get("tx_cnt", 0))
        with self.lock:
            pr = self.products.get(pid)
            if pr is None:
                pr = self.products[pid] = {"score": p, "streak": 0, "last_day": day, "hist": deque(maxlen=HISTORY)}
            else:
                gap = max(0, days_between(pr["last_day"], day) - 1)
                pr["score"] = ALPHA * p + (1 - ALPHA) * pr["score"] * (DECAY ** gap)
                pr["last_day"] = day
            pr["streak"] = pr["streak"] + 1 if p >= THRESHOLD else 0
            pr["hist"].append((day, p, tx, y))

            alert = p >= THRESHOLD
            self.tp += alert and y == 1
            self.fp += alert and y == 0
            self.fn += (not alert) and y == 1
            self.tn += (not alert) and y == 0
            self.total_scored += 1
            if self.score_date is None or day > self.score_date:
                self.score_date = day

            self.feed.appendleft({"pid": pid, "name": self.names.get(pid, ""), "day": day, "p": round(p, 3),
                                  "y": y, "alert": alert, "correct": alert == (y == 1)})

    # ---------- views ----------
    def _effective(self, pr):
        idle = max(0, days_between(pr["last_day"], self.score_date) - 1)
        return pr["score"] * (DECAY ** idle)

    def tick(self):
        with self.lock:
            self.rate_history.append(self._tick_count)
            self._tick_count = 0

    def snapshot(self):
        with self.lock:
            ranked = []
            if self.score_date:
                ranked = sorted(((self._effective(pr), pid) for pid, pr in self.products.items()), reverse=True)[:TOP_N]
            ranks = {pid: i + 1 for i, (_, pid) in enumerate(ranked)}

            # rank changes are measured against the end of the previous simulated day
            if self.rank_day != self.score_date:
                self.prev_ranks = self.last_ranks
                self.rank_day = self.score_date
            self.last_ranks = ranks

            board = []
            for score, pid in ranked:
                self.first_top_day.setdefault(pid, self.score_date)
                pr = self.products[pid]
                prev = self.prev_ranks.get(pid)
                board.append({
                    "pid": pid,
                    "name": self.names.get(pid, ""),
                    "score": round(score, 3),
                    "streak": pr["streak"],
                    "change": None if prev is None else prev - ranks[pid],
                    "new": days_between(self.first_top_day[pid], self.score_date) <= NEW_DAYS,
                    "spark": [round(h[1], 3) for h in list(pr["hist"])[-20:]],
                    "last_day": pr["last_day"],
                })

            alerts = self.tp + self.fp
            positives = self.tp + self.fn
            online = self.producer_status is not None and time.time() - self.producer_seen < 5
            return {
                "producer": self.producer_status if online else None,
                "sim_date": self.sim_date,
                "score_date": self.score_date,
                "rate": self.rate_history[-1],
                "rate_history": list(self.rate_history),
                "total_tx": self.total_tx,
                "total_scored": self.total_scored,
                "tracked": len(self.products),
                "alerts": alerts,
                "precision": round(self.tp / alerts, 3) if alerts else None,
                "recall": round(self.tp / positives, 3) if positives else None,
                "board": board,
                "feed": list(self.feed),
            }

    def product(self, pid):
        with self.lock:
            pr = self.products.get(pid)
            if pr is None:
                return None
            return {
                "pid": pid,
                "name": self.names.get(pid, ""),
                "score": round(self._effective(pr), 3),
                "streak": pr["streak"],
                "rank": self.last_ranks.get(pid),
                "history": [{"day": d, "p": round(p, 3), "tx": tx, "y": y} for d, p, tx, y in pr["hist"]],
            }


board = TrendBoard(load_names(NAMES_PATH))


# ---------- Kafka readers (background threads) ----------
def kafka_consumer(topic, offset):
    while True:
        try:
            return KafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                auto_offset_reset=offset,
                enable_auto_commit=False,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
        except NoBrokersAvailable:
            time.sleep(2)


def read_transactions():
    # live throughput only, so start from the newest messages
    for msg in kafka_consumer(TX_TOPIC, "latest"):
        board.on_transaction(msg.value.get("InvoiceDate", ""))


def read_scores():
    # replay all scores so a restarted dashboard rebuilds the full ranking
    for msg in kafka_consumer(SCORES_TOPIC, "earliest"):
        board.on_score(msg.value)


def read_status():
    for msg in kafka_consumer(STATUS_TOPIC, "latest"):
        board.on_status(msg.value)


command_producer = None
command_lock = threading.Lock()


def send_command(cmd):
    global command_producer
    with command_lock:
        if command_producer is None:
            command_producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        command_producer.send(CONTROL_TOPIC, value=cmd)
        command_producer.flush(timeout=5)


# ---------- web app ----------
clients = set()


async def broadcast_loop():
    while True:
        await asyncio.sleep(1)
        board.tick()
        snap = board.snapshot()   # computed every second so rank changes and "new" badges stay accurate
        for ws in list(clients):
            try:
                await ws.send_json(snap)
            except Exception:
                clients.discard(ws)


@asynccontextmanager
async def lifespan(app):
    threading.Thread(target=read_transactions, daemon=True).start()
    threading.Thread(target=read_scores, daemon=True).start()
    threading.Thread(target=read_status, daemon=True).start()
    task = asyncio.create_task(broadcast_loop())
    yield
    task.cancel()


app = FastAPI(title="Retail trend dashboard", lifespan=lifespan)


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")


@app.get("/api/state")
def state():
    return board.snapshot()


@app.get("/api/product/{pid}")
def product(pid: str):
    data = board.product(pid)
    return data if data else JSONResponse({"error": "unknown product"}, status_code=404)


@app.post("/api/control")
def control(cmd: dict = Body(...)):
    action = cmd.get("action")
    if action not in ("start", "pause", "resume", "speed"):
        return JSONResponse({"error": "unknown action"}, status_code=400)
    if action == "speed" and cmd.get("speed") not in (1, 5, 20):
        return JSONResponse({"error": "speed must be 1, 5 or 20"}, status_code=400)
    try:
        send_command({"action": action, "speed": cmd.get("speed")})
    except Exception:
        return JSONResponse({"error": "Kafka is not reachable"}, status_code=503)
    return {"ok": True}


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    try:
        await ws.send_json(board.snapshot())
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        clients.discard(ws)
