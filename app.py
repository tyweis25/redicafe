"""
Coffee Shop Order Queue - FastAPI backend

Bridges:
  - Customer/barista HTTP requests <-> Redis (strings, hashes, lists, sorted sets)
  - Order events <-> WebSocket clients (barista screen), via one of two
    interchangeable delivery mechanisms selected by ORDER_EVENT_METHOD in .env:
      "pubsub"  (default) - Redis Pub/Sub (PUBLISH/SUBSCRIBE), fire-and-forget
      "streams"           - Redis Streams (XADD/XREAD), events persist in a log
    Both implementations stay in this file regardless of which is active -
    see emit_order_event(), pubsub_listener(), and stream_listener() below.

Run:
  uvicorn app:app --reload
"""

import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import date

import numpy as np
import redis.asyncio as redis
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from redis import Redis as SyncRedis
from redisvl.index import SearchIndex
from redisvl.query import VectorQuery
from redisvl.schema import IndexSchema
from sentence_transformers import SentenceTransformer

load_dotenv(override=True)

REDIS_HOST = os.environ["REDIS_HOST"]
REDIS_PORT = int(os.environ["REDIS_PORT"])
REDIS_PASSWORD = os.environ["REDIS_PASSWORD"]
# This database has TLS disabled in the Redis Cloud console (Security > TLS: Off).
# Set REDIS_TLS=true in .env if you later enable TLS on the database.
REDIS_TLS = os.environ.get("REDIS_TLS", "false").lower() == "true"

ORDER_CHANNEL = "order_updates"
ORDER_STREAM_KEY = "orders:stream"
# Which delivery mechanism carries order events to the barista screen's WebSocket.
# "pubsub" (default) = PUBLISH/SUBSCRIBE, fire-and-forget, simplest, no history.
# "streams" = XADD/XREAD, events persist in a log, survives listener restarts.
# See the two listener functions below for the actual implementations of both.
ORDER_EVENT_METHOD = os.environ.get("ORDER_EVENT_METHOD", "pubsub").strip().lower()
# How many past stream entries to replay to a barista screen when it connects.
# This is the whole point of using Streams over Pub/Sub here: a barista who
# logs in after orders were placed still receives them, because XADD entries
# persist in the log rather than being broadcast once and discarded.
STREAM_REPLAY_COUNT = int(os.environ.get("STREAM_REPLAY_COUNT", "50"))
# Ready-for-pickup window (seconds). Default 600 (10 min) for real use;
# drop to something short like 20-30 for a live demo of TTL expiry.
PICKUP_TTL_SECONDS = int(os.environ.get("PICKUP_TTL_SECONDS", "600"))

# --- Redis connection (async client, for the coffee-shop endpoints) ---
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    ssl=REDIS_TLS,
    protocol=2,  # avoid redis-py 6.x RESP3/HELLO handshake quirk with Redis Cloud auth
    decode_responses=True,
)

# --- FAQ vector search setup (reuses the faq_idx index built by setup_faq_index.py) ---
# RedisVL's SearchIndex uses the sync redis client, so this is separate from the
# async client above. Same connection details, same shared database.
FAQ_EMBEDDING_DIMS = 384
FAQ_DISTANCE_THRESHOLD = float(os.environ.get("FAQ_DISTANCE_THRESHOLD", "0.6"))

faq_schema = IndexSchema.from_dict({
    "index": {
        "name": "faq_idx",
        "prefix": "faq",
        "storage_type": "hash",
    },
    "fields": [
        {"name": "id", "type": "tag"},
        {"name": "question", "type": "text"},
        {"name": "answer", "type": "text"},
        {
            "name": "question_embedding",
            "type": "vector",
            "attrs": {
                "dims": FAQ_EMBEDDING_DIMS,
                "distance_metric": "cosine",
                "algorithm": "flat",
                "datatype": "float32",
            },
        },
    ],
})

faq_sync_redis = SyncRedis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    ssl=REDIS_TLS,
    protocol=2,
    decode_responses=False,  # vector bytes must stay raw
)
faq_index = SearchIndex(faq_schema, redis_client=faq_sync_redis)

# The embedding model is loaded once at startup (see lifespan below), not per-request -
# model loading is the expensive part, encoding individual questions after that is fast.
faq_model: SentenceTransformer | None = None

# --- Track connected barista WebSocket clients ---
active_connections: list[WebSocket] = []


async def pubsub_listener():
    """
    Background task (Pub/Sub method): subscribe to order_updates, forward to
    all connected WebSockets. Fire-and-forget - if a message is published
    while no one is listening (or this task is momentarily down), it's gone.
    """
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(ORDER_CHANNEL)
    async for message in pubsub.listen():
        if message["type"] != "message":
            continue
        dead = []
        for ws in active_connections:
            try:
                await ws.send_text(message["data"])
            except Exception:
                dead.append(ws)
        for ws in dead:
            active_connections.remove(ws)


async def stream_listener():
    """
    Background task (Streams method): XREAD new entries from orders:stream and
    forward them to all connected WebSockets. This handles LIVE events only -
    history replay for a newly-connected client is handled separately by
    replay_stream_history() below, which is what gives a late-joining barista
    the orders placed before they connected. Pub/Sub cannot do this: a
    PUBLISH with no listener attached is discarded, while an XADD entry
    persists in the log and can be read later by anyone.
    """
    last_id = "$"  # live tail; prior entries are served by replay_stream_history()
    while True:
        try:
            response = await redis_client.xread({ORDER_STREAM_KEY: last_id}, block=0, count=10)
        except Exception:
            await asyncio.sleep(1)
            continue

        for _stream_name, entries in response:
            for entry_id, fields in entries:
                last_id = entry_id
                payload = json.dumps(fields)
                dead = []
                for ws in active_connections:
                    try:
                        await ws.send_text(payload)
                    except Exception:
                        dead.append(ws)
                for ws in dead:
                    active_connections.remove(ws)


async def replay_stream_history(websocket: WebSocket):
    """
    Send a newly-connected client the recent history from orders:stream, so a
    barista who logs in mid-shift sees the orders that were placed before they
    connected. Sent as a single batched message rather than one per entry, to
    avoid triggering a refetch storm on the client.

    Only meaningful in streams mode - there is no equivalent for Pub/Sub,
    because past PUBLISH messages simply don't exist to be replayed.
    """
    try:
        entries = await redis_client.xrevrange(ORDER_STREAM_KEY, count=STREAM_REPLAY_COUNT)
    except Exception:
        return

    if not entries:
        return

    # xrevrange returns newest-first; flip to chronological order.
    history = [fields for _entry_id, fields in reversed(entries)]
    await websocket.send_text(json.dumps({
        "event": "history",
        "count": len(history),
        "entries": history,
    }))


async def emit_order_event(event: dict):
    """
    Single entry point for publishing an order event, used by both /order and
    /order/{id}/complete. Routes to whichever mechanism ORDER_EVENT_METHOD
    selects - both implementations stay in the codebase regardless of which
    is active, so switching is just an .env change + restart.
    """
    if ORDER_EVENT_METHOD == "streams":
        await redis_client.xadd(ORDER_STREAM_KEY, {k: str(v) for k, v in event.items()})
    else:
        await redis_client.publish(ORDER_CHANNEL, json.dumps(event))


@asynccontextmanager
async def lifespan(app: FastAPI):
    global faq_model
    print("Loading FAQ embedding model (all-MiniLM-L6-v2)...")
    faq_model = await asyncio.to_thread(SentenceTransformer, "all-MiniLM-L6-v2")
    print("FAQ embedding model loaded.")

    if ORDER_EVENT_METHOD == "streams":
        print(f"Order event delivery: Redis Streams (XADD/XREAD on '{ORDER_STREAM_KEY}')")
        task = asyncio.create_task(stream_listener())
    else:
        print(f"Order event delivery: Redis Pub/Sub (PUBLISH/SUBSCRIBE on '{ORDER_CHANNEL}')")
        task = asyncio.create_task(pubsub_listener())

    yield
    task.cancel()


app = FastAPI(lifespan=lifespan)


# --- Request models ---
class OrderRequest(BaseModel):
    customer: str
    drink: str
    size: str
    milk: str = "none"


class FaqRequest(BaseModel):
    question: str
    threshold: float | None = None  # optional per-request override of FAQ_DISTANCE_THRESHOLD


# --- Endpoints ---

@app.get("/health", response_class=PlainTextResponse)
async def health():
    pong = await redis_client.ping()
    return "pong" if pong else "no response"


@app.post("/order")
async def create_order(order: OrderRequest):
    # Single persistent counter (not date-scoped) so order IDs are unique
    # forever. A date-scoped counter (e.g. orders:counter:{today}) resets to 1
    # each day, but order:{id} - the actual order record key - is NOT
    # date-scoped, so a reset counter would silently overwrite a previous
    # day's order:1 with today's order:1. Simpler to just never reset.
    order_id = await redis_client.incr("orders:counter")
    order_key = f"order:{order_id}"

    await redis_client.hset(
        order_key,
        mapping={
            "customer": order.customer,
            "drink": order.drink,
            "size": order.size,
            "milk": order.milk,
            "status": "queued",
            "created_at": date.today().isoformat(),
        },
    )
    await redis_client.rpush("queue:orders", order_id)
    await redis_client.zincrby("leaderboard:drinks", 1, order.drink)

    # Customer profile hash - a separate pattern from the order record hash
    # above: a small "account" object keyed by customer, tracking their
    # favorite (most recent) drink and running order count. Demo simplification:
    # slugified name as the key rather than a real customer/account ID.
    customer_key = f"customer:{order.customer.strip().lower().replace(' ', '_')}"
    await redis_client.hset(
        customer_key,
        mapping={"name": order.customer, "favorite_drink": order.drink},
    )
    total_orders = await redis_client.hincrby(customer_key, "total_orders", 1)

    await emit_order_event({"event": "new_order", "id": order_id})

    return {"order_id": order_id, "status": "queued", "customer_total_orders": total_orders}


@app.get("/customer/{name}")
async def get_customer(name: str):
    customer_key = f"customer:{name.strip().lower().replace(' ', '_')}"
    profile = await redis_client.hgetall(customer_key)
    if not profile:
        return {"found": False}
    return {"found": True, **profile}


@app.get("/queue")
async def get_queue():
    order_ids = await redis_client.lrange("queue:orders", 0, -1)
    orders = []
    for oid in order_ids:
        data = await redis_client.hgetall(f"order:{oid}")
        if data:
            data["id"] = oid
            orders.append(data)
    return orders


@app.post("/order/{order_id}/complete")
async def complete_order(order_id: str):
    await redis_client.hset(f"order:{order_id}", "status", "done")
    await redis_client.lrem("queue:orders", 1, order_id)

    # Transient "ready for pickup" window - a string with a TTL. Once this
    # key expires, the order has passed its pickup window. Separate from the
    # permanent status field in the order hash.
    await redis_client.set(f"order:{order_id}:pickup_status", "ready_for_pickup", ex=PICKUP_TTL_SECONDS)

    await emit_order_event({"event": "order_completed", "id": order_id})

    return {"order_id": order_id, "status": "done"}


@app.get("/leaderboard")
async def get_leaderboard():
    top = await redis_client.zrevrange("leaderboard:drinks", 0, 4, withscores=True)
    return [{"drink": drink, "count": int(count)} for drink, count in top]


@app.post("/faq/ask")
async def faq_ask(req: FaqRequest):
    """
    Embeds the question, runs a KNN vector search against faq_idx (built by
    setup_faq_index.py), and applies the same distance-threshold confidence
    check as query_faq.py. Runs on a background thread since sentence-transformers
    and the RedisVL sync client are both blocking calls.
    """
    def _search():
        vec = faq_model.encode(req.question).astype(np.float32).tobytes()
        vq = VectorQuery(
            vector=vec,
            vector_field_name="question_embedding",
            return_fields=["question", "answer"],
            num_results=1,
        )
        return faq_index.query(vq)

    results = await asyncio.to_thread(_search)

    # Per-request override lets the UI test different thresholds live, with no
    # server restart - falls back to the .env default (FAQ_DISTANCE_THRESHOLD)
    # when the request doesn't specify one.
    active_threshold = req.threshold if req.threshold is not None else FAQ_DISTANCE_THRESHOLD

    if not results:
        return {
            "confident": False, "matched_question": None, "answer": None,
            "distance": None, "threshold_used": active_threshold,
        }

    top = results[0]
    distance = float(top.get("vector_distance", 1.0))
    confident = distance < active_threshold

    return {
        "confident": confident,
        "matched_question": top["question"],
        "answer": top["answer"] if confident else None,
        "distance": distance,
        "threshold_used": active_threshold,
    }


@app.get("/order/{order_id}/pickup_status")
async def get_pickup_status(order_id: str):
    status = await redis_client.get(f"order:{order_id}:pickup_status")
    if status is None:
        return {"status": "expired_or_not_found", "seconds_remaining": 0}
    ttl = await redis_client.ttl(f"order:{order_id}:pickup_status")
    return {"status": status, "seconds_remaining": ttl}


@app.post("/order/{order_id}/pickup_confirm")
async def confirm_pickup(order_id: str):
    """
    Manually confirms an order was picked up, rather than waiting for its
    pickup_status TTL to expire naturally. Deletes the TTL'd key immediately
    (DEL) and updates the permanent order record's status for the full
    lifecycle: queued -> done -> picked_up.
    """
    deleted = await redis_client.delete(f"order:{order_id}:pickup_status")
    await redis_client.hset(f"order:{order_id}", "status", "picked_up")

    await emit_order_event({"event": "pickup_confirmed", "id": order_id})

    return {"order_id": order_id, "status": "picked_up", "deleted_pickup_key": bool(deleted)}


@app.get("/pickup")
async def get_pickup_orders():
    """
    All orders currently in their 'ready for pickup' TTL window, for the
    barista screen's pickup panel. Uses SCAN (non-blocking, safe on a live
    server) rather than KEYS (blocks Redis while it scans the whole keyspace).
    """
    orders = []
    async for key in redis_client.scan_iter(match="order:*:pickup_status"):
        order_id = key.split(":")[1]
        status = await redis_client.get(key)
        ttl = await redis_client.ttl(key)
        if status is None or ttl is None or ttl < 0:
            continue
        details = await redis_client.hgetall(f"order:{order_id}")
        orders.append({
            "id": order_id,
            "status": status,
            "seconds_remaining": ttl,
            "customer": details.get("customer", ""),
            "drink": details.get("drink", ""),
        })
    orders.sort(key=lambda o: o["seconds_remaining"])
    return orders


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.append(websocket)

    # Streams mode only: replay recent history to this specific client so a
    # late-connecting barista screen isn't missing orders placed before it
    # connected. Pub/Sub has no equivalent - see replay_stream_history().
    if ORDER_EVENT_METHOD == "streams":
        await replay_stream_history(websocket)

    try:
        while True:
            # We don't expect messages from the client; just keep the connection alive.
            await websocket.receive_text()
    except WebSocketDisconnect:
        if websocket in active_connections:
            active_connections.remove(websocket)


# Serve the static frontend files (customer.html, barista.html) from ./static
# Guarded so the backend still runs standalone (e.g. testing /health, /docs)
# before the static/ folder and frontend files exist.
if os.path.isdir("static"):
    app.mount("/", StaticFiles(directory="static", html=True), name="static")

