# RediCafé

A working coffee-shop backend + a semantic FAQ search tool, built as a demo for a Redis
Solutions Engineer interview. Covers Redis strings, hashes, lists, sorted sets, Pub/Sub,
Streams, TTLs, and vector search (via RedisVL + sentence transformers), all running against
a single Redis Cloud (free tier) database.

## What's here

- **Coffee shop order queue** — a customer-facing order screen and a live barista display,
  backed entirely by Redis. Places orders, tracks a FIFO queue, a drink-popularity
  leaderboard, per-customer profiles, and a TTL-based "ready for pickup" window.
- **FAQ vector search** — a small coffee-shop FAQ dataset indexed with RedisVL, queried by
  semantic similarity (sentence-transformers embeddings + RediSearch KNN), including a
  confidence-threshold check for out-of-scope questions. Exposed both as standalone scripts
  and as a live chat box on the customer screen.

## Architecture

```
customer.html ──POST /order──────┐
(order form +      │             │
 FAQ chat box)      │             ▼
                     │       FastAPI (app.py) ──────► Redis Cloud
                POST /faq/ask         │                (shared database,
                     │                │ PUBLISH          key-prefix
                     ▼                │ order_updates    namespaced:
              [vector search,         ▼                  order:*, customer:*,
               same index as    barista.html             queue:*, leaderboard:*,
               setup_faq_index]  ◄── WebSocket /ws        faq:*)
                                  (live queue updates)
                                  ◄── polling /leaderboard, /pickup
```

Redis holds all live operational state (queue, leaderboard, customer profiles, pickup
windows, the FAQ vector index) — it's the only datastore in this demo, standing in for
what would be the "fast operational layer" in a real system, with a relational database as
the system of record alongside it in a production version (see the lab writeups for the
full architecture discussion).

## Redis data types in use

| Type | Key(s) | Purpose |
|---|---|---|
| String | `orders:counter:{date}` | Atomic daily order-ID counter (`INCR`) |
| String + TTL | `order:{id}:pickup_status` | "Ready for pickup" window, auto-expires (`SET ... EX`, `TTL`) |
| Hash | `order:{id}` | Order record (customer, drink, size, milk, status, created_at) |
| Hash | `customer:{name}` | Customer profile (name, favorite_drink, total_orders) |
| List | `queue:orders` | FIFO barista queue (`RPUSH` / `LPOP` / `LREM` / `BLPOP`) |
| Sorted Set | `leaderboard:drinks` | Drink popularity ranking (`ZINCRBY` / `ZREVRANGE`) |
| Pub/Sub | `order_updates` channel | New-order / order-completed events, bridged to a WebSocket for the barista screen |
| Stream | `orders:stream` | The same events as a persistent log (`XADD`/`XREAD`), so late-joining barista screens can be replayed the orders they missed |
| Vector index | `faq_idx` (prefix `faq:`) | FAQ semantic search (RedisVL, cosine, FLAT, 384-dim `all-MiniLM-L6-v2` embeddings) |

## Setup

1. Create a Redis Cloud database (free tier works) with **Search and Query** capability
   enabled.
2. Copy `.env.example` to `.env` and fill in your database's host, port, and password.
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Build the FAQ vector index (one-time):
   ```bash
   python setup_faq_index.py
   ```
5. Start the backend:
   ```bash
   uvicorn app:app --reload
   ```
6. Open `http://localhost:8000/customer.html` and `http://localhost:8000/barista.html`
   (two separate browser tabs).

## Scripts

- `setup_faq_index.py` — builds the `faq_idx` RedisVL index and loads the FAQ dataset with
  embeddings. Re-runnable (drops and recreates the index each time).
- `query_faq.py` — standalone demo of the FAQ search: paraphrase queries, a keyword-vs-vector
  contrast, and an out-of-scope query showing the confidence-threshold check.

## Configuration (`.env`)

| Variable | Purpose |
|---|---|
| `REDIS_HOST` / `REDIS_PORT` / `REDIS_PASSWORD` | Redis Cloud connection details |
| `REDIS_TLS` | Whether to use TLS (free-tier Redis Cloud does not support it) |
| `FAQ_DISTANCE_THRESHOLD` | Cosine-distance cutoff for a "confident" FAQ match (lower = more similar) |
| `PICKUP_TTL_SECONDS` | How long a completed order stays in its pickup window before expiring |
| `ORDER_EVENT_METHOD` | How order events reach the barista screen: `pubsub` (fire-and-forget) or `streams` (persistent log, replayed to late-joining clients) |
| `STREAM_REPLAY_COUNT` | Streams mode only: how many past events to replay to a barista screen on connect |

## Notes

- Free-tier Redis Cloud allows one database, so the coffee-shop and FAQ workloads share it,
  separated by key prefix rather than by separate databases (see the lab writeups for the
  production tradeoff this implies).
- TLS is off because the free plan doesn't support it — noted explicitly rather than silently
  worked around.
