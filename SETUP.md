# RediCafé — Setup Guide

Steps to get the coffee-shop demo and FAQ vector search running from scratch on your own
machine. Written for macOS/Linux; Windows users can substitute PowerShell equivalents where
noted.

## 1. Prerequisites

- **Python 3.10+** (check with `python3 --version`)
- **pip**
- A **Redis Cloud** account (free tier works) — sign up at https://redis.io/try-free/
- (Optional but recommended) **Redis Insight** for visually inspecting keys —
  https://redis.io/downloads/#insight

## 2. Create your Redis Cloud database

1. Log into Redis Cloud, create a new subscription (free/Essentials plan).
2. Create a database. **Free tier allows one database**, so this single database will host
   both the coffee-shop data and the FAQ vector index (they're kept separate by key prefix,
   not by separate databases).
3. Under the database's capabilities, make sure **Search and Query** is included — this is
   required for the FAQ vector lab (`FT.CREATE` / vector fields). On most current free-tier
   databases this is included by default ("Advanced Capabilities: All"), but double-check.
4. Once created, open the database and click **"View connection details"**. Note down:
   - **Host** (e.g. `something.db.redis.io`)
   - **Port**
   - **Password**
5. Check the database's **Security** settings: on the free tier, **TLS is not available** —
   confirm it's off. This matters for the connection config in step 5.

## 3. Get the code

```bash
git clone https://github.com/tyweis25/redicafe.git
cd redicache
```
(or download/unzip the project files if you're not using git)

## 4. Set up a Python virtual environment

```bash
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
```
Your terminal prompt should now show `(venv)`.

## 5. Install dependencies

```bash
pip install -r requirements.txt
```
This installs FastAPI, the Redis Python client, RedisVL, sentence-transformers, and their
dependencies. The sentence-transformers install pulls in PyTorch, so this step can take a
few minutes on first run.

## 6. Configure your credentials

```bash
cp .env.example .env
```
Open `.env` in a text editor and fill in the values from step 2:
```
REDIS_HOST=<your host>
REDIS_PORT=<your port>
REDIS_PASSWORD=<your password>
REDIS_TLS=false
```
Leave the other variables (`FAQ_DISTANCE_THRESHOLD`, `PICKUP_TTL_SECONDS`,
`ORDER_EVENT_METHOD`, `STREAM_REPLAY_COUNT`) at their defaults for now — they're
explained in the README if you want to tune them later.

**Never commit your real `.env` file** — it's already excluded via `.gitignore`.

## 7. Verify the connection

```bash
python3 -c "
from dotenv import load_dotenv
import os, redis
load_dotenv()
r = redis.Redis(host=os.environ['REDIS_HOST'], port=int(os.environ['REDIS_PORT']),
                 password=os.environ['REDIS_PASSWORD'], ssl=False, protocol=2)
print(r.ping())
"
```
This should print `True`. If you get an authentication error, double-check the host/port/
password were copied correctly and that there's no stray environment variable overriding
them (`echo $REDIS_PASSWORD` should be empty).

## 8. Build the FAQ vector index (one-time)

```bash
python setup_faq_index.py
```
This creates the `faq_idx` search index, downloads the embedding model
(`all-MiniLM-L6-v2`, ~80MB, first run only), embeds the FAQ dataset, and loads it into
Redis. You should see a confirmation with a self-verification query result at the end.

## 9. Start the backend

```bash
uvicorn app:app --reload
```
Wait for these two lines in the terminal before continuing:
```
FAQ embedding model loaded.
Order event delivery: Redis Pub/Sub (PUBLISH/SUBSCRIBE on 'order_updates')
```

## 10. Open the app

In your browser, open two tabs:
- **Customer screen:** http://localhost:8000/customer.html
- **Barista screen:** http://localhost:8000/barista.html

Place an order on the customer screen and watch it appear instantly on the barista screen
— that live update is the Redis Pub/Sub → WebSocket bridge working.

## 11. Try the FAQ search

On the customer screen, use the "Ask a Question" box — try "Is the shop open on Sundays?"
It should return a confident answer even though the wording doesn't match any FAQ entry's
exact text (semantic search, not keyword matching).

## 12. (Optional) Explore in Redis Insight

Connect Redis Insight to the same host/port/password from step 2 (TLS off) and browse the
key list — you should see `order:*`, `customer:*`, `queue:orders`, `leaderboard:drinks`,
and `faq:*` keys populated from your testing.
