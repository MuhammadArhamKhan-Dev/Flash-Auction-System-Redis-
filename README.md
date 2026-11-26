# Flash Auction: real-time bidding engine (CEP prototype)

React + FastAPI (2 instances) + PostgreSQL + Redis, with Nginx as load balancer.

```
React  <== WebSocket ==>  Nginx :8080  -->  FastAPI :8001 / :8002
                                              | 1. pre-filter    --> Redis
                                              | 2. row lock      --> PostgreSQL (source of truth)
                                              | 3. publish state --> Redis Pub/Sub --> all instances --> all clients
```

## Run on Windows (no Docker, no virtualization)

You need: Python 3.10+, Node.js 18+, PostgreSQL, and Redis for Windows
([Memurai](https://redis.io/learn/howtos/how-to-run-redis-on-windows-natively-with-memurai)
or the [redis-server.exe zip](https://github.com/tporadowski/redis/releases), version 5 or later).
Keep Redis running on port 6379.

**1. Create the database** (once; pgAdmin also works). Tables are created automatically.

```
psql -U postgres -c "CREATE DATABASE flash_auction;"
```

**2. Install the backend** (PowerShell, in the `flash-auction` folder)

```
cd backend
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
```

**3. Start backend 1** (same terminal). Replace `YOURPASSWORD`; write `@` in a password as `%40`.

```
$env:DATABASE_URL="postgresql://postgres:YOURPASSWORD@127.0.0.1:5432/flash_auction"
$env:INSTANCE="backend-1"
.venv\Scripts\python -m uvicorn app.main:app --port 8001
```

**4. Start backend 2** (new terminal, inside `backend`)

```
$env:DATABASE_URL="postgresql://postgres:YOURPASSWORD@127.0.0.1:5432/flash_auction"
$env:INSTANCE="backend-2"
.venv\Scripts\python -m uvicorn app.main:app --port 8002
```

**5. Start the frontend** (new terminal)

```
cd frontend
npm install
npm run dev
```

Open two tabs to show cross-instance sync:
`http://localhost:5173/?port=8001` and `http://localhost:5173/?port=8002`

**6. Run the tests** (from the `flash-auction` folder; this resets the auction)

```
backend\.venv\Scripts\python loadtest\stress_test.py --users 200 --servers 127.0.0.1:8001,127.0.0.1:8002
```

Then increase `--users` (500, 1000, ...) and record the numbers for your report.

**7. Nginx (optional)**: download the Windows zip from nginx.org, extract it, replace its
`conf\nginx.conf` with `nginx\nginx.conf`, and run `start nginx` inside the extracted folder
(`.\nginx -s stop` to stop). Then open `http://localhost:5173/?port=8080`, or test with
`--servers 127.0.0.1:8080`. Nginx on Windows is limited to about 1024 connections
(each WebSocket uses two), so stay near 200-400 users through it.

## Run with Docker (friend's machine)

```
docker compose up --build
```

Open `http://localhost:8080`. Test with `--servers 127.0.0.1:8080`.

## How each CEP problem is solved

| Problem | Solution | Where | Evidence |
|---|---|---|---|
| Race conditions | `SELECT ... FOR UPDATE` serialises bids on an auction; `UNIQUE (auction_id, seq)` | `bidding.py` | Test 0 (old way) vs Test 1 |
| Duplicate bids / retries | Client-generated `bid_id`; checked in Redis, enforced by a primary key | `bidding.py` | Test 2, "Resend" button |
| Stale reads | Server pushes every change; `(round, version)` stops older updates replacing newer ones | `realtime.py`, `useAuction.js` | Test 3 "no stale screens" |
| Unfair timing | Deadline checked with the database clock after the lock; client clocks ignored | `bidding.py` | Test 3 late bid |
| Crashes in spikes | Async I/O, fixed connection pools, Redis rejects low bids early, a queue per client, 2 instances | `db.py`, `main.py`, `realtime.py` | Test 3 latency and errors |

## Bid flow

1. Client sends `{"bid_id": "<uuid>", "user_id": "ali", "amount": 150}`.
2. Redis: known `bid_id` returns the original answer; amount not above the cached price is rejected.
3. PostgreSQL transaction: lock row, re-check `bid_id`, deadline and price, update, insert, commit.
4. After commit, one Redis Lua script updates the cache and publishes the new state.
5. Every instance pushes it to its own clients; the bidder also gets a personal `bid_result`.

The cache is written only after a commit, so it can lag behind the database but not run ahead of it.
A lagging cache only means a bid goes to PostgreSQL, which makes the final decision.

## API

| Endpoint | Purpose |
|---|---|
| `WS /ws/auction/1` | live state and bidding |
| `GET /api/auction/1` | current state |
| `POST /api/auction/1/reset?seconds=60` | new round (demo) |
| `GET /api/auction/1/audit` | consistency check (demo) |
| `GET /api/health` | instance name and connection count |
| `POST /api/naive/bid` | intentionally broken "old REST" bid, for comparison |

## Limitations (state these in the report)

- 50,000 concurrent bidders is the architectural target. Report only the load you actually measured.
- All bids for one auction pass through one row lock. This guarantees correctness and caps throughput per auction.
- One item per auction; no login (the name is free text); reset, audit and naive endpoints are unprotected.
- PostgreSQL and Redis are single instances.
- A bid that arrives before the deadline but gets the lock after it is rejected.
- Accepted bids are stored; rejected bids are not. Starting a new round deletes the previous round's bids.
