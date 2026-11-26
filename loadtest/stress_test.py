"""
Concurrency tests against the running system.

    python loadtest/stress_test.py --users 500
    python loadtest/stress_test.py --users 500 --servers 127.0.0.1:8001,127.0.0.1:8002

--users    number of simultaneous WebSocket bidders
--servers  where to connect; bidders are spread evenly across them
"""
import argparse
import asyncio
import json
import random
import sys
import time
import urllib.request
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import websockets

failures = []


def check(name, passed, detail=""):
    print(f"  [{'PASS' if passed else 'FAIL'}] {name} {detail}")
    if not passed:
        failures.append(name)


def http(server, path, method="GET", body=None):
    request = urllib.request.Request(
        f"http://{server}{path}", method=method,
        data=json.dumps(body).encode() if body else None,
        headers={"Content-Type": "application/json"},
    )
    # ignore any system proxy: we are talking to a local server
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(request, timeout=60) as response:
        return json.load(response)


class Bidder:
    def __init__(self, name):
        self.name = name
        self.state = None
        self.ready = asyncio.Event()
        self.waiting = {}

    async def connect(self, server, gate):
        async with gate:  # open sockets in batches
            self.ws = await websockets.connect(
                f"ws://{server}/ws/auction/1", open_timeout=60, ping_interval=None
            )
        self.reader = asyncio.create_task(self.read())
        await asyncio.wait_for(self.ready.wait(), 60)

    async def read(self):
        try:
            async for raw in self.ws:
                msg = json.loads(raw)
                if msg["type"] == "state":
                    new = (msg["round"], msg["version"])
                    if self.state is None or new >= (self.state["round"], self.state["version"]):
                        self.state = msg
                    self.ready.set()
                elif msg["type"] == "bid_result":
                    future = self.waiting.pop(msg["bid_id"], None)
                    if future and not future.done():
                        future.set_result(msg)
        except websockets.ConnectionClosed:
            pass

    async def bid(self, amount, bid_id=None, user=None):
        bid_id = bid_id or str(uuid.uuid4())
        future = asyncio.get_running_loop().create_future()
        self.waiting[bid_id] = future
        started = time.perf_counter()
        await self.ws.send(json.dumps({"bid_id": bid_id, "user_id": user or self.name, "amount": amount}))
        result = await asyncio.wait_for(future, 60)
        result["latency_ms"] = (time.perf_counter() - started) * 1000
        return result


async def reset(server, seconds):
    return await asyncio.to_thread(http, server, f"/api/auction/1/reset?seconds={seconds}", "POST")


async def audit(server):
    return await asyncio.to_thread(http, server, "/api/auction/1/audit")


def percentile(values, p):
    values = sorted(values)
    return values[min(len(values) - 1, int(len(values) * p / 100))]


def naive_test(server):
    print("\nTEST 0: old REST approach (100 identical bids of $500 at once)")
    http(server, "/api/auction/2/reset?seconds=60", "POST")
    with ThreadPoolExecutor(max_workers=100) as pool:
        replies = list(pool.map(
            lambda i: http(server, "/api/naive/bid", "POST", {"user_id": f"u{i}", "amount": 500}),
            range(100),
        ))
    winners = sum(r["accepted"] for r in replies)
    print(f"  {winners} bidders were each told they won at $500 (correct answer: 1)")
    print("  Race condition reproduced." if winners > 1 else "  Race not triggered this time; run again.")


async def main(users, servers, rounds):
    main_server = servers[0]
    await asyncio.to_thread(naive_test, main_server)

    print(f"\nConnecting {users} WebSocket bidders to {', '.join(servers)} ...")
    await reset(main_server, 600)
    gate = asyncio.Semaphore(100)
    bidders = [Bidder(f"user_{i}") for i in range(users)]
    started = time.perf_counter()
    outcomes = await asyncio.gather(
        *(b.connect(servers[i % len(servers)], gate) for i, b in enumerate(bidders)),
        return_exceptions=True,
    )
    errors = [o for o in outcomes if isinstance(o, BaseException)]
    bidders = [b for b, o in zip(bidders, outcomes) if not isinstance(o, BaseException)]
    check("all bidders connected", not errors,
          f"({len(bidders)}/{users} in {time.perf_counter() - started:.1f}s)")
    if errors:
        print("   first error:", repr(errors[0]))
    if not bidders:
        return

    print("\nTEST 1: race condition (every bidder sends $500 at the same moment)")
    await reset(main_server, 600)
    await asyncio.sleep(1)
    results = await asyncio.gather(*(b.bid(500) for b in bidders))
    accepted = sum(r["accepted"] for r in results)
    check("exactly one bid accepted", accepted == 1, f"(accepted={accepted}, rejected={len(results) - accepted})")
    report = await audit(main_server)
    check("database holds exactly one bid", report["bid_count"] == 1, f"(rows={report['bid_count']})")

    print("\nTEST 2: idempotency (the same bid_id sent many times at once)")
    same_id = str(uuid.uuid4())
    copies = bidders[:50]
    results = await asyncio.gather(*(b.bid(600, same_id, "retry_user") for b in copies))
    first_time = sum(r["accepted"] and not r["duplicate"] for r in results)
    repeats = sum(r["accepted"] and r["duplicate"] for r in results)
    report = await audit(main_server)
    check("applied only once", first_time == 1 and report["bid_count"] == 2,
          f"(applied={first_time}, recognised as duplicate={repeats} of {len(copies)})")
    check("every copy got the same answer", all(r["accepted"] for r in results))

    print(f"\nTEST 3: final 10 seconds ({len(bidders)} bidders, {rounds} bids each, some after the deadline)")
    await reset(main_server, 10)
    await asyncio.sleep(0.5)

    async def spam(bidder):
        mine = []
        moments = sorted(random.uniform(0, 10.5) for _ in range(rounds))
        begin = time.perf_counter()
        for moment in moments:
            await asyncio.sleep(max(0, moment - (time.perf_counter() - begin)))
            mine.append(await bidder.bid(bidder.state["price"] + random.randint(1, 20)))
        return mine

    began = time.perf_counter()
    results = [r for group in await asyncio.gather(*(spam(b) for b in bidders)) for r in group]
    duration = time.perf_counter() - began
    await asyncio.sleep(1)
    late = await bidders[0].bid(10_000_000)
    await asyncio.sleep(2)  # let the last broadcasts arrive
    report = await audit(main_server)

    won = [r for r in results if r["accepted"]]
    latencies = [r["latency_ms"] for r in results]
    print(f"  bids sent: {len(results)}   accepted: {len(won)}   time: {duration:.1f}s"
          f"   throughput: {len(results) / duration:.0f} bids/s")
    print(f"  reply latency ms:  p50={percentile(latencies, 50):.0f}  p95={percentile(latencies, 95):.0f}"
          f"  p99={percentile(latencies, 99):.0f}  max={max(latencies):.0f}")
    print("  outcomes:", dict(Counter(f"{r['reason']} @ {r['stage']}" for r in results)))
    print("  audit:", report)

    check("no server errors", not any(r["reason"] == "SERVER_BUSY" for r in results))
    check("database is consistent", report["consistent"])
    check("no lost or extra bids", len(won) == report["bid_count"],
          f"(told accepted={len(won)}, saved={report['bid_count']})")
    check("winning price is the highest accepted bid",
          report["current_price"] == max((r["amount"] for r in won), default=report["current_price"]))
    check("bid after the deadline rejected", not late["accepted"], f"({late['reason']})")
    stale = sum(b.state["price"] != report["current_price"] for b in bidders)
    check("no stale screens (all bidders see the final price)", stale == 0, f"(stale={stale})")

    await asyncio.gather(*(b.ws.close() for b in bidders), return_exceptions=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--users", type=int, default=200)
    parser.add_argument("--bids", type=int, default=5, help="bids per user in test 3")
    parser.add_argument("--servers", default="127.0.0.1:8001")
    args = parser.parse_args()
    asyncio.run(main(args.users, args.servers.split(","), args.bids))
    print("\nALL CHECKS PASSED" if not failures else f"\nFAILED: {failures}")
    sys.exit(1 if failures else 0)
