"""
Core bidding engine.

A bid passes through two stages:
  1. Redis pre-filter : cheap rejection of duplicate and already-too-low bids.
  2. PostgreSQL       : SELECT ... FOR UPDATE row lock, so bids on one auction are
                        validated and written strictly one at a time.
"""
import json
from uuid import UUID

CHANNEL = "auction_updates"

# Runs atomically inside Redis:
#   a) remembers an accepted bid_id (idempotency),
#   b) updates the cached price and publishes it, ONLY if this state is newer than the cached one.
#      A late, older update can therefore never overwrite or follow a newer one.
PUBLISH_LUA = """
if ARGV[5] ~= '' then
    redis.call('SET', KEYS[2], ARGV[5], 'EX', 3600)
end
local round = tonumber(redis.call('HGET', KEYS[1], 'round') or '-1')
local version = tonumber(redis.call('HGET', KEYS[1], 'version') or '-1')
local new_round, new_version = tonumber(ARGV[1]), tonumber(ARGV[2])
if new_round > round or (new_round == round and new_version > version) then
    redis.call('HSET', KEYS[1], 'round', ARGV[1], 'version', ARGV[2], 'price', ARGV[3])
    redis.call('PUBLISH', KEYS[3], ARGV[4])
    return 1
end
return 0
"""


def cache_key(auction_id: int) -> str:
    return f"auction:{auction_id}:cache"


def to_state(row) -> dict:
    """The message every client receives. Time remaining is computed from the database clock."""
    remaining = (row["ends_at"] - row["now"]).total_seconds()
    return {
        "type": "state",
        "auction_id": row["id"],
        "item": row["item"],
        "price": row["current_price"],
        "leader": row["leader"],
        "round": row["round"],
        "version": row["version"],
        "ends_in_ms": max(0, int(remaining * 1000)),
    }


class BiddingEngine:
    def __init__(self, pool, redis):
        self.pool = pool
        self.redis = redis
        self.publish_script = redis.register_script(PUBLISH_LUA)

    async def get_state(self, auction_id: int):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT *, clock_timestamp() AS now FROM auctions WHERE id = $1", auction_id
            )
        return to_state(row) if row else None

    async def publish(self, state: dict, idem_key: str = "", idem_value: str = ""):
        auction_id = state["auction_id"]
        await self.publish_script(
            keys=[cache_key(auction_id), idem_key or "unused", CHANNEL],
            args=[state["round"], state["version"], state["price"], json.dumps(state), idem_value],
        )

    async def place_bid(self, auction_id: int, bid_id: UUID, user_id: str, amount: int) -> dict:
        def result(accepted, reason, stage, price=None, duplicate=False):
            return {
                "type": "bid_result", "bid_id": str(bid_id), "amount": amount,
                "accepted": accepted, "reason": reason, "stage": stage,
                "price": price, "duplicate": duplicate,
            }

        def repeat(owner, old_amount, stage):
            """Same bid_id seen again: repeat the original answer, never apply it twice."""
            if owner == user_id and old_amount == amount:
                return result(True, "ACCEPTED", stage, amount, duplicate=True)
            return result(False, "BID_ID_REUSED", stage)

        # ---- Stage 1: Redis pre-filter (both values are read in one atomic step) ----
        idem_key = f"bid:{auction_id}:{bid_id}"
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.get(idem_key)
            pipe.hget(cache_key(auction_id), "price")
            seen, cached_price = await pipe.execute()

        if seen:
            old = json.loads(seen)
            return repeat(old["user_id"], old["amount"], "redis")
        # The cache is written only after a commit, so it is never ahead of the real price.
        if cached_price is not None and amount <= int(cached_price):
            return result(False, "BID_TOO_LOW", "redis", int(cached_price))

        # ---- Stage 2: PostgreSQL transaction with a row-level lock ----
        state = None
        async with self.pool.acquire(timeout=10) as conn:
            async with conn.transaction():
                auction = await conn.fetchrow(
                    "SELECT * FROM auctions WHERE id = $1 FOR UPDATE", auction_id
                )
                if auction is None:
                    return result(False, "NO_SUCH_AUCTION", "postgres")

                old = await conn.fetchrow(
                    "SELECT user_id, amount FROM bids WHERE bid_id = $1", bid_id
                )
                if old:
                    return repeat(old["user_id"], old["amount"], "postgres")

                # Server time, read AFTER the lock is held. Client clocks are never trusted.
                now = await conn.fetchval("SELECT clock_timestamp()")
                if now >= auction["ends_at"]:
                    return result(False, "AUCTION_ENDED", "postgres", auction["current_price"])
                if amount <= auction["current_price"]:
                    return result(False, "BID_TOO_LOW", "postgres", auction["current_price"])

                row = await conn.fetchrow(
                    """UPDATE auctions SET current_price = $2, leader = $3, version = version + 1
                       WHERE id = $1 RETURNING *, clock_timestamp() AS now""",
                    auction_id, amount, user_id,
                )
                await conn.execute(
                    """INSERT INTO bids (bid_id, auction_id, seq, user_id, amount, created_at)
                       VALUES ($1, $2, $3, $4, $5, $6)""",
                    bid_id, auction_id, row["version"], user_id, amount, now,
                )
                state = to_state(row)

        # Committed. Only now tell Redis, which informs every backend instance.
        await self.publish(state, idem_key, json.dumps({"user_id": user_id, "amount": amount}))
        return result(True, "ACCEPTED", "postgres", amount)

    async def reset(self, auction_id: int, seconds: int):
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.fetchrow("SELECT id FROM auctions WHERE id = $1 FOR UPDATE", auction_id)
                await conn.execute("DELETE FROM bids WHERE auction_id = $1", auction_id)
                row = await conn.fetchrow(
                    """UPDATE auctions
                       SET current_price = start_price, leader = NULL, round = round + 1, version = 0,
                           ends_at = clock_timestamp() + $2::int * INTERVAL '1 second'
                       WHERE id = $1 RETURNING *, clock_timestamp() AS now""",
                    auction_id, seconds,
                )
        if row is None:
            return None
        state = to_state(row)
        await self.publish(state)
        return state

    async def audit(self, auction_id: int):
        """Consistency check used by the stress test and for the CEP evidence."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                WITH b AS (
                    SELECT amount, created_at, seq,
                           LAG(amount) OVER (ORDER BY seq) AS previous
                    FROM bids WHERE auction_id = $1
                )
                SELECT a.current_price, a.leader, a.version,
                       (SELECT COUNT(*) FROM b)                             AS bid_count,
                       (SELECT COALESCE(MAX(amount), a.start_price) FROM b) AS highest_bid,
                       (SELECT COUNT(*) FROM b WHERE amount <= previous)    AS out_of_order,
                       (SELECT COUNT(*) FROM b WHERE created_at >= a.ends_at) AS late_bids
                FROM auctions a WHERE a.id = $1
                """,
                auction_id,
            )
        if row is None:
            return None
        data = dict(row)
        data["consistent"] = (
            data["current_price"] == data["highest_bid"]
            and data["bid_count"] == data["version"]
            and data["out_of_order"] == 0
            and data["late_bids"] == 0
        )
        return data
