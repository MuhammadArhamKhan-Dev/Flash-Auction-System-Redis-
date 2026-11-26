"""PostgreSQL: the source of truth."""
import asyncpg

from . import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS auctions (
    id            INT PRIMARY KEY,
    item          TEXT        NOT NULL,
    start_price   BIGINT      NOT NULL,
    current_price BIGINT      NOT NULL,
    leader        TEXT,
    round         INT         NOT NULL DEFAULT 1,   -- +1 on every reset
    version       INT         NOT NULL DEFAULT 0,   -- +1 on every accepted bid
    ends_at       TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS bids (
    bid_id     UUID PRIMARY KEY,                    -- idempotency key
    auction_id INT         NOT NULL REFERENCES auctions(id),
    seq        INT         NOT NULL,                -- order of acceptance
    user_id    TEXT        NOT NULL,
    amount     BIGINT      NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (auction_id, seq)                        -- two bids can never take the same slot
);

INSERT INTO auctions (id, item, start_price, current_price, ends_at) VALUES
    (1, 'Limited Edition Smart Watch', 100, 100, now() + interval '2 minutes'),
    (2, 'Naive REST demo item',        100, 100, now() + interval '2 minutes')
ON CONFLICT (id) DO NOTHING;
"""


async def create_pool() -> asyncpg.Pool:
    # A small fixed pool: extra bids wait in line instead of exhausting PostgreSQL.
    pool = await asyncpg.create_pool(config.DATABASE_URL, min_size=2, max_size=config.DB_POOL_SIZE)
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Lock so that two instances starting together do not create tables at the same time.
            await conn.execute("SELECT pg_advisory_xact_lock(42)")
            await conn.execute(SCHEMA)
    return pool
