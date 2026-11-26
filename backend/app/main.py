"""FastAPI routes and the WebSocket endpoint."""
import asyncio
import json
import logging
from contextlib import asynccontextmanager
from uuid import UUID

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field, ValidationError

from . import config, db, naive
from .bidding import BiddingEngine, cache_key
from .realtime import Hub

log = logging.getLogger("uvicorn.error")
hub = Hub()


class BidMessage(BaseModel):
    bid_id: UUID
    user_id: str = Field(min_length=1, max_length=40)
    amount: int = Field(strict=True, gt=0, le=1_000_000_000)


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await db.create_pool()
    redis = aioredis.Redis(
        connection_pool=aioredis.BlockingConnectionPool.from_url(
            config.REDIS_URL, max_connections=50, decode_responses=True
        )
    )
    engine = BiddingEngine(pool, redis)
    # Clear any old cache (for example after the database was recreated). A miss is always safe.
    await redis.delete(cache_key(1), cache_key(2))
    app.state.engine = engine
    listener = asyncio.create_task(hub.listen(redis, engine))
    log.info("Instance %s ready", config.INSTANCE)
    yield
    listener.cancel()
    await asyncio.gather(listener, return_exceptions=True)
    await redis.aclose()
    await pool.close()


app = FastAPI(title="Flash Auction", lifespan=lifespan)
app.include_router(naive.router)


def found(value):
    if value is None:
        raise HTTPException(status_code=404, detail="No such auction")
    return value


@app.get("/api/health")
async def health():
    return {"status": "ok", "instance": config.INSTANCE, "connections": hub.count()}


@app.get("/api/auction/{auction_id}")
async def get_auction(auction_id: int):
    return found(await app.state.engine.get_state(auction_id))


@app.post("/api/auction/{auction_id}/reset")
async def reset_auction(auction_id: int, seconds: int = Query(60, ge=5, le=3600)):
    return found(await app.state.engine.reset(auction_id, seconds))


@app.get("/api/auction/{auction_id}/audit")
async def audit_auction(auction_id: int):
    return found(await app.state.engine.audit(auction_id))


@app.websocket("/ws/auction/{auction_id}")
async def auction_socket(websocket: WebSocket, auction_id: int):
    await websocket.accept()
    engine = app.state.engine
    # Join the room BEFORE reading the snapshot, so no update can fall between the two.
    client = hub.add(auction_id, websocket)
    try:
        state = await engine.get_state(auction_id)
        if state is None:
            await websocket.close(code=1008)
            return
        client.send(json.dumps({"type": "hello", "instance": config.INSTANCE}))
        client.send(json.dumps(state))
        while True:
            text = await websocket.receive_text()
            try:
                bid = BidMessage.model_validate_json(text)
            except ValidationError:
                client.send(json.dumps({"type": "error", "reason": "INVALID_MESSAGE"}))
                continue
            try:
                reply = await engine.place_bid(auction_id, bid.bid_id, bid.user_id, bid.amount)
            except Exception:
                log.exception("Bid failed")
                # The client may safely retry with the SAME bid_id.
                reply = {"type": "bid_result", "bid_id": str(bid.bid_id), "amount": bid.amount,
                         "accepted": False, "reason": "SERVER_BUSY", "stage": "error",
                         "price": None, "duplicate": False}
            client.send(json.dumps(reply))
    except WebSocketDisconnect:
        pass
    finally:
        hub.remove(auction_id, client)
