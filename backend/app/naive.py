"""
DELIBERATELY BROKEN endpoint that imitates the company's old REST API.
It exists only to demonstrate the race condition in the CEP ("before" evidence).
"""
import asyncio

from fastapi import APIRouter, Request
from pydantic import BaseModel

router = APIRouter(prefix="/api/naive")


class NaiveBid(BaseModel):
    user_id: str
    amount: int


@router.post("/bid")
async def naive_bid(bid: NaiveBid, request: Request):
    pool = request.app.state.engine.pool
    price = await pool.fetchval("SELECT current_price FROM auctions WHERE id = 2")  # 1. read
    await asyncio.sleep(0.05)                                                       # 2. "business logic"
    if bid.amount <= price:                                                         # 3. check stale value
        return {"accepted": False}
    await pool.execute(                                                             # 4. blind write
        "UPDATE auctions SET current_price = $1, leader = $2 WHERE id = 2",
        bid.amount, bid.user_id,
    )
    return {"accepted": True}
