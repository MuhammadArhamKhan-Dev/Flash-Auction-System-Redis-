"""WebSocket connections of THIS instance, fed by Redis Pub/Sub."""
import asyncio
import json
import logging

from .bidding import CHANNEL

log = logging.getLogger("uvicorn.error")


class Client:
    """One socket with its own outgoing queue, so a slow client never delays the others."""

    def __init__(self, websocket):
        self.websocket = websocket
        self.queue = asyncio.Queue(maxsize=100)
        self.writer = asyncio.create_task(self._write())

    def send(self, text: str):
        if self.queue.full():
            self.queue.get_nowait()  # drop the oldest message; newer prices replace it
        self.queue.put_nowait(text)

    async def _write(self):
        try:
            while True:
                await self.websocket.send_text(await self.queue.get())
        except Exception:
            pass  # socket closed; the receive loop does the cleanup


class Hub:
    def __init__(self):
        self.rooms: dict[int, set[Client]] = {}

    def add(self, auction_id: int, websocket) -> Client:
        client = Client(websocket)
        self.rooms.setdefault(auction_id, set()).add(client)
        return client

    def remove(self, auction_id: int, client: Client):
        client.writer.cancel()
        self.rooms.get(auction_id, set()).discard(client)

    def broadcast(self, auction_id: int, text: str):
        for client in self.rooms.get(auction_id, set()):
            client.send(text)

    def count(self) -> int:
        return sum(len(room) for room in self.rooms.values())

    async def listen(self, redis, engine):
        """Relay every published state to local sockets. Reconnects if Redis restarts."""
        while True:
            pubsub = redis.pubsub()
            try:
                await pubsub.subscribe(CHANNEL)
                # Resend current state in case updates were missed while disconnected.
                for auction_id in list(self.rooms):
                    state = await engine.get_state(auction_id)
                    if state:
                        self.broadcast(auction_id, json.dumps(state))
                async for message in pubsub.listen():
                    if message["type"] == "message":
                        data = message["data"]
                        self.broadcast(json.loads(data)["auction_id"], data)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                log.warning("Redis listener error: %s (retrying)", error)
                await asyncio.sleep(1)
            finally:
                try:
                    await pubsub.aclose()
                except Exception:
                    pass
