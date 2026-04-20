import asyncio
from cache_manager import redis_client, CHANNEL_DASHBOARD

subscribers = set()
latest_data = None

async def broadcaster():
    global latest_data

    pubsub = redis_client.pubsub()
    await pubsub.subscribe(CHANNEL_DASHBOARD)

    async for msg in pubsub.listen():
        if msg and msg["type"] == "message":
            data = msg["data"]

            if isinstance(data, bytes):
                data = data.decode("utf-8")

            latest_data = data

            # broadcast ไปทุก worker
            dead = set()
            for q in subscribers:
                try:
                    q.put_nowait(data)
                except:
                    dead.add(q)

            subscribers.difference_update(dead)