import asyncio
from cache_manager import redis_client, CHANNEL_DASHBOARD

subscribers = set()
latest_data = None


async def broadcaster():
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(CHANNEL_DASHBOARD)

    global latest_data

    async for message in pubsub.listen():
        if message["type"] != "message":
            continue

        data = message["data"]

        if isinstance(data, bytes):
            data = data.decode("utf-8")

        latest_data = data

        # fan-out ไปทุก client
        for queue in list(subscribers):
            try:
                queue.put_nowait(data)
            except:
                pass