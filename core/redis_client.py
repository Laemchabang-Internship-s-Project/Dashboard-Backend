import os
import redis.asyncio as redis

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    password=os.getenv("REDIS_PASSWORD"),
    db=0,
    decode_responses=True
)

CHANNEL_DASHBOARD = "dashboard_events"
KEY_DASHBOARD_CACHE = "dashboard_full_cache"
KEY_FUEL_CACHE   = "fuel_latest"
KEY_FUEL_HISTORY = "fuel_history"   
FUEL_HISTORY_MAX = 100
KEY_GRAPH_CACHE  = "graph_doctor_operation"