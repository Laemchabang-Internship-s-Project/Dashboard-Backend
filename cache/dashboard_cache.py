import json
from core.redis_client import (
    redis_client, CHANNEL_DASHBOARD, KEY_DASHBOARD_CACHE, 
    KEY_FUEL_CACHE, KEY_FUEL_HISTORY, FUEL_HISTORY_MAX, KEY_GRAPH_CACHE
)

async def get_cached_data(section: str = None):
    raw = await redis_client.get(KEY_DASHBOARD_CACHE)
    if not raw:
        return None
    data = json.loads(raw)
    return data.get(section) if section else data

async def patch_redis_cache(new_data: dict):
    try:
        raw = await redis_client.get(KEY_DASHBOARD_CACHE)
        full_data = json.loads(raw) if raw else {}

        for key, value in new_data.items():
            if isinstance(value, dict) and key in full_data and isinstance(full_data[key], dict):
                full_data[key].update(value)
            else:
                full_data[key] = value

        if "car" not in full_data:
            fuel_raw = await redis_client.get(KEY_FUEL_CACHE)
            full_data["car"] = {"fuel_latest": json.loads(fuel_raw) if fuel_raw else None}

        json_data = json.dumps(full_data, ensure_ascii=False)
        await redis_client.set(KEY_DASHBOARD_CACHE, json_data)
        await redis_client.publish(CHANNEL_DASHBOARD, json_data)
    except Exception as e:
        print(f"[Patch Cache] Error: {e}")

async def update_fuel_cache(fuel_data: dict) -> bool:
    try:
        json_str = json.dumps(fuel_data, ensure_ascii=False)
        await redis_client.set(KEY_FUEL_CACHE, json_str)
        await redis_client.lpush(KEY_FUEL_HISTORY, json_str)
        await redis_client.ltrim(KEY_FUEL_HISTORY, 0, FUEL_HISTORY_MAX - 1)
        await patch_redis_cache({"car": {"fuel_latest": fuel_data}})
        print(f"[Fuel Webhook] อัปเดตสำเร็จ: {fuel_data}")
        return True
    except Exception as e:
        print(f"[Fuel Webhook] Error: {e}")
        return False

async def get_graph_data():
    raw = await redis_client.get(KEY_GRAPH_CACHE)
    if not raw:
        return []
    return json.loads(raw)