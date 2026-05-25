import asyncio
import json
from fastapi import APIRouter, Request, Depends, HTTPException
from fastapi.responses import StreamingResponse
from cache_manager import redis_client, CHANNEL_DASHBOARD, KEY_DASHBOARD_CACHE
from utils.security import get_api_key
from routers.auth import get_current_user
from rate_limiter import limiter

router = APIRouter(prefix="/api/dashboard", tags=["Dashboard"])
from cache_operationroom import KEY_OPERATION_ROOM_CACHE


# --- Helper สำหรับกรองข้อมูลสำหรับคนนอก ---
def get_public_view(full_data: dict):
    """ส่งเฉพาะข้อมูลยอดรวม 4 อย่างหลัก (Public Card)"""
    s = full_data.get("system", {})
    return {
        "system": {
            "total_OPD": s.get("total_OPD", "-"),
            "total_walkin": s.get("total_walkin", "-"),
            "hos_telemed": s.get("hos_telemed", "-"),
            "total_drug_delivery": s.get("total_drug_delivery", "-"),
            "total_drug_delivery_postal": s.get("total_drug_delivery_postal", "-"),
            "total_drug_delivery_rider": s.get("total_drug_delivery_rider", "-"),
        },
        "status": "public_access",
    }


# ==========================================================
# 1. Public Route (เช็ค API Key)
# ==========================================================
@router.get("/public/snapshot", dependencies=[Depends(get_api_key)])
@limiter.limit("400/minute")
async def get_public_snapshot(request: Request):
    raw = await redis_client.get(KEY_DASHBOARD_CACHE)
    if not raw:
        raise HTTPException(status_code=503, detail="Loading...")
    return get_public_view(json.loads(raw))


@router.get("/public/stream")
async def public_stream(request: Request, api_key: str = Depends(get_api_key)):
    async def event_generator():
        pubsub = redis_client.pubsub()
        await pubsub.subscribe(CHANNEL_DASHBOARD)
        try:
            initial = await redis_client.get(KEY_DASHBOARD_CACHE)
            if initial:
                yield f"data: {json.dumps(get_public_view(json.loads(initial)))}\n\n"

            while True:
                if await request.is_disconnected():
                    break
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=1.0
                )
                if message:
                    data = json.loads(message["data"])
                    yield f"data: {json.dumps(get_public_view(data))}\n\n"
                else:
                    yield ": ping\n\n"
                await asyncio.sleep(0.5)
        finally:
            await pubsub.unsubscribe(CHANNEL_DASHBOARD)
            await pubsub.close()

    return StreamingResponse(event_generator(), media_type="text/event-stream")


# ==========================================================
# 2. Internal Route (เช็ค API Key + JWT Token)
# ==========================================================
@router.get("/internal/snapshot", dependencies=[Depends(get_api_key)])
@limiter.limit("300/minute")
async def get_internal_snapshot(
    request: Request,
    _user: dict = Depends(get_current_user),
):
    raw = await redis_client.get(KEY_DASHBOARD_CACHE)
    if not raw:
        raise HTTPException(status_code=503, detail="Loading...")
    return json.loads(raw)


@router.get("/internal/stream")
async def internal_stream(
    request: Request,
    api_key: str = Depends(get_api_key),
    _user: dict = Depends(get_current_user),
):
    async def event_generator():
        pubsub = redis_client.pubsub()
        await pubsub.subscribe(CHANNEL_DASHBOARD)
        try:
            initial = await redis_client.get(KEY_DASHBOARD_CACHE)
            if initial:
                yield f"data: {initial}\n\n"

            while True:
                if await request.is_disconnected():
                    break
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=1.0
                )
                if message:
                    yield f"data: {message['data']}\n\n"
                else:
                    yield ": ping\n\n"
                await asyncio.sleep(0.5)
        finally:
            await pubsub.unsubscribe(CHANNEL_DASHBOARD)
            await pubsub.close()

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@router.get("/internal/operation-rooms", dependencies=[Depends(get_api_key)])
@limiter.limit("50/minute")
async def get_operation_rooms_summary(
    request: Request, _user: dict = Depends(get_current_user)
):
    try:
        from cache_operationroom import KEY_OPERATION_ROOM_CACHE

        raw = await redis_client.get(KEY_OPERATION_ROOM_CACHE)

        if not raw:
            raise HTTPException(
                status_code=503, detail="กำลังประมวลผลข้อมูลห้องผ่าตัด กรุณารอสักครู่..."
            )

        return {"status": "success", "data": json.loads(raw)}
    except Exception as e:
        print(f"[Operation Endpoint] Error: {e}")
        raise HTTPException(status_code=500, detail="ไม่สามารถดึงข้อมูลห้องผ่าตัดได้")
