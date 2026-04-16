import asyncio
import json

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from cache_manager import get_cached_data, redis_client, CHANNEL_DASHBOARD

router = APIRouter(
    tags=["OPD Clinics"]
)


@router.get("/summary")
async def get_rooms_summary():
    """
    ดึงข้อมูลสรุป OPD ทั้งหมด (อ่านจาก Redis Cache)
    """
    data = await get_cached_data("opd_clinics")
    if not data:
        raise HTTPException(status_code=503, detail="ข้อมูลยังไม่พร้อม กรุณารอสักครู่")
    return data


@router.get("/summary/stream")
async def stream_opd_summary():
    """
    SSE Endpoint สำหรับรับข้อมูล OPD แบบ Real-time เฉพาะส่วน
    """
    async def event_generator():
        pubsub = redis_client.pubsub()
        await pubsub.subscribe(CHANNEL_DASHBOARD)
        try:
            # ส่งข้อมูลชุดแรกทันทีที่ต่อเข้ามา
            initial = await get_cached_data("opd_clinics")
            if initial:
                yield f"data: {json.dumps(initial, ensure_ascii=False)}\n\n"

            # รอรับสัญญาณจาก Redis Pub/Sub แบบ Async
            async for message in pubsub.listen():
                if message and message["type"] == "message":
                    # แกะเฉพาะส่วน opd_clinics ออกมาจากข้อมูลเต็ม
                    full_data = json.loads(message["data"])
                    opd_data = full_data.get("opd_clinics", {})
                    yield f"data: {json.dumps(opd_data, ensure_ascii=False)}\n\n"
                    
        except asyncio.CancelledError:
            pass
        finally:
            await pubsub.unsubscribe(CHANNEL_DASHBOARD)
            await pubsub.close()

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )