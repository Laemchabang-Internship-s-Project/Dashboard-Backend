import asyncio
import json

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from cache_manager import get_cached_data, redis_client, CHANNEL_DASHBOARD

router = APIRouter(
    tags=["OPD Clinics"]
)


@router.get("/summary")
def get_rooms_summary():
    """
    ดึงข้อมูลสรุป OPD ทั้งหมด (อ่านจาก Redis Cache)
    - header: ยอดรวม OPD แบบ Unique
    - rooms: รายละเอียดแต่ละห้องตรวจ
    """
    data = get_cached_data("opd_clinics")
    if not data:
        raise HTTPException(status_code=503, detail="ข้อมูลยังไม่พร้อม กรุณารอสักครู่")
    return data


@router.get("/summary/stream")
async def stream_opd_summary():
    """
    SSE Endpoint สำหรับรับข้อมูล OPD แบบ Real-time
    - ส่งข้อมูลชุดแรกทันทีที่ต่อเข้ามา
    - หลังจากนั้นรอรับสัญญาณ Publish จาก cache_manager ทุก 5 วินาที
    - ส่งเฉพาะ section 'opd_clinics' ไปให้ Frontend
    """
    async def event_generator():
        pubsub = redis_client.pubsub()
        pubsub.subscribe(CHANNEL_DASHBOARD)
        try:
            # ส่งข้อมูลชุดแรกทันที
            initial = get_cached_data("opd_clinics")
            if initial:
                yield f"data: {json.dumps(initial, ensure_ascii=False)}\n\n"

            # รอรับสัญญาณจาก Redis Pub/Sub
            while True:
                message = pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=1.0,
                )
                if message and message["type"] == "message":
                    # แกะเฉพาะ section ที่ต้องการ
                    full_data = json.loads(message["data"])
                    opd_data = full_data.get("opd_clinics", {})
                    yield f"data: {json.dumps(opd_data, ensure_ascii=False)}\n\n"
                else:
                    await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            pass
        finally:
            pubsub.unsubscribe(CHANNEL_DASHBOARD)
            pubsub.close()

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )