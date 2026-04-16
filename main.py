import asyncio
import json
from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import text
from dotenv import load_dotenv

from database_neoq import get_db as get_neoq_db
from database_hos import get_db as get_hos_db
# หมายเหตุ: ตรวจสอบว่ามีไฟล์ security.py ในโฟลเดอร์ utils หากใช้งาน API Key
# from utils.security import get_api_key

# --- นำเข้าจาก cache_manager (ศูนย์กลางข้อมูล) ---
from cache_manager import (
    update_redis_cache,
    redis_client,
    get_cached_data,
    CHANNEL_DASHBOARD,
)

from routers.queue_technical import Xray, Lab, Pharmacy, financial
from routers.queue_clinics import opd_main
from routers import fuel


load_dotenv()


# ==========================================================
# Lifespan: จัดการ Startup / Shutdown ของ App
# ==========================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- Startup ---
    task = asyncio.create_task(update_redis_cache())
    print("[Main] Cache Worker Task เริ่มทำงานแล้ว")
    yield
    # --- Shutdown ---
    task.cancel()
    print("[Main] Cache Worker Task หยุดทำงานแล้ว")


app = FastAPI(
    title="LCBH Dashboard API",
    version="2.0.0",
    description="ระบบ Dashboard โรงพยาบาลแหลมฉบัง (Real-time via Redis Pub/Sub)",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ==========================================================
# SSE Endpoint: Stream ข้อมูล Dashboard แบบ Real-time
# ==========================================================
@app.get("/api/dashboard/stream", tags=["Real-time"])
async def dashboard_stream():
    """
    Endpoint สำหรับเชื่อมต่อ SSE (Server-Sent Events) แบบ Real-time
    """
    async def event_generator():
        pubsub = redis_client.pubsub()
        await pubsub.subscribe(CHANNEL_DASHBOARD)
        try:
            # ส่งข้อมูลชุดแรกทันทีที่ต่อเข้ามา
            initial_data = await redis_client.get("dashboard_full_cache")
            if initial_data:
                yield f"data: {initial_data}\n\n"

            # รอรับสัญญาณจาก Pub/Sub แบบ Async
            async for message in pubsub.listen():
                if message and message["type"] == "message":
                    yield f"data: {message['data']}\n\n"
                    
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


# ==========================================================
# REST Endpoint: ดึงข้อมูล Dashboard แบบ Snapshot
# ==========================================================
@app.get("/api/dashboard/snapshot", tags=["Real-time"])
async def dashboard_snapshot():
    """ดึงข้อมูล Dashboard ทั้งหมดแบบครั้งเดียว (อ่านจาก Redis Cache)"""
    data = await get_cached_data()
    if not data:
        raise HTTPException(status_code=503, detail="ข้อมูลยังไม่พร้อม กรุณารอสักครู่")
    return data


# ==========================================================
# Router Registration
# ==========================================================

# Fuel Webhook (รับ trigger จาก Google Apps Script)
app.include_router(fuel.router)

# แผนกเทคนิค
app.include_router(Lab.router, prefix="/api/technical/lab", tags=["Technical Services"])
app.include_router(Xray.router, prefix="/api/technical/xray", tags=["Technical Services"])
app.include_router(Pharmacy.router, prefix="/api/technical/pharmacy", tags=["Technical Services"])
app.include_router(financial.router, prefix="/api/technical/finance", tags=["Technical Services"])

# ห้องตรวจ OPD
app.include_router(opd_main.router, prefix="/api/clinics", tags=["OPD Clinics"])


# ==========================================================
# System Endpoints
# ==========================================================
@app.get("/api/system/test-neoq-db", tags=["System"])
def test_neoq_db_connection(db: Session = Depends(get_neoq_db)):
    try:
        db.execute(text("SELECT 1"))
        return {"status": "success", "message": "Connected to neoq Database"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB Connection Error: {str(e)}")

@app.get("/api/system/test-hos-db", tags=["System"])
def test_hos_db_connection(db: Session = Depends(get_hos_db)):
    try:
        db.execute(text("SELECT 1"))
        return {"status": "success", "message": "Connected to hos Database"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB Connection Error: {str(e)}")

@app.get("/api/system/test-redis", tags=["System"])
async def test_redis_connection():
    try:
        await redis_client.ping()
        return {"status": "success", "message": "Connected to Redis"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Redis Connection Error: {str(e)}")


@app.get("/api/system/total-services", tags=["System"])
async def get_total_services():
    data = await get_cached_data("system")
    if not data:
        raise HTTPException(status_code=503, detail="ข้อมูลยังไม่พร้อม")
    return {"today_total": data.get("today_total_services", 0)}