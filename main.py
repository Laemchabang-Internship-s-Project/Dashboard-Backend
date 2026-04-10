import asyncio
import json
from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from sqlalchemy import text
from dotenv import load_dotenv

from database import get_db
from utils.security import get_api_key

# --- นำเข้าจาก cache_manager (ศูนย์กลางข้อมูล) ---
from cache_manager import (
    update_redis_cache,
    redis_client,
    get_cached_data,
    CHANNEL_DASHBOARD,
)

from routers import Car
from routers.queue_technical import Xray, Lab, Pharmacy, financial
from routers.queue_clinics import opd_main


load_dotenv()


# ==========================================================
# Lifespan: จัดการ Startup / Shutdown ของ App
# (ใช้แทน @app.on_event ที่ deprecated ใน FastAPI เวอร์ชันใหม่)
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
    dependencies=[Depends(get_api_key)],
)


# ==========================================================
# SSE Endpoint: Stream ข้อมูล Dashboard แบบ Real-time
# ==========================================================
@app.get("/api/dashboard/stream", tags=["Real-time"])
async def dashboard_stream():
    """
    Endpoint สำหรับเชื่อมต่อ SSE (Server-Sent Events) แบบ Real-time
    - เมื่อต่อเข้ามาจะได้รับข้อมูลชุดแรกทันที
    - หลังจากนั้นจะรอรับข้อมูลอัปเดตผ่าน Redis Pub/Sub ทุก 5 วินาที
    """
    async def event_generator():
        pubsub = redis_client.pubsub()
        pubsub.subscribe(CHANNEL_DASHBOARD)
        try:
            # ส่งข้อมูลชุดแรกทันทีที่ต่อเข้ามา (ไม่ต้องรอ 5 วินาที)
            initial_data = redis_client.get("dashboard_full_cache")
            if initial_data:
                yield f"data: {initial_data}\n\n"

            # รอรับสัญญาณจาก Pub/Sub (ไม่ Polling, ไม่โหลด CPU)
            while True:
                message = pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=1.0,
                )
                if message and message["type"] == "message":
                    yield f"data: {message['data']}\n\n"
                else:
                    # ให้ asyncio มีโอกาสทำงานอื่น (ป้องกัน blocking)
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
            "X-Accel-Buffering": "no",  # ป้องกัน Nginx buffer SSE
        },
    )


# ==========================================================
# REST Endpoint: ดึงข้อมูล Dashboard แบบ Snapshot (อ่านจาก Redis)
# ==========================================================
@app.get("/api/dashboard/snapshot", tags=["Real-time"])
def dashboard_snapshot():
    """ดึงข้อมูล Dashboard ทั้งหมดแบบครั้งเดียว (อ่านจาก Redis Cache)"""
    data = get_cached_data()
    if not data:
        raise HTTPException(status_code=503, detail="ข้อมูลยังไม่พร้อม กรุณารอสักครู่")
    return data


# ==========================================================
# Router Registration
# ==========================================================
# --- กลุ่มที่ 1: ระบบซัพพอร์ต ---
# app.include_router(Power.router)
app.include_router(Car.router)
# app.include_router(WFH.router)

# --- กลุ่มที่ 2: แผนกเทคนิค ---
app.include_router(Lab.router, prefix="/api/technical/lab", tags=["Technical Services"])
app.include_router(Xray.router, prefix="/api/technical/xray", tags=["Technical Services"])
app.include_router(Pharmacy.router, prefix="/api/technical/pharmacy", tags=["Technical Services"])
app.include_router(financial.router, prefix="/api/technical/finance", tags=["Technical Services"])

# --- กลุ่มที่ 3: ห้องตรวจ OPD ---
app.include_router(opd_main.router, prefix="/api/clinics", tags=["OPD Clinics"])


# ==========================================================
# System Endpoints
# ==========================================================
@app.get("/api/system/test-db", tags=["System"])
def test_db_connection(db: Session = Depends(get_db)):
    """ทดสอบการเชื่อมต่อ Database"""
    try:
        db.execute(text("SELECT 1"))
        return {"status": "success", "message": "Connected to Database"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB Connection Error: {str(e)}")


@app.get("/api/system/test-redis", tags=["System"])
def test_redis_connection():
    """ทดสอบการเชื่อมต่อ Redis"""
    try:
        redis_client.ping()
        return {"status": "success", "message": "Connected to Redis"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Redis Connection Error: {str(e)}")


@app.get("/api/system/total-services", tags=["System"])
def get_total_services():
    """ดึงจำนวนผู้รับบริการวันนี้ (จาก Redis Cache แทน DB ตรง)"""
    data = get_cached_data("system")
    if not data:
        raise HTTPException(status_code=503, detail="ข้อมูลยังไม่พร้อม")
    return {"today_total": data.get("today_total_services", 0)}