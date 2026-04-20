import asyncio
import json
from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends, HTTPException , Query, Request
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import text
from dotenv import load_dotenv
from datetime import date

from database_neoq import get_db as get_neoq_db
from database_hos import get_db as get_hos_db
# หมายเหตุ: ตรวจสอบว่ามีไฟล์ security.py ในโฟลเดอร์ utils หากใช้งาน API Key
from utils.security import get_api_key

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
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://dashboard.lcbh.go.th"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["x-api-key", "Content-Type"],
)

@app.get("/api/dashboard/summary-range", tags=["Filter"])
async def get_summary_range(
    api_key: str = Depends(get_api_key),
    start_date: date = Query(..., description="วันที่เริ่มต้น (YYYY-MM-DD)"),
    end_date: date = Query(..., description="วันที่สิ้นสุด (YYYY-MM-DD)"),
    db_hos: Session = Depends(get_hos_db) # ใช้ Dependency สำหรับต่อ HOSxP
):
    """ดึงข้อมูลสรุปยอดบริการ 4 อย่างหลัก ตามช่วงวันที่ที่กำหนด"""
    
    # 1. สร้าง Cache Key เพื่อเช็คใน Redis ก่อน (ป้องกันการรัน SQL ซ้ำๆ เมื่อเปิดพร้อมกัน)
    cache_key = f"summary:range:{start_date}:{end_date}"
    
    try:
        # 2. ลองดึงจาก Redis
        cached_raw = await redis_client.get(cache_key)
        if cached_raw:
            return json.loads(cached_raw)

        # 3. ถ้าไม่มีใน Cache ให้ Query จาก HOSxP
        # ใช้ Logic การรวมกลุ่ม (Walk-in + Kiosk) ตามมาตรฐานที่คุณใช้ใน cache_manager.py
        sql = text("""
            SELECT 
                COUNT(vn) as total_opd,
                SUM(CASE WHEN ovstist IN ('01', '06') THEN 1 ELSE 0 END) as walk_in,
                SUM(CASE WHEN ovstist = '05' THEN 1 ELSE 0 END) as telemed,
                (SELECT COUNT(DISTINCT o.vn) 
                 FROM opitemrece o 
                 WHERE o.vstdate BETWEEN :start AND :end 
                 AND o.icode IN ('3907489', '3907018', '3907508')
                ) as drug_delivery
            FROM ovst 
            WHERE vstdate BETWEEN :start AND :end
        """)
        
        res = db_hos.execute(sql, {"start": start_date, "end": end_date}).fetchone()
        
        result_data = {
            "period": {"start": str(start_date), "end": str(end_date)},
            "data": {
                "opd_total": int(res[0] or 0),
                "walk_in": int(res[1] or 0),
                "telemed": int(res[2] or 0),
                "drug_delivery": int(res[3] or 0)
            }
        }

        # 4. บันทึกลง Redis (ตั้งเวลา Expire 5 นาที เพื่อให้คนอื่นที่เปิดพร้อมกันได้ใช้ด้วย)
        await redis_client.set(cache_key, json.dumps(result_data), ex=300)
        
        return result_data

    except Exception as e:
        print(f"[Summary Range] Error: {e}")
        raise HTTPException(status_code=500, detail="ไม่สามารถดึงข้อมูลสรุปช่วงเวลาได้")

# ==========================================================
# SSE Endpoint: Stream ข้อมูล Dashboard แบบ Real-time
# ==========================================================
@app.get("/api/dashboard/stream", tags=["Real-time"])
async def dashboard_stream(
    request: Request,
    api_key: str = Depends(get_api_key)  # ✅ เปิด auth สำหรับ SSE
):
    async def event_generator():
        pubsub = redis_client.pubsub()
        await pubsub.subscribe(CHANNEL_DASHBOARD)

        try:
            # ✅ ส่ง snapshot ครั้งแรก
            initial_data = await redis_client.get("dashboard_full_cache")
            if initial_data:
                if isinstance(initial_data, bytes):
                    initial_data = initial_data.decode("utf-8")

                yield f"data: {initial_data}\n\n"

            while True:
                # ถ้า client disconnect → stop loop ทันที
                if await request.is_disconnected():
                    print("🔌 Client disconnected")
                    break

                message = await pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=1.0
                )

                if message:
                    data = message["data"]

                    if isinstance(data, bytes):
                        data = data.decode("utf-8")

                    yield f"data: {data}\n\n"
                else:
                    # keep-alive กัน proxy ตัด connection
                    yield ": ping\n\n"

                await asyncio.sleep(0.1)

        except asyncio.CancelledError:
            print("🔌 SSE cancelled")

        finally:
            await pubsub.unsubscribe(CHANNEL_DASHBOARD)
            await pubsub.close()
            print("🧹 Redis pubsub closed")

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            # ปิด buffering reverse proxy
            "X-Accel-Buffering": "no",
            #กัน proxy แปลง content
            "Content-Type": "text/event-stream",
        },
    )

# ==========================================================
# REST Endpoint: ดึงข้อมูล Dashboard แบบ Snapshot
# ==========================================================
@app.get("/api/dashboard/snapshot", tags=["Real-time"])
async def dashboard_snapshot(
    api_key: str = Depends(get_api_key)
):
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
app.include_router(Lab.router, prefix="/api/technical/lab", tags=["Technical Services"], dependencies=[Depends(get_api_key)])
app.include_router(Xray.router, prefix="/api/technical/xray", tags=["Technical Services"], dependencies=[Depends(get_api_key)])
app.include_router(Pharmacy.router, prefix="/api/technical/pharmacy", tags=["Technical Services"], dependencies=[Depends(get_api_key)])
app.include_router(financial.router, prefix="/api/technical/finance", tags=["Technical Services"], dependencies=[Depends(get_api_key)])

# ห้องตรวจ OPD
app.include_router(opd_main.router, prefix="/api/clinics", tags=["OPD Clinics"], dependencies=[Depends(get_api_key)])


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