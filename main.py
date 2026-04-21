import asyncio
import json
import ipaddress
from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends, HTTPException , Query, Request
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import text
from dotenv import load_dotenv
from datetime import date
from routers import dashboard

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
from fastapi.openapi.utils import get_openapi
from fastapi.security import APIKeyHeader


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
    swagger_ui_parameters={"docExpansion": "none"}
)

X_FORWARDED_FOR_HEADER = APIKeyHeader(name="X-Forwarded-For", auto_error=False)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://dashboard.lcbh.go.th", 
        "http://localhost:5173" #dev
        ],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["x-api-key", "Content-Type","X-Forwarded-For"],
)

def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    # เพิ่มนิยามของ Header ที่เราต้องการหลอก Swagger
    openapi_schema["components"]["securitySchemes"]["XForwardedFor"] = {
        "type": "apiKey",
        "name": "X-Forwarded-For",
        "in": "header"
    }
    # สั่งให้ทุก Endpoint สามารถใช้ช่องนี้ได้ (หรือจะระบุเฉพาะบางหน้าก็ได้)
    for path in openapi_schema["paths"]:
        for method in openapi_schema["paths"][path]:
            if "security" not in openapi_schema["paths"][path][method]:
                openapi_schema["paths"][path][method]["security"] = []
            openapi_schema["paths"][path][method]["security"].append({"XForwardedFor": []})
            
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi

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
# Router Registration
# ==========================================================
app.include_router(dashboard.router)
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

# ==========================================================
# Security & Access Control: เช็คเครือข่ายภายในโรงพยาบาล
# ==========================================================
INTERNAL_NETWORKS = [
    "10.0.0.0/24",     
    "127.0.0.1/32",  
    "125.24.18.19/32"   
]

def is_ip_internal(client_ip: str) -> bool:
    try:
        client_addr = ipaddress.ip_address(client_ip)
        for network in INTERNAL_NETWORKS:
            if client_addr in ipaddress.ip_network(network):
                return True
        return False
    except ValueError:
        return False

@app.get("/api/check-network", tags=["Security"])
async def check_network(request: Request):
    """ตรวจสอบว่า User ที่เรียกเข้ามา อยู่ในวงเน็ตของโรงพยาบาลหรือไม่"""
    
    # ดึง IP จาก Apache Header (X-Forwarded-For)
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        # กรณีผ่าน Proxy หลายชั้น ตัวแรกสุดคือ IP ของ User จริงๆ
        client_ip = forwarded_for.split(",")[0].strip()
    else:
        # ถ้าไม่มี Header (เช่นรัน Local) ให้ดึงตรงๆ
        client_ip = request.client.host

    is_internal = is_ip_internal(client_ip)
    
    return {
        "isInternal": is_internal,
        "client_ip": client_ip # ส่งกลับไปดูเพื่อเช็คว่าตรวจเจอ IP อะไร
    }