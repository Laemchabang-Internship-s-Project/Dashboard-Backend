import asyncio
import json
import ipaddress
from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends, HTTPException , Query, Request, Response
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import text
from dotenv import load_dotenv
from datetime import date
from routers import dashboard

from database_neoq import get_db as get_neoq_db
from database_hos import get_db as get_hos_db
# หมายเหตุ: ตรวจสอบว่ามีไฟล์ security.py ในโฟลเดอร์ utils หากใช้งาน API Key
from utils.security import get_api_key
from slowapi.middleware import SlowAPIMiddleware
from utils.network import get_client_ip

# --- นำเข้าจาก cache_manager (ศูนย์กลางข้อมูล) ---
from cache_manager import (
    update_redis_cache,
    redis_client,
    get_cached_data,
    CHANNEL_DASHBOARD,
)
from routers import fuel
from fastapi.openapi.utils import get_openapi
from fastapi.security import APIKeyHeader
from utils.security import verify_ip

# --- Rate Limiter ---
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from rate_limiter import limiter


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
app.add_middleware(SlowAPIMiddleware)


# ==========================================================
# Rate Limiter — ผูกกับ app
# ==========================================================
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)



X_FORWARDED_FOR_HEADER = APIKeyHeader(name="X-Forwarded-For", auto_error=False)


# ==========================================================
# Middleware Stack (เรียงจากล่างขึ้นบน = ทำงานจากบนลงล่าง)
# ==========================================================

# 1. CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://dashboard.lcbh.go.th",
        "http://localhost:5173"
        ],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["x-api-key", "Content-Type","X-Forwarded-For"],
)

# 2. Trusted Host Middleware — จำกัด hostname ที่รับ
app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=[
        "dashboard.lcbh.go.th",
        "localhost",
        "127.0.0.1",
        "*.lcbh.go.th",
    ],
)


# ==========================================================
# 3. Security Headers Middleware — ป้องกัน XSS, Clickjacking, MIME Sniffing
# ==========================================================
@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    
    # ป้องกัน MIME type sniffing
    response.headers["X-Content-Type-Options"] = "nosniff"
    # ป้องกัน Clickjacking (ไม่ให้แสดงใน iframe)
    response.headers["X-Frame-Options"] = "DENY"
    # ป้องกัน XSS (สำหรับ browser เก่า)
    response.headers["X-XSS-Protection"] = "1; mode=block"
    # บังคับใช้ HTTPS (1 ปี)
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    # ป้องกัน information leakage ผ่าน Referer
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    # ป้องกันการเรียกใช้ API ที่ไม่ได้รับอนุญาต
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    # ไม่ให้ cache API response (ป้องกันข้อมูลเก่าค้าง)
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    
    return response


# ==========================================================
# 4. Request Size Limit Middleware — จำกัดขนาด body ≤ 1MB
# ==========================================================
MAX_REQUEST_BODY_SIZE = 1 * 1024 * 1024  # 1 MB

@app.middleware("http")
async def limit_request_size(request: Request, call_next):
    # ตรวจสอบ Content-Length header
    content_length = request.headers.get("content-length")
    if content_length:
        if int(content_length) > MAX_REQUEST_BODY_SIZE:
            return JSONResponse(
                status_code=413,
                content={"detail": "Request body too large (สูงสุด 1MB)"}
            )
    return await call_next(request)


# ==========================================================
# OpenAPI Customization
# ==========================================================
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

# ==========================================================
# Endpoints ใน main.py (พร้อม Rate Limit)
# ==========================================================
@app.get("/api/dashboard/summary-range", tags=["Filter"])
@limiter.limit("20/minute")
async def get_summary_range(
    request: Request,
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

# ==========================================================
# System Endpoints
# ==========================================================
@app.get("/api/system/health", tags=["System"], dependencies=[Depends(get_api_key), Depends(verify_ip)])
@limiter.limit("10/minute")
async def health_check(
    request: Request,
    neoq_db: Session = Depends(get_neoq_db),
    hos_db: Session = Depends(get_hos_db),
):
    results = {}

    # neoq DB
    try:
        neoq_db.execute(text("SELECT 1"))
        results["neoq_db"] = {"status": "success", "message": "Connected to neoq Database"}
    except Exception as e:
        results["neoq_db"] = {"status": "error", "message": str(e)}

    # hos DB
    try:
        hos_db.execute(text("SELECT 1"))
        results["hos_db"] = {"status": "success", "message": "Connected to hos Database"}
    except Exception as e:
        results["hos_db"] = {"status": "error", "message": str(e)}

    # Redis
    try:
        await redis_client.ping()
        results["redis"] = {"status": "success", "message": "Connected to Redis"}
    except Exception as e:
        results["redis"] = {"status": "error", "message": str(e)}

    # overall
    all_ok = all(v["status"] == "success" for v in results.values())
    return {"overall": "ok" if all_ok else "degraded", "services": results}

@app.get("/api/check-network", tags=["Security"])
async def check_network(request: Request):
    await verify_ip(request)

    client_ip = get_client_ip(request)
    return {
        "isInternal": True,
        "client_ip": client_ip
    }