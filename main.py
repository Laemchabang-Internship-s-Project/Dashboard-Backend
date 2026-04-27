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
from routers import auth as auth_router
from routers import graph as graph_router

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
from tasks.bed_worker import task_update_beds
from routers import bed as bed_router
from routers import fuel
from fastapi.openapi.utils import get_openapi
from fastapi.security import APIKeyHeader

# --- Rate Limiter ---
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from rate_limiter import limiter

# --- Filter Router ---
from routers import filter as filter_router


load_dotenv()


# ==========================================================
# Lifespan: จัดการ Startup / Shutdown ของ App
# ==========================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- Startup ---
    task = asyncio.create_task(update_redis_cache())
    bed_task = asyncio.create_task(task_update_beds())
    print("[Main] Cache Worker Task เริ่มทำงานแล้ว")
    yield
    # --- Shutdown ---
    task.cancel()
    bed_task.cancel()
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
        "http://localhost:5174"
        ],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["x-api-key", "Content-Type", "X-Forwarded-For", "Authorization"],
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

app.include_router(filter_router.router ,tags = ["Filter Data"])

# ==========================================================
# Router Registration
# ==========================================================
app.include_router(dashboard.router)
# Fuel Webhook (รับ trigger จาก Google Apps Script)
app.include_router(fuel.router)
# Auth Router
app.include_router(auth_router.router)
# Graph Router
app.include_router(graph_router.router)
app.include_router(bed_router.router)

# ==========================================================
# System Endpoints
# ==========================================================
@app.get("/api/system/health", tags=["System"], dependencies=[Depends(get_api_key)])
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

