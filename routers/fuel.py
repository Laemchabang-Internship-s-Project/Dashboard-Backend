"""
routers/fuel.py
POST /api/fuel/webhook  — รับข้อมูลตรวจเช็ครถจาก Google Apps Script แล้วอัป Redis ทันที
GET  /api/fuel/latest   — ดูค่าล่าสุดจาก Redis
"""

from fastapi import APIRouter, HTTPException, Header, Request
from pydantic import BaseModel
from typing import Optional
import os

from cache_manager import update_fuel_cache, redis_client, KEY_FUEL_CACHE, KEY_FUEL_HISTORY
from rate_limiter import limiter
import json

router = APIRouter(prefix="/api/fuel", tags=["Fuel"])

FUEL_WEBHOOK_SECRET = os.getenv("FUEL_WEBHOOK_SECRET", "")


# ----------------------------------------------------------
# Schema ตามคอลัมน์ใน Google Form
# ----------------------------------------------------------
class FuelPayload(BaseModel):
    timestamp:       str            # Timestamp จาก Google Form
    date:            str            # วัน
    machine:         str            # เครื่อง
    fuel_level_be4:  float          # ระดับน้ำมันก่อน
    fuel_level_aft:  float          # ระดับน้ำมันหลัง
    battery_pole:    str            # สภาพขั้วแบตเตอรี่
    battery_water:   str            # ระดับน้ำกลั่นแบตเตอรี่
    radiator_water:  str            # ระดับน้ำในหม้อน้ำ
    engine_oil:      str            # ระดับน้ำมันเครื่อง
    control_light:   str            # สัญญาณไฟควบคุม
    tech_name:       str            # ชื่อช่าง
    status:          str            # สถานะ
    app_name:        Optional[str] = ""  # ชื่อผู้อนุมัติ


# ----------------------------------------------------------
# POST /api/fuel/webhook
# ----------------------------------------------------------
@router.post("/webhook")
@limiter.limit("10/minute")
async def fuel_webhook(
    request: Request,
    payload: FuelPayload,
    x_webhook_secret: str = Header(default="", alias="X-Webhook-Secret"),
):
    if FUEL_WEBHOOK_SECRET and x_webhook_secret != FUEL_WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid webhook secret")

    fuel_data = payload.model_dump()
    success = await update_fuel_cache(fuel_data)

    if not success:
        raise HTTPException(status_code=500, detail="Failed to update Redis cache")

    return {
        "status": "ok",
        "message": "Fuel data updated and broadcasted",
        "data": fuel_data,
    }


# ----------------------------------------------------------
# GET /api/fuel/latest
# ----------------------------------------------------------
@router.get("/latest")
@limiter.limit("30/minute")
async def get_latest_fuel(request: Request):
    """ดึงข้อมูลตรวจเช็ครถล่าสุดจาก Redis"""
    raw = await redis_client.get(KEY_FUEL_CACHE)
    if not raw:
        raise HTTPException(status_code=404, detail="ยังไม่มีข้อมูล")
    return json.loads(raw)


# ----------------------------------------------------------
# GET /api/fuel/history?limit=100
# ----------------------------------------------------------
@router.get("/history")
@limiter.limit("30/minute")
async def get_fuel_history(request: Request, limit: int = 100):
    """
    ดึงประวัติการตรวจเช็ครถล่าสุดจาก Redis List
    - limit: จำนวนรายการที่ต้องการ (สูงสุด 100, default 100)
    - เรียงจากใหม่ → เก่า (index 0 = ล่าสุด)
    """
    limit = min(limit, 100)  # cap ไว้ที่ 100
    items = await redis_client.lrange(KEY_FUEL_HISTORY, 0, limit - 1)
    return {
        "total": len(items),
        "records": [json.loads(item) for item in items],
    }


# ----------------------------------------------------------
# POST /api/fuel/backfill
# โหลดข้อมูลเก่าจาก Sheet เข้า Redis List ครั้งเดียว
# ----------------------------------------------------------
class BackfillPayload(BaseModel):
    records: list[dict]   # array ของ record ทุก row จาก Sheet


@router.post("/backfill")
@limiter.limit("5/minute")
async def fuel_backfill(
    request: Request,
    payload: BackfillPayload,
    x_webhook_secret: str = Header(default="", alias="X-Webhook-Secret"),
):
    """
    รับ records หลายรายการพร้อมกัน แล้ว push เข้า Redis List
    ใช้รันครั้งเดียวเพื่อโหลดประวัติเก่าจาก Google Sheet
    records ควรเรียงจากเก่า → ใหม่ (เพื่อให้ index 0 = ล่าสุด)
    """
    from cache_manager import FUEL_HISTORY_MAX

    if FUEL_WEBHOOK_SECRET and x_webhook_secret != FUEL_WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    if not payload.records:
        raise HTTPException(status_code=400, detail="ไม่มีข้อมูล")

    # ✅ ล้าง List เก่าก่อนเสมอ — ป้องกัน duplicate ถ้ารันซ้ำ
    await redis_client.delete(KEY_FUEL_HISTORY)

    # Push ทีละ record ลำดับจาก Sheet คือเก่า -> ใหม่
    # ใช้ lpush เข้าไปเรื่อยๆ ท้ายสุดข้อมูลใหม่สุดจะถูกดันไปอยู่หน้าสุด (index 0)
    for r in payload.records:
        await redis_client.lpush(KEY_FUEL_HISTORY, json.dumps(r, ensure_ascii=False))


    # Trim เก็บไว้ไม่เกิน FUEL_HISTORY_MAX
    await redis_client.ltrim(KEY_FUEL_HISTORY, 0, FUEL_HISTORY_MAX - 1)

    # อัป latest = record ล่าสุด (index 0 หลัง push) ลง KEY_FUEL_CACHE
    # ไม่เรียก update_fuel_cache เพื่อป้องกันการถูกดัน(lpush) ซ้ำอีก 1 รอบ
    latest_raw = await redis_client.lindex(KEY_FUEL_HISTORY, 0)
    if latest_raw:
        from cache_manager import KEY_FUEL_CACHE
        await redis_client.set(KEY_FUEL_CACHE, latest_raw)

    total = await redis_client.llen(KEY_FUEL_HISTORY)
    return {
        "status": "ok",
        "pushed": len(payload.records),
        "total_in_redis": total,
    }
