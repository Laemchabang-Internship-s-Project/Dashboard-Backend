"""
routers/fuel.py
POST /api/fuel/webhook  — รับข้อมูลตรวจเช็ครถจาก Google Apps Script แล้วอัป Redis ทันที
GET  /api/fuel/latest   — ดูค่าล่าสุดจาก Redis
GET  /api/fuel/history  — ดูประวัติจาก Redis
"""

from fastapi import APIRouter, HTTPException, Header, Depends, Request
from pydantic import BaseModel
from typing import Optional
import os

from cache_manager import update_fuel_cache, redis_client, KEY_FUEL_CACHE, KEY_FUEL_HISTORY
from rate_limiter import limiter
import json
from utils.security import get_api_key

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

class FuelUpdatePayload(BaseModel):
    timestamp: str       # Timestamp ที่ต้องการอัปเดต
    status: str          # สถานะใหม่
    app_name: str        # ชื่อผู้อนุมัติ

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

    # อัปเดตลง Redis Cache
    success = await update_fuel_cache(fuel_data)

    if not success:
        raise HTTPException(status_code=500, detail="Failed to update Redis cache")

    return {
        "status": "ok",
        "message": "Fuel data updated and broadcasted",
        "data": fuel_data,
    }


# ----------------------------------------------------------
# POST /api/fuel/webhook/update
# ----------------------------------------------------------
@router.post("/webhook/update")
@limiter.limit("20/minute")
async def fuel_webhook_update(
    request: Request,
    payload: FuelUpdatePayload,
    x_webhook_secret: str = Header(default="", alias="X-Webhook-Secret"),
):
    if FUEL_WEBHOOK_SECRET and x_webhook_secret != FUEL_WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    from cache_manager import patch_redis_cache

    # normalize timestamp ก่อนเปรียบเทียบ ป้องกัน space/format drift
    def normalize_ts(ts: str) -> str:
        return ts.strip().replace("  ", " ")

    target_ts = normalize_ts(payload.timestamp)

    # อัปเดตใน Redis History List
    items = await redis_client.lrange(KEY_FUEL_HISTORY, 0, -1)
    updated_data = None

    for i, item_str in enumerate(items):
        item_data = json.loads(item_str)
        if normalize_ts(item_data.get("timestamp", "")) == target_ts:
            item_data["status"] = payload.status
            item_data["app_name"] = payload.app_name
            updated_data = item_data

            # อัปเดตกลับไปที่ List เดิม
            await redis_client.lset(KEY_FUEL_HISTORY, i, json.dumps(item_data, ensure_ascii=False))

            # อัปเดต KEY_FUEL_CACHE เสมอ (ไม่ใช่แค่ index 0)
            # เพราะอาจมีรายการใหม่บันทึกมาแทรกก่อนหน้า
            await redis_client.set(KEY_FUEL_CACHE, json.dumps(item_data, ensure_ascii=False))
            await patch_redis_cache({"car": {"fuel_latest": item_data}})
            break

    if not updated_data:
        # Log ชัดเจนเพื่อ debug ง่าย
        print(f"[Fuel Update]  ไม่เจอ timestamp: '{target_ts}' ใน {len(items)} รายการใน Redis")
        raise HTTPException(
            status_code=404,
            detail=f"Record not found in Redis history (timestamp='{target_ts}')"
        )

    print(f"[Fuel Update]  อัปเดตสำเร็จ: '{target_ts}' → {payload.status}")
    return {
        "status": "ok",
        "message": "Fuel status updated",
        "data": updated_data,
    }


# ----------------------------------------------------------
# GET /api/fuel/latest
# ----------------------------------------------------------
@router.get("/latest", dependencies=[Depends(get_api_key)])
@limiter.limit("30/minute")
async def get_latest_fuel(request: Request):
    """ดึงข้อมูลตรวจเช็ครถล่าสุดจาก Redis"""
    raw = await redis_client.get(KEY_FUEL_CACHE)
    if not raw:
        raise HTTPException(status_code=404, detail="ยังไม่มีข้อมูล")
    return json.loads(raw)


# ----------------------------------------------------------
# GET /api/fuel/history?limit=100&offset=0
# ----------------------------------------------------------
@router.get("/history", dependencies=[Depends(get_api_key)])
@limiter.limit("30/minute")
async def get_fuel_history(request: Request, limit: int = 100, offset: int = 0):
    """
    ดึงประวัติการตรวจเช็ครถจาก Redis List
    - limit: จำนวนรายการ (สูงสุด 100)
    - offset: เริ่มต้นจาก index ที่เท่าไหร่
    """
    limit = min(limit, 100)  # cap ไว้ที่ 100

    items = await redis_client.lrange(KEY_FUEL_HISTORY, offset, offset + limit - 1)
    if not items:
        return {
            "total": 0,
            "source": "redis",
            "records": [],
        }

    return {
        "total": len(items),
        "source": "redis",
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
    รับ records หลายรายการพร้อมกัน เอาเข้า Redis
    ใช้รันครั้งเดียวเพื่อโหลดประวัติเก่าจาก Google Sheet
    """
    from cache_manager import FUEL_HISTORY_MAX

    if FUEL_WEBHOOK_SECRET and x_webhook_secret != FUEL_WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    if not payload.records:
        raise HTTPException(status_code=400, detail="ไม่มีข้อมูล")

    # เคลียร์ Redis History แล้วใส่ข้อมูลใหม่ทั้งหมด
    await redis_client.delete(KEY_FUEL_HISTORY)

    # records ควรเรียงจากเก่า -> ใหม่ เมื่อใช้ lpush อันใหม่สุดจะอยู่หน้า
    for r in payload.records:
        await redis_client.lpush(KEY_FUEL_HISTORY, json.dumps(r, ensure_ascii=False))

    await redis_client.ltrim(KEY_FUEL_HISTORY, 0, FUEL_HISTORY_MAX - 1)

    # อัป latest = record ล่าสุด
    latest_raw = await redis_client.lindex(KEY_FUEL_HISTORY, 0)
    if latest_raw:
        from cache_manager import KEY_FUEL_CACHE
        await redis_client.set(KEY_FUEL_CACHE, latest_raw)

    total_redis = await redis_client.llen(KEY_FUEL_HISTORY)
    return {
        "status": "ok",
        "pushed_to_redis": len(payload.records),
        "total_in_redis": total_redis,
    }
