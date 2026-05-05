"""
routers/fuel.py
POST /api/fuel/webhook  — รับข้อมูลตรวจเช็ครถจาก Google Apps Script แล้วอัป Redis ทันที
GET  /api/fuel/latest   — ดูค่าล่าสุดจาก Redis
"""

from fastapi import APIRouter, HTTPException, Header, Depends, Request
from pydantic import BaseModel
from typing import Optional
import os

from cache_manager import update_fuel_cache, redis_client, KEY_FUEL_CACHE, KEY_FUEL_HISTORY
from rate_limiter import limiter
import json
from utils.security import get_api_key
from sqlalchemy.orm import Session
from database_analytics import get_analytics_db
from models_analytics import FuelRecord

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
    db: Session = Depends(get_analytics_db)
):
    if FUEL_WEBHOOK_SECRET and x_webhook_secret != FUEL_WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid webhook secret")

    fuel_data = payload.model_dump()
    
    # 1. บันทึกลง Database
    try:
        new_record = FuelRecord(**fuel_data)
        db.add(new_record)
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    # 2. อัปเดตลง Redis Cache
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
@router.get("/latest" , dependencies=[Depends(get_api_key)] )
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
@router.get("/history", dependencies=[Depends(get_api_key)])
@limiter.limit("30/minute")
async def get_fuel_history(request: Request, limit: int = 100, offset: int = 0, db: Session = Depends(get_analytics_db)):
    """
    ดึงประวัติการตรวจเช็ครถล่าสุด
    - ถ้า offset = 0 ดึงจาก Redis List ทันที (เร็วมาก)
    - ถ้า offset > 0 ดึงจาก PostgreSQL เพื่อโหลดประวัติย้อนหลัง
    """
    limit = min(limit, 100)  # cap ไว้ที่ 100

    if offset == 0:
        # ดึง 100 อันดับแรกจาก Redis
        items = await redis_client.lrange(KEY_FUEL_HISTORY, 0, limit - 1)
        if items:
            return {
                "total": len(items),
                "source": "redis",
                "records": [json.loads(item) for item in items],
            }

    # ดึงประวัติเพิ่มเติมจาก Database
    try:
        records = db.query(FuelRecord).order_by(FuelRecord.id.desc()).offset(offset).limit(limit).all()
        result_list = []
        for r in records:
            r_dict = r.__dict__.copy()
            r_dict.pop('_sa_instance_state', None)
            result_list.append(r_dict)
            
        return {
            "total": len(result_list),
            "source": "database",
            "records": result_list,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")


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
    db: Session = Depends(get_analytics_db)
):
    """
    รับ records หลายรายการพร้อมกัน เอาเข้า Database และ Redis
    ใช้รันครั้งเดียวเพื่อโหลดประวัติเก่าจาก Google Sheet
    """
    from cache_manager import FUEL_HISTORY_MAX

    if FUEL_WEBHOOK_SECRET and x_webhook_secret != FUEL_WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    if not payload.records:
        raise HTTPException(status_code=400, detail="ไม่มีข้อมูล")

    # 1. เอาข้อมูลลง Database
    try:
        # ตรวจสอบเพื่อไม่ให้ข้อมูลซ้ำแบบมักง่าย คือลบข้อมูลเก่าใน db ให้หมดก่อนแบ็คฟิล
        db.query(FuelRecord).delete()
        
        # เพิ่มข้อมูลทั้งหมด
        db_records = [FuelRecord(**r) for r in payload.records]
        db.add_all(db_records)
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    # 2. เอาข้อมูลลง Redis 100 รายการล่าสุด
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
        "pushed_to_db": len(payload.records),
        "total_in_redis": total_redis,
    }
