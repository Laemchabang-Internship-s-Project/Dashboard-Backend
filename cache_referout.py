"""
cache_referout.py
=================
โมดูลจัดการ Cache ข้อมูลการส่งต่อผู้ป่วย (Refer Out) แยกตามความรุนแรง (Emergency Type)

ตารางที่อ้างอิง:
  - referout : ตารางบันทึกการส่งต่อผู้ป่วยนอก/ใน
"""

import asyncio
import json
import os
from datetime import datetime

import redis.asyncio as redis
from sqlalchemy import text
from database_hos import SessionLocal as SessionHOS

# ==========================================================
# Redis Connection
# ==========================================================
redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    password=os.getenv("REDIS_PASSWORD"),
    db=0,
    decode_responses=True,
)

# Redis Keys
KEY_REFEROUT_TODAY = "referout_today_count"


# ==========================================================
# Sync Functions: ดึงข้อมูลจากฐานข้อมูล HOSxP
# ==========================================================
def fetch_referout_today_sync() -> dict:
    """ดึงยอดรวมข้อมูลการ Refer Out ประจำวันปัจจุบัน"""
    default_data = {
        "life_threatening_count": 0,
        "emergency_count": 0,
        "urgent_count": 0,
        "acute_count": 0,
        "non_acute_count": 0,
        "unknown_count": 0,
        "total_all_cases": 0,
    }
    try:
        with SessionHOS() as db_hos:
            sql = text(
                """
                SELECT 
                    COUNT(CASE WHEN referout_emergency_type_id = 1 THEN 1 END) AS life_threatening_count,
                    COUNT(CASE WHEN referout_emergency_type_id = 2 THEN 1 END) AS emergency_count,
                    COUNT(CASE WHEN referout_emergency_type_id = 3 THEN 1 END) AS urgent_count,
                    COUNT(CASE WHEN referout_emergency_type_id = 4 THEN 1 END) AS acute_count,
                    COUNT(CASE WHEN referout_emergency_type_id = 5 THEN 1 END) AS non_acute_count,
                    COUNT(CASE WHEN referout_emergency_type_id IS NULL OR referout_emergency_type_id NOT IN (1,2,3,4,5) THEN 1 END) AS unknown_count,
                    COUNT(*) AS total_all_cases
                FROM referout
                WHERE vstdate = CURDATE();
            """
            )
            row = db_hos.execute(sql).fetchone()
            if row:
                return {
                    "life_threatening_count": int(row[0] or 0),
                    "emergency_count": int(row[1] or 0),
                    "urgent_count": int(row[2] or 0),
                    "acute_count": int(row[3] or 0),
                    "non_acute_count": int(row[4] or 0),
                    "unknown_count": int(row[5] or 0),
                    "total_all_cases": int(row[6] or 0),
                }
    except Exception as e:
        print(f"[Cache Referout] Today Query Error: {e}")
    return default_data


def fetch_referout_range_sync(start_date: str, end_date: str) -> dict:
    """ดึงยอดรวมข้อมูลการ Refer Out แบบกำหนดช่วงวัน (ดึงตรงจาก DB ไม่ผ่าน Cache วนลูป)"""
    default_data = {
        "life_threatening_count": 0,
        "emergency_count": 0,
        "urgent_count": 0,
        "acute_count": 0,
        "non_acute_count": 0,
        "unknown_count": 0,
        "total_all_cases": 0,
    }
    try:
        with SessionHOS() as db_hos:
            sql = text(
                """
                SELECT
                    COUNT(CASE WHEN referout_emergency_type_id = 1 THEN 1 END) AS life_threatening_count,
                    COUNT(CASE WHEN referout_emergency_type_id = 2 THEN 1 END) AS emergency_count,
                    COUNT(CASE WHEN referout_emergency_type_id = 3 THEN 1 END) AS urgent_count,
                    COUNT(CASE WHEN referout_emergency_type_id = 4 THEN 1 END) AS acute_count,
                    COUNT(CASE WHEN referout_emergency_type_id = 5 THEN 1 END) AS non_acute_count,
                    COUNT(CASE WHEN referout_emergency_type_id IS NULL OR referout_emergency_type_id NOT IN (1,2,3,4,5) THEN 1 END) AS unknown_count,
                    COUNT(*) AS total_all_cases
                FROM referout
                WHERE vstdate BETWEEN :start AND :end;
            """
            )
            row = db_hos.execute(sql, {"start": start_date, "end": end_date}).fetchone()
            if row:
                return {
                    "life_threatening_count": int(row[0] or 0),
                    "emergency_count": int(row[1] or 0),
                    "urgent_count": int(row[2] or 0),
                    "acute_count": int(row[3] or 0),
                    "non_acute_count": int(row[4] or 0),
                    "unknown_count": int(row[5] or 0),
                    "total_all_cases": int(row[6] or 0),
                }
    except Exception as e:
        print(f"[Cache Referout] Range Query Error: {e}")
    return default_data


# ==========================================================
# Background Task & Getters
# ==========================================================
async def task_update_referout():
    """Background Worker สำหรับคอยรีเฟรชข้อมูลของวันนี้ทุกๆ 5 นาทีเพื่อความ Real-time"""
    print("[Task Referout] เริ่มทำงาน (อัปเดตทุก 5 นาที)...")
    from cache_manager import patch_redis_cache

    while True:
        try:
            today_data = await asyncio.to_thread(fetch_referout_today_sync)
            if today_data:
                # 1. บันทึกลง Key แยกของ Referout
                await redis_client.set(
                    KEY_REFEROUT_TODAY, json.dumps(today_data, ensure_ascii=False)
                )
                # 2. ปล่อยสัญญาณ Patch เข้า Dashboard cache เผื่อใช้งานระบบ SSE Stream ร่วมด้วย
                await patch_redis_cache({"referout_today": today_data})

                print(
                    f"[Task Referout] อัปเดตสำเร็จ ยอดรวมวันนี้: {today_data['total_all_cases']} เคส"
                )
        except Exception as e:
            print(f"[Task Referout] Loop Error: {e}")

        await asyncio.sleep(300)  # ทำงานในทุกๆ 5 นาที


async def get_referout_data(
    view: str = "today", start_date: str = None, end_date: str = None
) -> dict:
    """ฟังก์ชัน Getter สำหรับให้ Router เรียกอ่านข้อมูล"""
    if view == "range" and start_date and end_date:
        # ถ้าดึงช่วงวันที่ ให้ยิงดึงตรงจากฐานข้อมูลผ่านกระทู้ Thread ป้องกันการบล็อก Event Loop
        return await asyncio.to_thread(fetch_referout_range_sync, start_date, end_date)

    # มุมมองดีฟอลต์ (today): ดึงจาก Redis Cache ทันที
    raw = await redis_client.get(KEY_REFEROUT_TODAY)
    if raw:
        return json.loads(raw)

    # กรณี Cache ว่างเปล่า ให้ Fetch สดส่งกลับไป
    return await asyncio.to_thread(fetch_referout_today_sync)
