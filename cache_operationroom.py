"""
cache_operationRoom.py
======================
โมดูลจัดการ Cache ข้อมูลห้องผ่าตัดและการใช้งานแบบ Real-time สำหรับ Dashboard

ตารางที่อ้างอิง:
  - operation_room : ตารางหลักเก็บรายชื่อห้องผ่าตัด (room_id, room_name)
  - operation_list : ตารางเก็บรายการผ่าตัดและเวลาเข้าออก (room_id, operation_date, enter_date, enter_time, leave_date, leave_time)
"""

import asyncio
import json
import os
import redis.asyncio as redis
from sqlalchemy import text


from database_hos import SessionLocal as SessionHOS

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    password=os.getenv("REDIS_PASSWORD"),
    db=0,
    decode_responses=True,
)

# Key สำหรับเก็บผลลัพธ์เฉพาะของระบบห้องผ่าตัด
KEY_OPERATION_ROOM_CACHE = "operation_rooms_summary"


def fetch_operation_rooms_sync() -> list:
    """
    ดึงข้อมูลยอดรวมและสถานะ Real-time ของห้องผ่าตัดทั้งหมดจาก HOSxP
    แก้ไข Logic ให้รวมเป็น 1 Query เพื่อประหยัดทรัพยากรและแม่นยำขึ้น
    """
    try:
        with SessionHOS() as db_hos:
            sql = text(
                """
                SELECT
                    r.room_name,
                    
                    -- 1. ยอดรวมเคสทั้งหมดของวันนี้ (นับตามวันที่เริ่ม operation)
                    COUNT(DISTINCT CASE WHEN l.operation_date = CURDATE() THEN l.operation_id END) AS total_cases_today,
                    
                    -- 2. วิเคราะห์สถานะแบบ Real-time: มีเคสกำลังคาอยู่ในห้อง ณ วินาทีนี้หรือไม่
                    IF(
                        SUM(
                            CASE WHEN 
                                l.operation_date = CURDATE()
                                AND l.enter_time IS NOT NULL 
                                AND NOW() >= STR_TO_DATE(CONCAT(l.enter_date, ' ', l.enter_time), '%Y-%m-%d %H:%i:%s')
                                AND (
                                    l.leave_time IS NULL 
                                    OR l.leave_time = '00:00:00' 
                                    OR NOW() <= STR_TO_DATE(CONCAT(l.leave_date, ' ', l.leave_time), '%Y-%m-%d %H:%i:%s')
                                )
                            THEN 1 ELSE 0 END
                        ) > 0,
                        'กำลังใช้งาน',
                        'ว่าง'
                    ) AS room_status
                    
                FROM operation_room r
                LEFT JOIN operation_list l 
                    ON r.room_id = l.room_id 
                    AND l.operation_date = CURDATE()
                GROUP BY r.room_id, r.room_name
                ORDER BY r.room_name ASC;
            """
            )

            rows = db_hos.execute(sql).fetchall()

            result_list = []
            for r in rows:
                if r[0] is None:
                    continue
                result_list.append(
                    {
                        "room_name": r[0],
                        "total_cases_today": int(r[1] or 0),
                        "room_status": r[2] or "ว่าง",
                    }
                )

            return result_list

    except Exception as e:
        print(f"[Cache OperationRoom] HOSxP Query Error: {e}")
        return []


async def task_update_operation_rooms():
    """
    Background Task ดึงข้อมูลห้องผ่าตัดมาพักใน Redis และกระจายสัญญาณ Pub/Sub
    """
    print("[Task OperationRoom] เริ่มทำงาน (อัปเดตทุก 1 นาที)...")

    from cache_manager import patch_redis_cache

    while True:
        try:

            op_data = await asyncio.to_thread(fetch_operation_rooms_sync)

            if op_data:

                await redis_client.set(
                    KEY_OPERATION_ROOM_CACHE, json.dumps(op_data, ensure_ascii=False)
                )

                await patch_redis_cache({"operation_rooms": op_data})

                active_count = sum(
                    1 for room in op_data if room["room_status"] == "กำลังใช้งาน"
                )
                print(
                    f"[Task OperationRoom] อัปเดตสำเร็จ: ทั้งหมด {len(op_data)} ห้อง (กำลังใช้งาน {active_count} ห้อง)"
                )

        except Exception as e:
            print(f"[Task OperationRoom] Loop Error: {e}")

        await asyncio.sleep(60)
