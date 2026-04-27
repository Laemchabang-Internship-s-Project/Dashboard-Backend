import asyncio
import json
from sqlalchemy import text
from database_hos import SessionLocal as SessionHOS
from services.bed_service import summarize_beds
from cache_manager import redis_client

# กำหนดชื่อ Key ใน Redis สำหรับเก็บข้อมูลเตียงโดยเฉพาะ
KEY_BED_CACHE = "beds_summary_cache"

def fetch_beds_sync():
    try:
        with SessionHOS() as db:
            # ใช้ SQL ของคุณในการดึงข้อมูล โดย Mapping ชื่อคอลัมน์ให้เข้ากับ Service
            sql = text("""
                SELECT 
                    w.name AS ward,           -- ชื่อวอร์ด
                    r.name AS room,           -- ชื่อห้อง
                    b.bed_status_type_id AS bed_status_type_id
                FROM bedno b
                LEFT JOIN roomno r ON r.roomno = b.roomno
                LEFT JOIN ward w ON w.ward = r.ward
            """)
            
            result = db.execute(sql).fetchall()
            
            # แปลง SQLAlchemy Row เป็น List of Dict
            rows = [dict(row._mapping) for row in result]
            
            # ส่งไปประมวลผลที่ Service (ซึ่งรองรับค่า NULL เป็น 'other' แล้ว)
            return summarize_beds(rows)
            
    except Exception as e:
        print(f"[Bed Worker] Error fetching from DB: {e}")
        return None

async def task_update_beds():
    print("[Task Beds] เริ่มทำงาน (อัปเดตทุก 1 นาที)...")
    while True:
        try:
            bed_data = await asyncio.to_thread(fetch_beds_sync)
            if bed_data:
                # บันทึกข้อมูลลง Redis
                await redis_client.set(KEY_BED_CACHE, json.dumps(bed_data, ensure_ascii=False))
        except Exception as e:
            print(f"[Task Beds] Loop Error: {e}")
        
        # หน่วงเวลา 60 วินาที (1 นาที)
        await asyncio.sleep(10)