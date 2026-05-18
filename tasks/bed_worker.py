import asyncio
import json
from sqlalchemy import text
from database_hos import SessionLocal as SessionHOS
from services.bed_service import summarize_beds
from cache_manager import redis_client

# กำหนดชื่อ Key ใน Redis สำหรับเก็บข้อมูลเตียงโดยเฉพาะ
KEY_BED_CACHE = "beds_summary_cache"
KEY_BED_CONFIG = "beds_config_fixed_wards"

DEFAULT_CONFIG = {
    "wards": {
        "ผู้ป่วยอายุรกรรมหญิง": 30,
        "ผู้ป่วยพิเศษอาคารอ่าวอุดม ชั้น 4": 12,
        "ER Observ": 8,
        "ODS ward": 6,
        "หน่วยไตเทียม": 8,
        "มินิธัญญารักษ์": 6,
        "หอผู้ป่วยวิกฤตทารกแรกเกิด": 3,
        "หลังคลอด": 16,
        "ผู้ป่วยศัลยชาย": 22,
        "ผู้ป่วยศัลยหญิง": 20,
        "ผู้ป่วยอายุรกรรมชาย": 30,
        "ผู้ป่วยเด็ก": 20,
        "หอผู้ป่วย ICU": 10,
        "ห้องคลอด": 6,
    },
    "allowed_wards": [
        "หลังคลอด",
        "ผู้ป่วยเด็ก",
        "ผู้ป่วยศัลยชาย",
        "ผู้ป่วยศัลยหญิง",
        "ผู้ป่วยอายุรกรรมชาย",
        "ผู้ป่วยอายุรกรรมหญิง",
        "ผู้ป่วยพิเศษอาคารอ่าวอุดม ชั้น 4",
        "มินิธัญญารักษ์"
    ],
    "total_beds": 150
}

def fetch_beds_sync(config_data=None):
    try:
        with SessionHOS() as db:
            # เพิ่ม b.bedno AS bed_name เพื่อให้ Service รู้ว่าเตียงนี้เลขอะไร
            sql = text("""
                SELECT 
                    w.name AS ward,
                    r.name AS room,
                    b.bedno AS bed_name,
                    b.bed_status_type_id AS bed_status_type_id
                FROM bedno b
                LEFT JOIN roomno r ON r.roomno = b.roomno
                LEFT JOIN ward w ON w.ward = r.ward
            """)
            result = db.execute(sql).fetchall()
            rows = [dict(row._mapping) for row in result]
            return summarize_beds(rows, config_data)
            
    except Exception as e:
        print(f"[Bed Worker] Error fetching from DB: {e}")
        return None

async def task_update_beds():
    print("[Task Beds] เริ่มทำงาน (อัปเดตทุก 1 นาที)...")
    while True:
        try:
            # ดึง Config จาก Redis
            raw_config = await redis_client.get(KEY_BED_CONFIG)
            if raw_config:
                parsed = json.loads(raw_config)
                if "wards" not in parsed:
                    config_data = {
                        "wards": parsed,
                        "allowed_wards": DEFAULT_CONFIG["allowed_wards"],
                        "total_beds": DEFAULT_CONFIG["total_beds"]
                    }
                else:
                    config_data = parsed
            else:
                config_data = DEFAULT_CONFIG

            bed_data = await asyncio.to_thread(fetch_beds_sync, config_data)
            if bed_data:
                # บันทึกข้อมูลลง Redis
                await redis_client.set(KEY_BED_CACHE, json.dumps(bed_data, ensure_ascii=False))
        except Exception as e:
            print(f"[Task Beds] Loop Error: {e}")
        
        # หน่วงเวลา 60 วินาที (1 นาที)
        await asyncio.sleep(60)