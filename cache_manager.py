import asyncio
import json
import redis
import os
import requests
from sqlalchemy import text
from database import SessionLocal

# ==========================================================
# Redis Connection
# ==========================================================
redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=0,
    decode_responses=True
)

CHANNEL_DASHBOARD = "dashboard_events"
KEY_DASHBOARD_CACHE = "dashboard_full_cache"
SHEET_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vROk5cTxHtrHUXSuS7dSAQ3kdRgj7GcHs-c1YdWXsFQ52ZURrCugyRNdjyT0Qrzxj91oUQxdpRl69e2/pub?output=csv"


# ==========================================================
# Helper: ดึงข้อมูลจาก Redis Cache
# ==========================================================
def get_cached_data(section: str = None) -> dict | None:
    raw = redis_client.get(KEY_DASHBOARD_CACHE)
    if not raw:
        return None
    data = json.loads(raw)
    if section:
        return data.get(section)
    return data


# ==========================================================
# Fuel Data
# ==========================================================
async def fetch_fuel_data():
    try:
        res = await asyncio.to_thread(requests.get, SHEET_URL, timeout=10)
        res.encoding = "utf-8"
        lines = res.text.strip().split("\n")
        if len(lines) > 1:
            last = lines[-1].split(",")
            return {
                "date":       last[0].strip('"'),
                "time":       last[1].strip('"'),
                "shift":      last[2].strip('"'),
                "type":       last[3].strip('"'),
                "fuel_level": float(last[4].strip('"')),
                "mileage":    float(last[5].strip('"')),
            }
    except Exception as e:
        print(f"[Cache Worker] Fuel Error: {e}")
    return None


# ==========================================================
# Master Data
# ==========================================================

# ห้องที่ใช้นับยอด OPD Total (header)
OPD_TOTAL_ROOMS = (
    '010',  # จุดซักประวัติผู้ป่วยนอก
    '062',  # จุดซักประวัติผู้ป่วยนอก (นัด)
    '005',  # ห้องทันตกรรม
    '041',  # แพทย์แผนไทย
    '042',  # กายภาพ
    '109',  # จุดซักประวัติผู้ป่วย (สูติกรรม)
    '110',  # จุดซักประวัติผู้ป่วย (ศัลยกรรม)
    '111',  # จุดซักประวัติผู้ป่วย (อายุรกรรม)
    '001',  # ความดัน
    '002',  # เบาหวาน
)

# ห้องที่แสดงในตาราง
MASTER_ROOMS = [
    {"code": "010", "name": "จุดซักประวัติผู้ป่วยนอก"},
    {"code": "062", "name": "จุดซักประวัติผู้ป่วยนอก (นัด)"},
    {"code": "023", "name": "จุดรอตรวจ"},
    {"code": "014", "name": "ห้องหลังพบแพทย์"},
    {"code": "110", "name": "จุดซักประวัติผู้ป่วย (ศัลยกรรม)"},
    {"code": "109", "name": "จุดซักประวัติผู้ป่วย (สูติกรรม)"},
    {"code": "111", "name": "จุดซักประวัติผู้ป่วย (อายุรกรรม)"},
    {"code": "005", "name": "ห้องทันตกรรม"},
    {"code": "041", "name": "แพทย์แผนไทย"},
    {"code": "042", "name": "กายภาพ"},
    {"code": "082", "name": "จุดคัดกรอง OPD"},
    {"code": "113", "name": "ห้องตรวจโรคผิวหนัง"},
    {"code": "134", "name": "คลินิกตรวจอัลตราซาวด์"},
]


# ==========================================================
# Background Worker
# ==========================================================
async def update_redis_cache():
    print("[Cache Worker] เริ่มทำงาน... (ดึงข้อมูลทุก 5 วินาที)")
    while True:
        db = SessionLocal()
        try:
            # ==========================================
            # 1. ยอด OPD Total (เฉพาะห้องที่กำหนด)
            # ==========================================
            sys_sql = text("""
                SELECT
                    COUNT(DISTINCT hn) AS opd_total,
                    COUNT(DISTINCT CASE WHEN room_code = '062' THEN hn END) AS appointment,
                    COUNT(DISTINCT CASE WHEN room_code != '062' THEN hn END) AS walk_in
                FROM opd_queue
                WHERE date = CURDATE()
                AND room_code IN :rooms
            """)
            sys_res = db.execute(sys_sql, {"rooms": OPD_TOTAL_ROOMS}).fetchone()
            total_services = int(sys_res[0]) if sys_res else 0

            # ==========================================
            # 2. ข้อมูลห้องตรวจ OPD (ตาราง)
            # ==========================================
            target_codes = tuple(room["code"] for room in MASTER_ROOMS)

            opd_rooms_sql = text("""
                SELECT 
                    q.room_code,
                    SUM(CASE WHEN q.room_code = '062' THEN 1 ELSE 0 END) AS appointment,
                    SUM(CASE WHEN q.room_code != '062' THEN 1 ELSE 0 END) AS walk_in,
                    COUNT(*) AS total,
                    SUM(CASE WHEN q.status_id = '3' THEN 1 ELSE 0 END) AS finished,
                    SUM(CASE WHEN q.status_id != '3' THEN 1 ELSE 0 END) AS waiting
                FROM opd_queue q
                WHERE q.date = CURDATE() AND q.room_code IN :rooms
                GROUP BY q.room_code
            """)
            opd_rooms_res = db.execute(opd_rooms_sql, {"rooms": target_codes}).fetchall()
            db_map = {row[0]: row for row in opd_rooms_res}

            final_rooms = []
            for master in MASTER_ROOMS:
                code = master["code"]
                if code in db_map:
                    row = db_map[code]
                    final_rooms.append({
                        "room_code": code, "room_name": master["name"],
                        "appointment": int(row[1]), "walk_in": int(row[2]),
                        "total": int(row[3]), "finished": int(row[4]), "waiting": int(row[5])
                    })
                else:
                    final_rooms.append({
                        "room_code": code, "room_name": master["name"],
                        "appointment": 0, "walk_in": 0, "total": 0, "finished": 0, "waiting": 0
                    })

            # ==========================================
            # 3. OPD Header (ใช้ยอดจาก sys_res เดิมได้เลย)
            # ==========================================
            opd_header = {
                "opd_total":   int(sys_res[0] or 0),
                "appointment": int(sys_res[1] or 0),
                "walk_in":     int(sys_res[2] or 0),
            }

            # ==========================================
            # 4. แผนกเทคนิค
            # ==========================================
            tech_results = {}

            for dept in ["xray_queue", "lab_queue"]:
                sql = text(f"""
                    SELECT COUNT(*),
                           SUM(CASE WHEN status_id != '3' THEN 1 ELSE 0 END),
                           SUM(CASE WHEN status_id = '3' THEN 1 ELSE 0 END)
                    FROM {dept} WHERE date = CURDATE()
                """)
                res = db.execute(sql).fetchone()
                tech_results[dept] = {
                    "all":      int(res[0] or 0),
                    "waiting":  int(res[1] or 0),
                    "finished": int(res[2] or 0),
                }

            for dept in ["pharmacy_queue", "finance_queue"]:
                sql = text(f"""
                    SELECT COUNT(*),
                           SUM(CASE WHEN status_id = '3' THEN 1 ELSE 0 END),
                           SUM(CASE WHEN status_id != '3' THEN 1 ELSE 0 END)
                    FROM {dept} WHERE date = CURDATE()
                """)
                res = db.execute(sql).fetchone()
                tech_results[dept] = {
                    "all":      int(res[0] or 0),
                    "finished": int(res[1] or 0),
                    "waiting":  int(res[2] or 0),
                }

            # ==========================================
            # 5. น้ำมันรถ
            # ==========================================
            fuel_data = await fetch_fuel_data()

            # ==========================================
            # 6. ประกอบ JSON
            # ==========================================
            dashboard_data = {
                "system": {
                    "today_total_services": total_services,
                },
                "opd_clinics": {
                    "header": opd_header,
                    "rooms": final_rooms,
                },
                "technical_services": {
                    "xray":     tech_results["xray_queue"],
                    "lab":      tech_results["lab_queue"],
                    "pharmacy": tech_results["pharmacy_queue"],
                    "finance":  tech_results["finance_queue"],
                },
                "car": {
                    "fuel_latest": fuel_data,
                },
            }

            json_data = json.dumps(dashboard_data, ensure_ascii=False)

            redis_client.set(KEY_DASHBOARD_CACHE, json_data)
            redis_client.publish(CHANNEL_DASHBOARD, json_data)

        except Exception as e:
            print(f"[Cache Worker] Error: {e}")
        finally:
            db.close()

        await asyncio.sleep(5)