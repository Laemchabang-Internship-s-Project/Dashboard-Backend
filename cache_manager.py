import asyncio
import json
import redis
import os
import requests
from sqlalchemy import text, bindparam
from database_neoq import SessionLocal as SessionNEOQ
from database_hos import SessionLocal as SessionHOS

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
# Helper: Redis
# ==========================================================
def get_cached_data(section: str = None):
    raw = redis_client.get(KEY_DASHBOARD_CACHE)
    if not raw:
        return None
    data = json.loads(raw)
    return data.get(section) if section else data


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
                "date": last[0].strip('"'),
                "time": last[1].strip('"'),
                "shift": last[2].strip('"'),
                "type": last[3].strip('"'),
                "fuel_level": float(last[4].strip('"')),
                "mileage": float(last[5].strip('"')),
            }

    except Exception as e:
        print(f"[Cache Worker] Fuel Error: {e}")

    return None


# ==========================================================
# Master Data
# ==========================================================
OPD_TOTAL_ROOMS = (
    '010','062','005','041','042','109','110','111','001','002'
)

MASTER_ROOMS = [
    {"code": "010", "name": "จุดซักประวัติผู้ป่วยนอก"},
    {"code": "062", "name": "จุดซักประวัติผู้ป่วยนอก (นัด)"},
    {"code": "023", "name": "จุดรอตรวจ"},
    {"code": "014", "name": "ห้องหลังพบแพทย์"},
    {"code": "110", "name": "ศัลยกรรม"},
    {"code": "109", "name": "สูติกรรม"},
    {"code": "111", "name": "อายุรกรรม"},
    {"code": "005", "name": "ทันตกรรม"},
    {"code": "041", "name": "แพทย์แผนไทย"},
    {"code": "042", "name": "กายภาพ"},
    {"code": "082", "name": "คัดกรอง OPD"},
    {"code": "113", "name": "ผิวหนัง"},
    {"code": "134", "name": "อัลตราซาวด์"},
]


# ==========================================================
# Background Worker
# ==========================================================
async def update_redis_cache():
    print("[Cache Worker] เริ่มทำงาน... (ทุก 5 วิ)")

    while True:
        db = SessionNEOQ()
        db2 = SessionHOS()

        try:
            hos_sql = text("""
                SELECT 
                    SUM(CASE WHEN ovstist = '01' THEN 1 ELSE 0 END) as walk_in_count,
                    SUM(CASE WHEN ovstist = '02' THEN 1 ELSE 0 END) as appointment_count,
                    SUM(CASE WHEN ovstist = '03' THEN 1 ELSE 0 END) as referIn_count,
                    SUM(CASE WHEN ovstist = '04' THEN 1 ELSE 0 END) as ems_count,
                    SUM(CASE WHEN ovstist = '05' THEN 1 ELSE 0 END) as telemed_count,
                    SUM(CASE WHEN ovstist = '06' THEN 1 ELSE 0 END) as kiosk_count
                FROM ovst 
                WHERE vstdate = CURDATE()
            """)
            hos_res = db2.execute(hos_sql).fetchone()
            hos_walk_in = int(hos_res[0] or 0)
            hos_appointment = int(hos_res[1] or 0)
            hos_referIn = int(hos_res[2] or 0)
            hos_ems = int(hos_res[3] or 0)
            hos_telemed = int(hos_res[4] or 0)
            hos_kiosk = int(hos_res[5] or 0)

            hos_opd = text("""
                SELECT
                           
            """)
            # ==========================================
            # 1. OPD TOTAL (ยอดรวมภาพรวมคนไข้ไม่ซ้ำ HN)
            # ==========================================
            sys_sql = text("""
                SELECT
                    COUNT(DISTINCT hn),
                    COUNT(DISTINCT CASE WHEN room_code = '062' THEN hn END),
                    COUNT(DISTINCT CASE WHEN room_code != '062' THEN hn END)
                FROM opd_queue
                WHERE date = CURDATE()
                AND room_code IN :rooms
            """).bindparams(bindparam("rooms", expanding=True))

            sys_res = db.execute(sys_sql, {"rooms": OPD_TOTAL_ROOMS}).fetchone()

            opd_total   = int(sys_res[0] or 0)
            appointment = int(sys_res[1] or 0)
            walk_in     = int(sys_res[2] or 0)

            # ==========================================
            # 2. ROOM TABLE (ดึงข้อมูลทุกจุดบริการ)
            # ==========================================
            target_codes = tuple(r["code"] for r in MASTER_ROOMS)

            opd_sql = text("""
                SELECT 
                    room_code,
                    SUM(CASE WHEN room_code = '062' THEN 1 ELSE 0 END), -- index 1
                    SUM(CASE WHEN room_code != '062' THEN 1 ELSE 0 END), -- index 2
                    COUNT(*),                                          -- index 3: Total
                    SUM(CASE WHEN status_id = '3' THEN 1 ELSE 0 END),    -- index 4: Finished
                    SUM(CASE WHEN status_id != '3' THEN 1 ELSE 0 END)    -- index 5: Waiting
                FROM opd_queue
                WHERE date = CURDATE()
                AND room_code IN :rooms
                GROUP BY room_code
            """).bindparams(bindparam("rooms", expanding=True))

            res = db.execute(opd_sql, {"rooms": target_codes}).fetchall()
            db_map = {r[0]: r for r in res}

            # ==========================================
            # 3. TECH SERVICES (Lab, X-Ray, ยา, การเงิน)
            # ==========================================
            tech = {}
            for dept in ["xray_queue", "lab_queue"]:
                sql = text(f"""
                    SELECT COUNT(*),
                           SUM(CASE WHEN status_id != '3' THEN 1 ELSE 0 END),
                           SUM(CASE WHEN status_id = '3' THEN 1 ELSE 0 END)
                    FROM {dept}
                    WHERE date = CURDATE()
                """)
                r = db.execute(sql).fetchone()
                tech[dept] = {
                    "all": int(r[0] or 0),
                    "waiting": int(r[1] or 0),
                    "finished": int(r[2] or 0),
                }

            for dept in ["pharmacy_queue", "finance_queue"]:
                sql = text(f"""
                    SELECT COUNT(*),
                           SUM(CASE WHEN status_id = '3' THEN 1 ELSE 0 END),
                           SUM(CASE WHEN status_id != '3' THEN 1 ELSE 0 END)
                    FROM {dept}
                    WHERE date = CURDATE()
                """)
                r = db.execute(sql).fetchone()
                tech[dept] = {
                    "all": int(r[0] or 0),
                    "finished": int(r[1] or 0),
                    "waiting": int(r[2] or 0),
                }

            # ==========================================
            # 4. KPI Calculation (Logic ใหม่: ดึงจาก 'รอรับบริการ' ตรงๆ)
            # ==========================================
            # ดึงข้อมูลจากจุด 010 (ซักประวัติ) และ 062 (ซักนัด)
            data_010 = db_map.get('010', [0,0,0,0,0,0])
            data_062 = db_map.get('062', [0,0,0,0,0,0])
            data_023 = db_map.get('023', [0,0,0,0,0,0])

            # ✅ รวมผู้บริการภายนอก = Total 010 + Total 062
            custom_opd_total = int(data_010[3]) + int(data_062[3])

            # ✅ รอซักประวัติ = Waiting 010 + Waiting 062
            waiting_screening = int(data_010[5]) + int(data_062[5])

            # ✅ รอตรวจ = Waiting 023 (จุดรอตรวจ)
            waiting_exam = int(data_023[5])

            # ==========================================
            # 5. BUILD ROOMS LIST
            # ==========================================
            rooms = []
            for m in MASTER_ROOMS:
                r = db_map.get(m["code"])
                if r:
                    rooms.append({
                        "room_code": m["code"],
                        "room_name": m["name"],
                        "appointment": int(r[1]),
                        "walk_in": int(r[2]),
                        "total": int(r[3]),
                        "finished": int(r[4]),
                        "waiting": int(r[5]),
                    })
                else:
                    rooms.append({"room_code": m["code"], "room_name": m["name"], "appointment": 0, "walk_in": 0, "total": 0, "finished": 0, "waiting": 0})

            # ==========================================
            # 6. FUEL & FINAL JSON
            # ==========================================
            fuel = await fetch_fuel_data()

            data = {
                "system": {
                    "today_total_services": opd_total,
                    # ---- มาจากตัวแปรที่คิวรี่จาก db2 ---
                    "hos_walk_in":hos_walk_in,
                    "hos_appointment":hos_appointment,
                    "hos_referIn":hos_referIn,
                    "hos_ems":hos_ems,
                    "hos_telemed":hos_telemed,
                    "hos_kiosk":hos_kiosk,
                    "tatal_walkin":hos_walk_in+hos_kiosk
                },
                "opd_clinics": {
                    "header": {
                        "opd_total": opd_total,
                        "appointment": appointment,
                        "walk_in": walk_in,
                        "custom_opd_total": custom_opd_total,
                        "waiting_screening": waiting_screening,
                        "waiting_exam": waiting_exam
                    },
                    "rooms": rooms,
                },
                "technical_services": {
                    "xray": tech["xray_queue"],
                    "lab": tech["lab_queue"],
                    "pharmacy": tech["pharmacy_queue"],
                    "finance": tech["finance_queue"],
                },
                "car": {
                    "fuel_latest": fuel,
                },
            }

            json_data = json.dumps(data, ensure_ascii=False)
            redis_client.set(KEY_DASHBOARD_CACHE, json_data)
            redis_client.publish(CHANNEL_DASHBOARD, json_data)

        except Exception as e:
            print(f"[Cache Worker] Error: {e}")
        finally:
            db.close()
            db2.close()

        await asyncio.sleep(5)