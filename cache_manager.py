import asyncio
import json
import redis.asyncio as redis
import os
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
KEY_FUEL_CACHE   = "fuel_latest"
KEY_FUEL_HISTORY = "fuel_history"   # Redis List เก็บ 100 รายการล่าสุด
FUEL_HISTORY_MAX = 100

# ==========================================================
# Helper: Redis (ฟังก์ชันนี้จำเป็นสำหรับ main.py)
# ==========================================================
async def get_cached_data(section: str = None):
    raw = await redis_client.get(KEY_DASHBOARD_CACHE)
    if not raw:
        return None
    data = json.loads(raw)
    return data.get(section) if section else data

# ==========================================================
# Fuel Data — Webhook-driven (ไม่ polling อีกต่อไป)
# ==========================================================
async def update_fuel_cache(fuel_data: dict) -> bool:
    """
    รับข้อมูลจาก webhook แล้วอัป Redis:
      - fuel_latest  : record ล่าสุด (GET string)
      - fuel_history : list 100 รายการล่าสุด (LPUSH + LTRIM)
      - dashboard_full_cache : patch + publish SSE
    """
    try:
        json_str = json.dumps(fuel_data, ensure_ascii=False)

        # 1. เก็บ record ล่าสุด
        await redis_client.set(KEY_FUEL_CACHE, json_str)

        # 2. Push เข้า List (ใหม่สุดอยู่ index 0) แล้วตัดให้เหลือ 100
        await redis_client.lpush(KEY_FUEL_HISTORY, json_str)
        await redis_client.ltrim(KEY_FUEL_HISTORY, 0, FUEL_HISTORY_MAX - 1)

        # 3. Patch dashboard cache + publish SSE
        raw = await redis_client.get(KEY_DASHBOARD_CACHE)
        if raw:
            full = json.loads(raw)
            full["car"] = {"fuel_latest": fuel_data}
            patched = json.dumps(full, ensure_ascii=False)
            await redis_client.set(KEY_DASHBOARD_CACHE, patched)
            await redis_client.publish(CHANNEL_DASHBOARD, patched)

        print(f"[Fuel Webhook] อัปเดตสำเร็จ: {fuel_data}")
        return True
    except Exception as e:
        print(f"[Fuel Webhook] Error: {e}")
        return False

# ==========================================================
# Master Data
# ==========================================================
OPD_TOTAL_ROOMS = (
    '010', '062', '005', '041', '042', '109', '110', '111', '001', '002'
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
    print("[Cache Worker] เริ่มทำงาน... (อัปเดตระบบคิวทุก 5 วิ)")

    while True:
        # ==========================================
        # 1. โหลด fuel จาก Redis key แยก (set โดย webhook)
        # ==========================================
        fuel_raw = await redis_client.get(KEY_FUEL_CACHE)
        cached_fuel = json.loads(fuel_raw) if fuel_raw else None

        hos_data = {
            "walk_in": 0, "appointment": 0, "referIn": 0,
            "ems": 0, "telemed": 0, "kiosk": 0,
            "drug_delivery": 0
        }

        opd_total = appointment = walk_in = 0
        custom_opd_total = waiting_screening = waiting_exam = 0
        avg_wait_exam_total = 0.0
        rooms = []
        tech = {
            "xray_queue":     {"all": 0, "waiting": 0, "finished": 0},
            "lab_queue":      {"all": 0, "waiting": 0, "finished": 0},
            "pharmacy_queue": {"all": 0, "waiting": 0, "finished": 0},
            "finance_queue":  {"all": 0, "waiting": 0, "finished": 0},
        }
    
        # ==========================================
        # 3. HOSxP Query
        # ==========================================
        try:
            with SessionHOS() as db_hos:
                hos_sql = text("""
                    SELECT 
                        SUM(CASE WHEN ovstist = '01' THEN 1 ELSE 0 END) as walk_in_count,
                        SUM(CASE WHEN ovstist = '02' THEN 1 ELSE 0 END) as appointment_count,
                        SUM(CASE WHEN ovstist = '03' THEN 1 ELSE 0 END) as referIn_count,
                        SUM(CASE WHEN ovstist = '04' THEN 1 ELSE 0 END) as ems_count,
                        SUM(CASE WHEN ovstist = '05' THEN 1 ELSE 0 END) as telemed_count,
                        SUM(CASE WHEN ovstist = '06' THEN 1 ELSE 0 END) as kiosk_count,
                        SUM(CASE WHEN cur_dep = '999' THEN 1 ELSE 0 END) as go_home_count
                    FROM ovst 
                    WHERE vstdate = CURDATE()
                """)
                hos_res = db_hos.execute(hos_sql).fetchone()
                if hos_res:
                    hos_data["walk_in"]      = int(hos_res[0] or 0)
                    hos_data["appointment"]  = int(hos_res[1] or 0)
                    hos_data["referIn"]      = int(hos_res[2] or 0)
                    hos_data["ems"]          = int(hos_res[3] or 0)
                    hos_data["telemed"]      = int(hos_res[4] or 0)
                    hos_data["kiosk"]        = int(hos_res[5] or 0)
                    hos_data["go_home"]      = int(hos_res[6] or 0)
                
                delivery_sql = text("""
                    SELECT COUNT(DISTINCT vn) AS total_delivery
                    FROM opitemrece
                    WHERE icode IN ('3907489', '3907018', '3907508')
                      AND vstdate = CURDATE()
                """)
                delivery_res = db_hos.execute(delivery_sql).fetchone()
                if delivery_res:
                    hos_data["drug_delivery"] = int(delivery_res[0] or 0)
        except Exception as e:
            print(f"[Cache Worker] HOSxP Error: {e}")

        # --- 2. NEOQ Query ---
        try:
            with SessionNEOQ() as db_neoq:
                # 2.1 OPD TOTAL
                try:
                    sys_sql = text("""
                        SELECT
                            COUNT(DISTINCT hn),
                            COUNT(DISTINCT CASE WHEN room_code = '062' THEN hn END),
                            COUNT(DISTINCT CASE WHEN room_code != '062' THEN hn END)
                        FROM opd_queue
                        WHERE date = CURDATE()
                        AND room_code IN :rooms
                    """).bindparams(bindparam("rooms", expanding=True))
                    sys_res = db_neoq.execute(sys_sql, {"rooms": OPD_TOTAL_ROOMS}).fetchone()
                    if sys_res:
                        opd_total   = int(sys_res[0] or 0)
                        appointment = int(sys_res[1] or 0)
                        walk_in     = int(sys_res[2] or 0)
                except Exception as e:
                    pass

                # 2.2 ROOM TABLE
                try:
                    target_codes = tuple(r["code"] for r in MASTER_ROOMS)
                    opd_sql = text("""
                        SELECT 
                            room_code,
                            SUM(CASE WHEN room_code = '062' THEN 1 ELSE 0 END),
                            SUM(CASE WHEN room_code != '062' THEN 1 ELSE 0 END),
                            COUNT(*),
                            SUM(CASE WHEN status_id = '3' THEN 1 ELSE 0 END),
                            SUM(CASE WHEN status_id != '3' THEN 1 ELSE 0 END)
                        FROM opd_queue
                        WHERE date = CURDATE()
                        AND room_code IN :rooms
                        GROUP BY room_code
                    """).bindparams(bindparam("rooms", expanding=True))
                    res = db_neoq.execute(opd_sql, {"rooms": target_codes}).fetchall()
                    db_map = {r[0]: r for r in res}
                except Exception as e:
                    db_map = {}

                # 2.3 CALCULATION: WAIT TIME (รายห้อง แบบอัจฉริยะ)
                try:
                    target_codes = tuple(r["code"] for r in MASTER_ROOMS)
                    wait_sql = text("""
                        SELECT
                            x.room_code,
                            ROUND(AVG(x.wait_minutes), 1) AS avg_wait_minutes
                        FROM (
                            SELECT
                                c.vn,
                                c.room_code,
                                GREATEST(
                                    TIMESTAMPDIFF(
                                        MINUTE,
                                        COALESCE(
                                            CASE WHEN c.room_code IN ('010', '062', '082') THEN NULL ELSE sub.finish_time END,
                                            CONCAT(q.date, ' ', q.time)
                                        ),
                                        CONCAT(c.date, ' ', MIN(c.time))
                                    ),
                                    0
                                ) AS wait_minutes
                            FROM opd_queue_call c
                            JOIN opd_queue q ON c.vn = q.vn AND c.date = q.date
                            LEFT JOIN (
                                SELECT vn, date, MAX(time) as finish_time 
                                FROM opd_queue_call 
                                WHERE room_code IN ('010', '062')
                                GROUP BY vn, date
                            ) sub ON c.vn = sub.vn AND c.date = sub.date
                            WHERE c.date = CURDATE()
                              AND c.room_code IN :rooms
                            GROUP BY c.vn, c.room_code, q.date, q.time, sub.finish_time, c.date
                        ) x
                        WHERE x.wait_minutes < 180 
                        GROUP BY x.room_code
                    """).bindparams(bindparam("rooms", expanding=True))
                    wait_res = db_neoq.execute(wait_sql, {"rooms": target_codes}).fetchall()
                    wait_map = {r[0]: float(r[1]) for r in wait_res}
                except Exception as e:
                    print(f"[Cache Worker] NEOQ Wait Time Error: {e}")
                    wait_map = {}

                # 2.3.1 CALCULATION: SUMMARY WAIT EXAM (ภาพรวมทั้ง รพ.)
                try:
                    exam_wait_sql = text("""
                        SELECT 
                            ROUND(AVG(x.wait_exam_minutes), 1) AS avg_wait_exam_total
                        FROM (
                            SELECT 
                                c.vn,
                                GREATEST(
                                    TIMESTAMPDIFF(
                                        MINUTE,
                                        COALESCE(sub.finish_time, CONCAT(q.date, ' ', q.time)), 
                                        CONCAT(c.date, ' ', c.time)
                                    ), 
                                    0
                                ) AS wait_exam_minutes
                            FROM opd_queue_call c
                            JOIN opd_queue q ON c.vn = q.vn AND c.date = q.date
                            LEFT JOIN (
                                SELECT vn, date, MAX(time) as finish_time 
                                FROM opd_queue_call 
                                WHERE room_code IN ('010', '062')
                                GROUP BY vn, date
                            ) sub ON c.vn = sub.vn AND c.date = sub.date
                            WHERE c.date = CURDATE()
                              AND c.room_code NOT IN ('010', '062', '082')
                        ) x
                        WHERE x.wait_exam_minutes < 240
                    """)
                    exam_wait_res = db_neoq.execute(exam_wait_sql).fetchone()
                    if exam_wait_res and exam_wait_res[0] is not None:
                        avg_wait_exam_total = float(exam_wait_res[0])
                except Exception as e:
                    pass

                # 2.4 TECH SERVICES
                for dept in ["xray_queue", "lab_queue"]:
                    try:
                        sql = text(f"""
                            SELECT COUNT(*),
                                   SUM(CASE WHEN status_id != '3' THEN 1 ELSE 0 END),
                                   SUM(CASE WHEN status_id = '3' THEN 1 ELSE 0 END)
                            FROM {dept}
                            WHERE date = CURDATE()
                        """)
                        r = db_neoq.execute(sql).fetchone()
                        if r: tech[dept] = {"all": int(r[0] or 0), "waiting": int(r[1] or 0), "finished": int(r[2] or 0)}
                    except Exception as e: pass

                for dept in ["pharmacy_queue", "finance_queue"]:
                    try:
                        sql = text(f"""
                            SELECT COUNT(*),
                                   SUM(CASE WHEN status_id = '3' THEN 1 ELSE 0 END),
                                   SUM(CASE WHEN status_id != '3' THEN 1 ELSE 0 END)
                            FROM {dept}
                            WHERE date = CURDATE()
                        """)
                        r = db_neoq.execute(sql).fetchone()
                        if r: tech[dept] = {"all": int(r[0] or 0), "finished": int(r[1] or 0), "waiting": int(r[2] or 0)}
                    except Exception as e: pass

                # 2.5 KPI Calculation
                data_010 = db_map.get('010', [0, 0, 0, 0, 0, 0])
                data_062 = db_map.get('062', [0, 0, 0, 0, 0, 0])
                data_023 = db_map.get('023', [0, 0, 0, 0, 0, 0])

                custom_opd_total  = int(data_010[3]) + int(data_062[3])
                waiting_screening = int(data_010[5]) + int(data_062[5])
                waiting_exam      = int(data_023[5])

                # 2.6 BUILD ROOMS LIST
                for m in MASTER_ROOMS:
                    r = db_map.get(m["code"])
                    avg_wait = wait_map.get(m["code"], None)

                    if r:
                        room_data = {
                            "room_code": m["code"], 
                            "room_name": m["name"],
                            "appointment": int(r[1]), 
                            "walk_in": int(r[2]),
                            "total": int(r[3]), 
                            "finished": int(r[4]), 
                            "waiting": int(r[5])
                        }
                        if avg_wait is not None:
                             room_data["avg_wait_minutes"] = avg_wait 
                        rooms.append(room_data)
                    else:
                        rooms.append({
                            "room_code": m["code"], 
                            "room_name": m["name"],
                            "appointment": 0, "walk_in": 0, "total": 0, "finished": 0, "waiting": 0
                        })

        except Exception as e:
            print(f"[Cache Worker] NEOQ Connection Error: {e}")

        # --- 3. ASSEMBLE JSON & PUBLISH ---
        try:
            total_walkin_kiosk = hos_data["walk_in"] + hos_data["kiosk"]
            total_hos_opd = (hos_data.get("walk_in", 0) + hos_data.get("appointment", 0) + 
                             hos_data.get("referIn", 0) + hos_data.get("ems", 0) + 
                             hos_data.get("telemed", 0) + hos_data.get("kiosk", 0))

            data = {
                "system": {
                    "today_total_services": opd_total,
                    "hos_walk_in":          hos_data["walk_in"],
                    "hos_appointment":      hos_data["appointment"],
                    "hos_referIn":          hos_data["referIn"],
                    "hos_ems":              hos_data["ems"],
                    "hos_telemed":          hos_data["telemed"],
                    "hos_kiosk":            hos_data["kiosk"],
                    "hos_go_home":          hos_data.get("go_home", 0),
                    "total_walkin":         total_walkin_kiosk,
                    "total_OPD":            total_hos_opd,
                    "total_drug_delivery":  hos_data["drug_delivery"]
                },
                "summary": {
                    "avg_wait_examination": avg_wait_exam_total
                },
                "opd_clinics": {
                    "header": {
                        "opd_total":        opd_total,
                        "appointment":      appointment,
                        "walk_in":          walk_in,
                        "custom_opd_total": custom_opd_total,
                        "waiting_screening": waiting_screening,
                        "waiting_exam":     waiting_exam
                    },
                    "rooms": rooms,
                },
                "technical_services": {
                    "xray":     tech["xray_queue"],
                    "lab":      tech["lab_queue"],
                    "pharmacy": tech["pharmacy_queue"],
                    "finance":  tech["finance_queue"],
                },
                "car": {
                    "fuel_latest": cached_fuel,
                },
            }

            json_data = json.dumps(data, ensure_ascii=False)
            await redis_client.set(KEY_DASHBOARD_CACHE, json_data)
            await redis_client.publish(CHANNEL_DASHBOARD, json_data)

        except Exception as e:
            print(f"[Cache Worker] Redis Error: {e}")

        await asyncio.sleep(5)