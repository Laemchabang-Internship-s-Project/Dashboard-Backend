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
    password=os.getenv("REDIS_PASSWORD"),
    db=0,
    decode_responses=True
)

CHANNEL_DASHBOARD = "dashboard_events"
KEY_DASHBOARD_CACHE = "dashboard_full_cache"
KEY_FUEL_CACHE   = "fuel_latest"
KEY_FUEL_HISTORY = "fuel_history"   # Redis List เก็บ 100 รายการล่าสุด
FUEL_HISTORY_MAX = 100
KEY_GRAPH_CACHE  = "graph_doctor_operation"
KEY_DENTAL_CACHE = "graph_dental_summary"

# ==========================================================
# Helper: Redis
# ==========================================================
async def get_cached_data(section: str = None):
    raw = await redis_client.get(KEY_DASHBOARD_CACHE)
    if not raw:
        return None
    data = json.loads(raw)
    return data.get(section) if section else data

async def patch_redis_cache(new_data: dict):
    """
    อัปเดตข้อมูลลง Redis แบบ Deep Update 
    เพื่อไม่ให้ข้อมูลย่อยใน "system" ของ HOS และ NEOQ เขียนทับกันเอง
    """
    try:
        raw = await redis_client.get(KEY_DASHBOARD_CACHE)
        full_data = json.loads(raw) if raw else {}

        for key, value in new_data.items():
            if isinstance(value, dict) and key in full_data and isinstance(full_data[key], dict):
                full_data[key].update(value)
            else:
                full_data[key] = value

        if "car" not in full_data:
            fuel_raw = await redis_client.get(KEY_FUEL_CACHE)
            full_data["car"] = {"fuel_latest": json.loads(fuel_raw) if fuel_raw else None}

        json_data = json.dumps(full_data, ensure_ascii=False)
        await redis_client.set(KEY_DASHBOARD_CACHE, json_data)
        await redis_client.publish(CHANNEL_DASHBOARD, json_data)
    except Exception as e:
        print(f"[Patch Cache] Error: {e}")

# ==========================================================
# Fuel Data — Webhook-driven
# ==========================================================
async def update_fuel_cache(fuel_data: dict) -> bool:
    try:
        json_str = json.dumps(fuel_data, ensure_ascii=False)

        await redis_client.set(KEY_FUEL_CACHE, json_str)

        await redis_client.lpush(KEY_FUEL_HISTORY, json_str)
        await redis_client.ltrim(KEY_FUEL_HISTORY, 0, FUEL_HISTORY_MAX - 1)

        await patch_redis_cache({"car": {"fuel_latest": fuel_data}})

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
# Database Sync Functions (ย้าย Query มาอยู่ใน Thread)
# ==========================================================
def fetch_hos_sync():
    hos_data = {
        "walk_in": 0, "appointment": 0, "referIn": 0,
        "ems": 0, "telemed": 0, "kiosk": 0, "go_home": 0,
        "drug_delivery": 0,
        "avg_total": 0.0,
        "avg_wait_screening": 0.0,
        "avg_wait_exam": 0.0,
        "avg_wait_drug": 0.0,
        "waiting_drug": 0,
        "waiting_payment": 0,
        "dep_010": {
            "avg_total": 0.0, "avg_wait_screening": 0.0,
            "avg_wait_exam": 0.0, "avg_wait_drug": 0.0,
            "waiting_drug": 0, "waiting_payment": 0,
        },
        "dep_062": {
            "avg_total": 0.0, "avg_wait_screening": 0.0,
            "avg_wait_exam": 0.0, "avg_wait_drug": 0.0,
            "waiting_drug": 0, "waiting_payment": 0,
        },
    }
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
                WHERE icode IN ('3907018', '3907508') 
                  AND vstdate = CURDATE()
            """)
            delivery_res = db_hos.execute(delivery_sql).fetchone()
            if delivery_res:
                hos_data["drug_delivery"] = int(delivery_res[0] or 0)

            service_sql = text("""
                SELECT
                    -- Combined
                    ROUND(AVG(GREATEST((TIME_TO_SEC(IFNULL(s.service7, s.service12)) - TIME_TO_SEC(s.service3)) / 60.0, 0)), 1),
                    ROUND(AVG(GREATEST((TIME_TO_SEC(s.service4)  - TIME_TO_SEC(s.service3))  / 60.0, 0)), 1),
                    ROUND(AVG(GREATEST((TIME_TO_SEC(s.service11) - TIME_TO_SEC(s.service4))  / 60.0, 0)), 1),
                    ROUND(AVG(GREATEST((TIME_TO_SEC(s.service6)  - TIME_TO_SEC(s.service12)) / 60.0, 0)), 1),
                    SUM(CASE WHEN s.service12 IS NOT NULL AND s.service6  IS NULL THEN 1 ELSE 0 END),
                    SUM(CASE WHEN s.service19 IS NOT NULL AND s.service7  IS NULL THEN 1 ELSE 0 END),
                    
                    -- 010
                    ROUND(AVG(CASE WHEN o.main_dep = '010' THEN GREATEST((TIME_TO_SEC(IFNULL(s.service7, s.service12)) - TIME_TO_SEC(s.service3)) / 60.0, 0) END), 1),
                    ROUND(AVG(CASE WHEN o.main_dep = '010' THEN GREATEST((TIME_TO_SEC(s.service4)  - TIME_TO_SEC(s.service3))  / 60.0, 0) END), 1),
                    ROUND(AVG(CASE WHEN o.main_dep = '010' THEN GREATEST((TIME_TO_SEC(s.service11) - TIME_TO_SEC(s.service4))  / 60.0, 0) END), 1),
                    ROUND(AVG(CASE WHEN o.main_dep = '010' THEN GREATEST((TIME_TO_SEC(s.service6)  - TIME_TO_SEC(s.service12)) / 60.0, 0) END), 1),
                    SUM(CASE WHEN o.main_dep = '010' AND s.service12 IS NOT NULL AND s.service6 IS NULL THEN 1 ELSE 0 END),
                    SUM(CASE WHEN o.main_dep = '010' AND s.service19 IS NOT NULL AND s.service7 IS NULL THEN 1 ELSE 0 END),

                    -- 062
                    ROUND(AVG(CASE WHEN o.main_dep = '062' THEN GREATEST((TIME_TO_SEC(IFNULL(s.service7, s.service12)) - TIME_TO_SEC(s.service3)) / 60.0, 0) END), 1),
                    ROUND(AVG(CASE WHEN o.main_dep = '062' THEN GREATEST((TIME_TO_SEC(s.service4)  - TIME_TO_SEC(s.service3))  / 60.0, 0) END), 1),
                    ROUND(AVG(CASE WHEN o.main_dep = '062' THEN GREATEST((TIME_TO_SEC(s.service11) - TIME_TO_SEC(s.service4))  / 60.0, 0) END), 1),
                    ROUND(AVG(CASE WHEN o.main_dep = '062' THEN GREATEST((TIME_TO_SEC(s.service6)  - TIME_TO_SEC(s.service12)) / 60.0, 0) END), 1),
                    SUM(CASE WHEN o.main_dep = '062' AND s.service12 IS NOT NULL AND s.service6 IS NULL THEN 1 ELSE 0 END),
                    SUM(CASE WHEN o.main_dep = '062' AND s.service19 IS NOT NULL AND s.service7 IS NULL THEN 1 ELSE 0 END)

                FROM service_time s
                JOIN ovst o ON s.vn = o.vn
                WHERE s.vstdate = CURDATE()
                  AND o.main_dep IN ('010', '062')
                  AND s.service3  IS NOT NULL
                  AND s.service4  IS NOT NULL
                  AND s.service11 IS NOT NULL
            """)
            svc_res = db_hos.execute(service_sql).fetchone()
            if svc_res:
                # Combined
                hos_data["avg_total"]          = float(svc_res[0] or 0)
                hos_data["avg_wait_screening"] = float(svc_res[1] or 0)
                hos_data["avg_wait_exam"]      = float(svc_res[2] or 0)
                hos_data["avg_wait_drug"]      = float(svc_res[3] or 0)
                hos_data["waiting_drug"]       = int(svc_res[4] or 0)
                hos_data["waiting_payment"]    = int(svc_res[5] or 0)
                
                # 010
                hos_data["dep_010"] = {
                    "avg_total":          float(svc_res[6] or 0),
                    "avg_wait_screening": float(svc_res[7] or 0),
                    "avg_wait_exam":      float(svc_res[8] or 0),
                    "avg_wait_drug":      float(svc_res[9] or 0),
                    "waiting_drug":       int(svc_res[10] or 0),
                    "waiting_payment":    int(svc_res[11] or 0),
                }

                # 062
                hos_data["dep_062"] = {
                    "avg_total":          float(svc_res[12] or 0),
                    "avg_wait_screening": float(svc_res[13] or 0),
                    "avg_wait_exam":      float(svc_res[14] or 0),
                    "avg_wait_drug":      float(svc_res[15] or 0),
                    "waiting_drug":       int(svc_res[16] or 0),
                    "waiting_payment":    int(svc_res[17] or 0),
                }

    except Exception as e:
        print(f"[Cache Worker] HOSxP Error: {e}")
        
    return hos_data

def fetch_graph_sync():
    """ดึงข้อมูลกราฟการผ่าตัดของแพทย์"""
    data = []
    try:
        with SessionHOS() as db_hos:
            sql = text("""
                SELECT 
                    DATE(begin_date_time) AS op_date,
                    COUNT(*) AS total_operations
                FROM doctor_operation
                WHERE 
                    begin_date_time IS NOT NULL
                    AND begin_date_time >= '2010-01-01'  -- ตัด 1899, 1917
                    AND begin_date_time <= CURDATE()     -- ตัดอนาคต เช่น 2056
                    AND begin_date_time != '0001-01-01'  
                GROUP BY DATE(begin_date_time)
                ORDER BY op_date DESC;
            """)
            res = db_hos.execute(sql).fetchall()
            for r in res:
                data.append({
                    "op_date": str(r[0]),
                    "total_operations": int(r[1])
                })
    except Exception as e:
        print(f"[Cache Worker] Graph HOSxP Error: {e}")
    return data

def fetch_neoq_sync():
    opd_total = appointment = walk_in = 0
    custom_opd_total = waiting_screening = waiting_exam = 0
    rooms = []
    tech = {
        "xray_queue":     {"all": 0, "waiting": 0, "finished": 0},
        "lab_queue":      {"all": 0, "waiting": 0, "finished": 0},
        "pharmacy_queue": {"all": 0, "waiting": 0, "finished": 0},
        "finance_queue":  {"all": 0, "waiting": 0, "finished": 0},
    }
    
    dept_stats = {
        "010": {"total": 0, "waiting_screening": 0, "waiting_exam": 0, "waiting_lab": 0, "waiting_xray": 0},
        "062": {"total": 0, "waiting_screening": 0, "waiting_exam": 0, "waiting_lab": 0, "waiting_xray": 0}
    }

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
            except Exception: pass

            # 2.2 ROOM TABLE
            db_map = {}
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
            except Exception: pass
            
            # --- NEW: Department Specific Subqueries ---
            try:
                for code in ["010", "062"]:
                    row = db_map.get(code, [0, 0, 0, 0, 0, 0])
                    dept_stats[code]["total"] = int(row[3])
                    dept_stats[code]["waiting_screening"] = int(row[5])
                    
                    exam_sql = text("""
                        SELECT COUNT(DISTINCT q.vn) 
                        FROM opd_queue q 
                        WHERE q.date = CURDATE() AND q.room_code = '023' AND q.status_id != '3'
                        AND q.vn IN (SELECT vn FROM opd_queue WHERE room_code = :code AND date = CURDATE())
                    """)
                    dept_stats[code]["waiting_exam"] = db_neoq.execute(exam_sql, {"code": code}).scalar() or 0

                    lab_sql = text("""
                        SELECT COUNT(DISTINCT l.vn) 
                        FROM lab_queue l 
                        WHERE l.date = CURDATE() AND l.status_id != '3'
                        AND l.vn IN (SELECT vn FROM opd_queue WHERE room_code = :code AND date = CURDATE())
                    """)
                    dept_stats[code]["waiting_lab"] = db_neoq.execute(lab_sql, {"code": code}).scalar() or 0

                    xray_sql = text("""
                        SELECT COUNT(DISTINCT x.vn) 
                        FROM xray_queue x 
                        WHERE x.date = CURDATE() AND x.status_id != '3'
                        AND x.vn IN (SELECT vn FROM opd_queue WHERE room_code = :code AND date = CURDATE())
                    """)
                    dept_stats[code]["waiting_xray"] = db_neoq.execute(xray_sql, {"code": code}).scalar() or 0
            except Exception as e:
                print(f"[Cache Worker] Dept Stats Subquery Error: {e}")

            # 2.3 CALCULATION: WAIT TIME (รายห้อง แบบอัจฉริยะ)
            wait_map = {}
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
            except Exception: pass

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
                except Exception: pass

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
                except Exception: pass

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

    return {
        "opd_total": opd_total,
        "appointment": appointment,
        "walk_in": walk_in,
        "custom_opd_total": custom_opd_total,
        "waiting_screening": waiting_screening,
        "waiting_exam": waiting_exam,
        "rooms": rooms,
        "tech": tech,
        "dept_stats": dept_stats
    }

# ==========================================================
# Background Workers (แยก Task อิสระ)
# ==========================================================
async def task_update_hos():
    """จัดการอัปเดตข้อมูลฝั่ง HOSxP ทุก 5 วินาที"""
    print("[Task HOSxP] เริ่มทำงาน...")
    while True:
        try:
            hos_data = await asyncio.to_thread(fetch_hos_sync)
            
            total_walkin_kiosk = hos_data["walk_in"] + hos_data["kiosk"]
            total_hos_opd = (hos_data.get("walk_in", 0) + hos_data.get("appointment", 0) + 
                             hos_data.get("referIn", 0) + hos_data.get("ems", 0) + 
                             hos_data.get("telemed", 0) + hos_data.get("kiosk", 0))

            patch_data = {
                "system": {
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
                    "avg_wait_total":       hos_data["avg_total"],
                    "avg_wait_screening":   hos_data["avg_wait_screening"],
                    "avg_wait_examination": hos_data["avg_wait_exam"],
                    "avg_wait_drug":        hos_data["avg_wait_drug"],
                    "waiting_drug":         hos_data["waiting_drug"],
                    "waiting_payment":      hos_data["waiting_payment"],
                    "dep_010":              hos_data["dep_010"],
                    "dep_062":              hos_data["dep_062"],
                }
            }
            await patch_redis_cache(patch_data)
        except Exception as e:
            print(f"[Task HOSxP] Loop Error: {e}")
            
        await asyncio.sleep(5)

async def task_update_graph():
    """จัดการอัปเดตข้อมูลกราฟ (รันสัปดาห์ละครั้ง)"""
    print("[Task Graph] เริ่มทำงาน...")
    while True:
        try:
            graph_data = await asyncio.to_thread(fetch_graph_sync)
            if graph_data:
                await redis_client.set(KEY_GRAPH_CACHE, json.dumps(graph_data, ensure_ascii=False))
        except Exception as e:
            print(f"[Task Graph] Loop Error: {e}")
            
        await asyncio.sleep(604800) # 7 วัน

async def get_graph_data():
    raw = await redis_client.get(KEY_GRAPH_CACHE)
    if not raw:
        return []
    return json.loads(raw)

def fetch_dental_sync():
    """ดึงข้อมูลกราฟแผนกทันตกรรมจาก dtmain"""
    data = []
    try:
        with SessionHOS() as db_hos:
            sql = text("""
                SELECT 
                    vstdate AS date,
                    COUNT(DISTINCT hn)     AS patient_count,
                    COUNT(dtmain_id)       AS case_count,
                    SUM(fee)               AS total_revenue,
                    COUNT(DISTINCT doctor) AS doctor_count
                FROM dtmain
                WHERE vstdate IS NOT NULL
                GROUP BY vstdate
                ORDER BY date DESC
            """)
            res = db_hos.execute(sql).fetchall()
            for r in res:
                if r[0] is None:
                    continue
                data.append({
                    "date":          str(r[0]),
                    "patient_count": int(r[1] or 0),
                    "case_count":    int(r[2] or 0),
                    "total_revenue": float(r[3] or 0),
                    "doctor_count":  int(r[4] or 0),
                })
    except Exception as e:
        print(f"[Cache Worker] Dental HOSxP Error: {e}")
    return data


async def task_update_dental():
    """จัดการอัปเดตข้อมูลกราฟทันตกรรม (รันสัปดาห์ละครั้ง)"""
    print("[Task Dental] เริ่มทำงาน...")
    while True:
        try:
            dental_data = await asyncio.to_thread(fetch_dental_sync)
            if dental_data:
                await redis_client.set(KEY_DENTAL_CACHE, json.dumps(dental_data, ensure_ascii=False))
                print(f"[Task Dental] อัปเดต {len(dental_data)} แถว เรียบร้อย")
        except Exception as e:
            print(f"[Task Dental] Loop Error: {e}")
        await asyncio.sleep(604800)  # 7 วัน

async def get_dental_data():
    raw = await redis_client.get(KEY_DENTAL_CACHE)
    if not raw:
        return []
    return json.loads(raw)

async def task_update_neoq():
    print("[Task NEOQ] เริ่มทำงาน...")
    while True:
        try:
            n_data = await asyncio.to_thread(fetch_neoq_sync)

            patch_data = {
                "system": {
                    "today_total_services": n_data["opd_total"]
                },
                "opd_clinics": {
                    "header": {
                        "opd_total":         n_data["opd_total"],
                        "appointment":       n_data["appointment"],
                        "walk_in":           n_data["walk_in"],
                        "custom_opd_total":  n_data["custom_opd_total"],
                        "waiting_screening": n_data["waiting_screening"],
                        "waiting_exam":      n_data["waiting_exam"]
                    },
                    "rooms": n_data["rooms"],
                    "stats_010": n_data["dept_stats"]["010"],
                    "stats_062": n_data["dept_stats"]["062"]
                },
                "technical_services": {
                    "xray":     n_data["tech"]["xray_queue"],
                    "lab":      n_data["tech"]["lab_queue"],
                    "pharmacy": n_data["tech"]["pharmacy_queue"],
                    "finance":  n_data["tech"]["finance_queue"],
                }
            }
            await patch_redis_cache(patch_data)
        except Exception as e:
            print(f"[Task NEOQ] Loop Error: {e}")
            
        await asyncio.sleep(5)

# ==========================================================
# Main Entry Point
# ==========================================================
async def update_redis_cache():
    """
    ฟังก์ชันหลักที่รักษาชื่อเดิมไว้ เพื่อไม่ให้ไฟล์ main.py (ที่เรียกใช้คำสั่งนี้) พัง
    จะทำหน้าที่เป็นคนแตกงานออกเป็น 2 ส่วนให้ทำงานขนานกัน
    """
    print("[Cache Worker] เริ่มกระจายงาน (HOSxP และ NEOQ รันขนานกัน)...")
    
    asyncio.create_task(task_update_hos())
    asyncio.create_task(task_update_neoq())
    asyncio.create_task(task_update_graph())
    asyncio.create_task(task_update_dental())
    
    while True:
        await asyncio.sleep(3600)