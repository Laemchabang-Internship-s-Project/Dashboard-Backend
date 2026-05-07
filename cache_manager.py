import asyncio
import json
import redis.asyncio as redis
from cache_graph import task_update_graph, task_update_dental, task_update_death, task_update_depression
import os
from sqlalchemy import text, bindparam
from database_neoq import SessionLocal as SessionNEOQ
from database_hos import SessionLocal as SessionHOS
from database_analytics import SessionAnalytics
from datetime import datetime, timedelta

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
KEY_FUEL_HISTORY = "fuel_history"   
FUEL_HISTORY_MAX = 100

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

def init_analytics_db():
    with SessionAnalytics() as db:
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS hospital_logs (
                id SERIAL PRIMARY KEY,
                log_time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                total_opd INTEGER,
                total_walkin INTEGER,
                total_telemed INTEGER,
                total_drug_delivery INTEGER,
                total_drug_delivery_postal INTEGER,
                total_drug_delivery_rider INTEGER
            );
        """))
        db.commit()
        
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS hospital_dept_logs (
                id SERIAL PRIMARY KEY,
                log_id INTEGER REFERENCES hospital_logs(id) ON DELETE CASCADE,
                dept_code VARCHAR(10),
                total_patients INTEGER,
                waiting_screening INTEGER,
                waiting_exam INTEGER,
                waiting_lab INTEGER,
                waiting_xray INTEGER,
                avg_total FLOAT,
                avg_wait_screening FLOAT,
                avg_wait_exam FLOAT,
                avg_wait_drug FLOAT,
                waiting_drug INTEGER,
                waiting_payment INTEGER,
                go_home INTEGER
            );
        """))
        db.commit()

async def save_hospital_log(full_data: dict):
    try:
        with SessionAnalytics() as db:
            sys = full_data.get("system", {})
            result = db.execute(
                text("""
                    INSERT INTO hospital_logs 
                    (total_opd, total_walkin, total_telemed, 
                    total_drug_delivery, total_drug_delivery_postal, total_drug_delivery_rider)
                    VALUES (:to, :tw, :tt, :tdd, :tdd_postal, :tdd_rider)
                    RETURNING id
                """),
                {
                    "to":         sys.get("total_OPD", 0),
                    "tw":         sys.get("total_walkin", 0),
                    "tt":         sys.get("hos_telemed", 0),
                    "tdd":        sys.get("total_drug_delivery", 0),
                    "tdd_postal": sys.get("total_drug_delivery_postal", 0),
                    "tdd_rider":  sys.get("total_drug_delivery_rider", 0),
                }
            )
            log_id = result.fetchone()[0]

            clinics = full_data.get("opd_clinics", {})
            rooms = clinics.get("rooms", [])
            summary = full_data.get("summary", {})

            # ใช้ TRACKED_DEPTS เพื่อวนลูปบันทึก Log รายแผนก
            for code in TRACKED_DEPTS:
                room = next((r for r in rooms if r["room_code"] == code), {})
                dep_sum = summary.get(f"dep_{code}", {})
                stats = clinics.get(f"stats_{code}", {})

                db.execute(
                    text("""
                    INSERT INTO hospital_dept_logs 
                    (log_id, dept_code, total_patients, waiting_screening, 
                     waiting_exam, waiting_lab, waiting_xray, 
                     avg_total, avg_wait_screening, avg_wait_exam, avg_wait_drug, 
                     waiting_drug, waiting_payment, go_home)
                    VALUES (:log_id, :code, :total, :w_screen, :w_exam, :w_lab, :w_xray,
                            :a_total, :a_screen, :a_exam, :a_drug, :wd, :wp, :gh)
                    """),
                    {
                        "log_id": log_id, "code": code,
                        "total": room.get("total", 0),
                        "w_screen": room.get("waiting", 0),
                        "w_exam": stats.get("waiting_exam", 0),
                        "w_lab": stats.get("waiting_lab", 0),
                        "w_xray": stats.get("waiting_xray", 0),
                        "a_total": dep_sum.get("avg_total", 0),
                        "a_screen": dep_sum.get("avg_wait_screening", 0),
                        "a_exam": dep_sum.get("avg_wait_exam", 0),
                        "a_drug": dep_sum.get("avg_wait_drug", 0),
                        "wd": dep_sum.get("waiting_drug", 0),
                        "wp": dep_sum.get("waiting_payment", 0),
                        "gh": room.get("finished", 0)
                    }
                )
            db.commit()
    except Exception as e:
        print(f"[Save Log] Error: {e}")

async def cleanup_old_logs(days_to_keep: int = 1095): 
    try:
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        with SessionAnalytics() as db:
            db.execute(
                text("DELETE FROM hospital_logs WHERE log_time < :cutoff"),
                {"cutoff": cutoff_date}
            )
            db.commit()
            print(f"[Cleanup] สำเร็จ: ลบข้อมูลที่เก่ากว่าวันที่ {cutoff_date.date()} เรียบร้อย")
    except Exception as e:
        print(f"[Cleanup Error] : {e}")

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
# ตัวแปรหลักสำหรับดึงข้อมูลห้องที่ต้องการคำนวณเวลาแบบ Dynamics (ไม่ต้อง hardcode แล้ว)
TRACKED_DEPTS = ["010", "062", "108", "109", "110", "111"]

OPD_TOTAL_ROOMS = (
    '010', '062', '005', '041', '042', '109', '110', '111', '001', '002',
    '108', '132', '069', '020', '019', '048'
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
    {"code": "108", "name": "กุมารเวชกรรม"},
]

# ==========================================================
# Database Sync Functions
# ==========================================================
def fetch_hos_sync():
    hos_data = {
        "walk_in": 0, "appointment": 0, "referIn": 0,
        "ems": 0, "telemed": 0, "kiosk": 0, "go_home": 0,
        "drug_delivery": 0,
        "drug_delivery_postal": 0,
        "drug_delivery_rider": 0,
        "avg_total": 0.0,
        "avg_wait_screening": 0.0,
        "avg_wait_exam": 0.0,
        "avg_wait_drug": 0.0,
        "waiting_drug": 0,
        "waiting_payment": 0,
    }
    
    # สร้าง Dictionary รองรับทุกห้องอัตโนมัติ
    for dept in TRACKED_DEPTS:
        hos_data[f"dep_{dept}"] = {
            "avg_total": 0.0, "avg_wait_screening": 0.0,
            "avg_wait_exam": 0.0, "avg_wait_drug": 0.0,
            "waiting_drug": 0, "waiting_payment": 0,
        },
    
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
                SELECT 
                    COUNT(DISTINCT CASE WHEN icode IN ('3907018', '3907508') THEN vn END) AS postal,
                    COUNT(DISTINCT CASE WHEN icode = '3907489' THEN vn END) AS rider,
                    COUNT(DISTINCT vn) AS total_delivery
                FROM opitemrece
                WHERE icode IN ('3907018', '3907508', '3907489')
                  AND vstdate = CURDATE()
            """)
            delivery_res = db_hos.execute(delivery_sql).fetchone()
            
            if delivery_res:
                hos_data["total_drug_delivery"] = int(delivery_res[2] or 0)
                hos_data["total_drug_delivery_postal"] = int(delivery_res[0] or 0)
                hos_data["total_drug_delivery_rider"] = int(delivery_res[1] or 0)

            # --- สร้างคำสั่ง SQL ย่อยสำหรับแต่ละห้องแบบไดนามิก ---
            dept_selects = []
            for dept in TRACKED_DEPTS:
                dept_selects.append(f"""
                    ROUND(AVG(CASE WHEN o.main_dep = '{dept}' THEN GREATEST((TIME_TO_SEC(IFNULL(s.service7, s.service12)) - TIME_TO_SEC(s.service3)) / 60.0, 0) END), 1),
                    ROUND(AVG(CASE WHEN o.main_dep = '{dept}' THEN GREATEST((TIME_TO_SEC(s.service4)  - TIME_TO_SEC(s.service3))  / 60.0, 0) END), 1),
                    ROUND(AVG(CASE WHEN o.main_dep = '{dept}' THEN GREATEST((TIME_TO_SEC(s.service11) - TIME_TO_SEC(s.service4))  / 60.0, 0) END), 1),
                    ROUND(AVG(CASE WHEN o.main_dep = '{dept}' THEN GREATEST((TIME_TO_SEC(s.service6)  - TIME_TO_SEC(s.service12)) / 60.0, 0) END), 1),
                    SUM(CASE WHEN o.main_dep = '{dept}' AND s.service12 IS NOT NULL AND s.service6 IS NULL THEN 1 ELSE 0 END),
                    SUM(CASE WHEN o.main_dep = '{dept}' AND s.service19 IS NOT NULL AND s.service7 IS NULL THEN 1 ELSE 0 END)
                """)
            
            dept_select_str = ",\n".join(dept_selects)
            tracked_depts_str = ", ".join([f"'{d}'" for d in TRACKED_DEPTS])

            service_sql = text(f"""
                SELECT
                    -- Combined
                    ROUND(AVG(GREATEST((TIME_TO_SEC(IFNULL(s.service7, s.service12)) - TIME_TO_SEC(s.service3)) / 60.0, 0)), 1),
                    ROUND(AVG(GREATEST((TIME_TO_SEC(s.service4)  - TIME_TO_SEC(s.service3))  / 60.0, 0)), 1),
                    ROUND(AVG(GREATEST((TIME_TO_SEC(s.service11) - TIME_TO_SEC(s.service4))  / 60.0, 0)), 1),
                    ROUND(AVG(GREATEST((TIME_TO_SEC(s.service6)  - TIME_TO_SEC(s.service12)) / 60.0, 0)), 1),
                    SUM(CASE WHEN s.service12 IS NOT NULL AND s.service6  IS NULL THEN 1 ELSE 0 END),
                    SUM(CASE WHEN s.service19 IS NOT NULL AND s.service7  IS NULL THEN 1 ELSE 0 END),
                    
                    {dept_select_str}

                FROM service_time s
                JOIN ovst o ON s.vn = o.vn
                WHERE s.vstdate = CURDATE()
                  AND o.main_dep IN ({tracked_depts_str})
                  AND s.service3  IS NOT NULL
                  AND s.service4  IS NOT NULL
                  AND s.service11 IS NOT NULL
            """)
            svc_res = db_hos.execute(service_sql).fetchone()
            if svc_res:
                hos_data["avg_total"]          = float(svc_res[0] or 0)
                hos_data["avg_wait_screening"] = float(svc_res[1] or 0)
                hos_data["avg_wait_exam"]      = float(svc_res[2] or 0)
                hos_data["avg_wait_drug"]      = float(svc_res[3] or 0)
                hos_data["waiting_drug"]       = int(svc_res[4] or 0)
                hos_data["waiting_payment"]    = int(svc_res[5] or 0)
                
                # นำข้อมูลรายห้องที่ Query ออกมาวนเก็บใน Dictionary
                idx = 6
                for dept in TRACKED_DEPTS:
                    hos_data[f"dep_{dept}"] = {
                        "avg_total":          float(svc_res[idx] or 0),
                        "avg_wait_screening": float(svc_res[idx+1] or 0),
                        "avg_wait_exam":      float(svc_res[idx+2] or 0),
                        "avg_wait_drug":      float(svc_res[idx+3] or 0),
                        "waiting_drug":       int(svc_res[idx+4] or 0),
                        "waiting_payment":    int(svc_res[idx+5] or 0),
                    }
                    idx += 6

                # ลบ Hardcode 109, 110, 111, 108 ออกเพราะทำงานรวมอยู่ใน loop ด้านบนแล้ว

    except Exception as e:
        print(f"[Cache Worker] HOSxP Error: {e}")
        
    return hos_data


def fetch_neoq_sync():

    opd_total = appointment = walk_in = 0
    custom_opd_total = 0

    rooms = []

    tech = {
        "xray_queue":     {"all": 0, "waiting": 0, "finished": 0},
        "lab_queue":      {"all": 0, "waiting": 0, "finished": 0},
        "pharmacy_queue": {"all": 0, "waiting": 0, "finished": 0},
        "finance_queue":  {"all": 0, "waiting": 0, "finished": 0},
    }

    department_cards = []

    dept_stats = {
        code: {
            "total": 0,
            "waiting_screening": 0,
            "waiting_exam": 0,
            "waiting_lab": 0,
            "waiting_xray": 0,
            "waiting_payment": 0,
            "waiting_drug": 0,
            "finished": 0,
        }
        for code in TRACKED_DEPTS
    }

    try:

        with SessionNEOQ() as db_neoq, SessionHOS() as db_hos:

            # ==========================================================
            # 1. TOTAL OPD (USE VN NOT HN)
            # ==========================================================

            try:

                sys_sql = text("""
                    SELECT
                        COUNT(DISTINCT vn),
                        COUNT(DISTINCT CASE WHEN room_code = '062' THEN vn END),
                        COUNT(DISTINCT CASE WHEN room_code != '062' THEN vn END)
                    FROM opd_queue
                    WHERE date = CURDATE()
                    AND room_code IN :rooms
                """).bindparams(bindparam("rooms", expanding=True))

                sys_res = db_neoq.execute(
                    sys_sql,
                    {"rooms": OPD_TOTAL_ROOMS}
                ).fetchone()

                if sys_res:
                    opd_total   = int(sys_res[0] or 0)
                    appointment = int(sys_res[1] or 0)
                    walk_in     = int(sys_res[2] or 0)

            except Exception as e:
                print(f"[OPD TOTAL ERROR] {e}")

            # ==========================================================
            # 2. ROOM TABLE
            # ==========================================================

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

                res = db_neoq.execute(
                    opd_sql,
                    {"rooms": target_codes}
                ).fetchall()

                db_map = {r[0]: r for r in res}

            except Exception as e:
                print(f"[ROOM TABLE ERROR] {e}")

            # ==========================================================
            # 3. UNIQUE VN -> MAIN DEPT
            # ==========================================================

            dept_vn_map = {}

            try:

                dept_sql = text("""
                    SELECT
                        vn,
                        MIN(room_code) AS room_code
                    FROM opd_queue
                    WHERE date = CURDATE()
                      AND room_code IN :rooms
                    GROUP BY vn
                """).bindparams(bindparam("rooms", expanding=True))

                dept_rows = db_neoq.execute(
                    dept_sql,
                    {"rooms": TRACKED_DEPTS}
                ).fetchall()

                for vn, room_code in dept_rows:
                    dept_vn_map[vn] = room_code

            except Exception as e:
                print(f"[DEPT MAP ERROR] {e}")

            # ==========================================================
            # 4. PRELOAD STATES
            # ==========================================================

            try:

                exam_vn = set(
                    r[0] for r in db_neoq.execute(text("""
                        SELECT DISTINCT vn
                        FROM opd_queue
                        WHERE date = CURDATE()
                          AND room_code = '023'
                          AND status_id != '3'
                    """)).fetchall()
                )

                lab_vn = set(
                    r[0] for r in db_neoq.execute(text("""
                        SELECT DISTINCT vn
                        FROM lab_queue
                        WHERE date = CURDATE()
                          AND status_id != '3'
                    """)).fetchall()
                )

                xray_vn = set(
                    r[0] for r in db_neoq.execute(text("""
                        SELECT DISTINCT vn
                        FROM xray_queue
                        WHERE date = CURDATE()
                          AND status_id != '3'
                    """)).fetchall()
                )

                payment_vn = set(
                    r[0] for r in db_hos.execute(text("""
                        SELECT DISTINCT o.vn
                        FROM service_time s
                        JOIN ovst o ON s.vn = o.vn
                        WHERE o.vstdate = CURDATE()
                          AND s.service19 IS NOT NULL
                          AND s.service7 IS NULL
                    """)).fetchall()
                )

                drug_vn = set(
                    r[0] for r in db_hos.execute(text("""
                        SELECT DISTINCT o.vn
                        FROM service_time s
                        JOIN ovst o ON s.vn = o.vn
                        WHERE o.vstdate = CURDATE()
                          AND s.service12 IS NOT NULL
                          AND s.service6 IS NULL
                    """)).fetchall()
                )

                finished_vn = set(
                    r[0] for r in db_hos.execute(text("""
                        SELECT DISTINCT vn
                        FROM ovst
                        WHERE vstdate = CURDATE()
                          AND cur_dep = '999'
                    """)).fetchall()
                )

            except Exception as e:
                print(f"[PRELOAD STATES ERROR] {e}")

            # ==========================================================
            # 5. EXCLUSIVE STATE MACHINE
            # ==========================================================

            try:

                for vn, dept_code in dept_vn_map.items():

                    dept_stats[dept_code]["total"] += 1

                    # PRIORITY ORDER
                    if vn in finished_vn:
                        dept_stats[dept_code]["finished"] += 1

                    elif vn in drug_vn:
                        dept_stats[dept_code]["waiting_drug"] += 1

                    elif vn in payment_vn:
                        dept_stats[dept_code]["waiting_payment"] += 1

                    elif vn in xray_vn:
                        dept_stats[dept_code]["waiting_xray"] += 1

                    elif vn in lab_vn:
                        dept_stats[dept_code]["waiting_lab"] += 1

                    elif vn in exam_vn:
                        dept_stats[dept_code]["waiting_exam"] += 1

                    else:
                        dept_stats[dept_code]["waiting_screening"] += 1

            except Exception as e:
                print(f"[STATE MACHINE ERROR] {e}")

            # ==========================================================
            # 6. KPI TOTALS
            # ==========================================================

            custom_opd_total = len(dept_vn_map)

            waiting_screening = sum(
                d["waiting_screening"]
                for d in dept_stats.values()
            )

            waiting_exam = sum(
                d["waiting_exam"]
                for d in dept_stats.values()
            )

            waiting_lab = sum(
                d["waiting_lab"]
                for d in dept_stats.values()
            )

            waiting_xray = sum(
                d["waiting_xray"]
                for d in dept_stats.values()
            )

            waiting_payment = sum(
                d["waiting_payment"]
                for d in dept_stats.values()
            )

            waiting_drug = sum(
                d["waiting_drug"]
                for d in dept_stats.values()
            )

            finished_total = sum(
                d["finished"]
                for d in dept_stats.values()
            )

            # ==========================================================
            # 7. BUILD ROOMS
            # ==========================================================

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
                    rooms.append({
                        "room_code": m["code"],
                        "room_name": m["name"],
                        "appointment": 0,
                        "walk_in": 0,
                        "total": 0,
                        "finished": 0,
                        "waiting": 0,
                    })

            # ==========================================================
            # 8. DEPARTMENT CARDS
            # ==========================================================

            dept_groups = [
                {"dept_name": "ศัลยกรรม", "codes": ["110"]},
                {"dept_name": "สูติกรรม", "codes": ["109"]},
                {"dept_name": "อายุรกรรม", "codes": ["111"]},
                {"dept_name": "กุมารเวชกรรม", "codes": ["108"]},
            ]

            for g in dept_groups:

                c_total = 0
                c_waiting = 0
                c_finished = 0

                for code in g["codes"]:

                    stat = dept_stats.get(code, {})

                    c_total += stat.get("total", 0)

                    c_waiting += (
                        stat.get("waiting_screening", 0)
                        + stat.get("waiting_exam", 0)
                        + stat.get("waiting_lab", 0)
                        + stat.get("waiting_xray", 0)
                        + stat.get("waiting_payment", 0)
                        + stat.get("waiting_drug", 0)
                    )

                    c_finished += stat.get("finished", 0)

                department_cards.append({
                    "dept_name": g["dept_name"],
                    "total": c_total,
                    "waiting": c_waiting,
                    "finished": c_finished,
                })

    except Exception as e:
        print(f"[Cache Worker] NEOQ ERROR: {e}")

    return {
        "opd_total": opd_total,
        "appointment": appointment,
        "walk_in": walk_in,

        "custom_opd_total": custom_opd_total,

        "waiting_screening": waiting_screening,
        "waiting_exam": waiting_exam,
        "waiting_lab": waiting_lab,
        "waiting_xray": waiting_xray,
        "waiting_payment": waiting_payment,
        "waiting_drug": waiting_drug,
        "finished_total": finished_total,

        "rooms": rooms,
        "department_cards": department_cards,
        "tech": tech,
        "dept_stats": dept_stats
    }

# ==========================================================
# Background Workers
# ==========================================================
async def task_update_hos():
    print("[Task HOSxP] เริ่มทำงาน...")
    init_analytics_db()

    log_counter = 0
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
                    "total_drug_delivery":        hos_data["total_drug_delivery"], 
                    "total_drug_delivery_postal": hos_data["total_drug_delivery_postal"],  
                    "total_drug_delivery_rider":  hos_data["total_drug_delivery_rider"],
                },
                "summary": {
                    "avg_wait_total":       hos_data["avg_total"],
                    "avg_wait_screening":   hos_data["avg_wait_screening"],
                    "avg_wait_examination": hos_data["avg_wait_exam"],
                    "avg_wait_drug":        hos_data["avg_wait_drug"],
                    "waiting_drug":         hos_data["waiting_drug"],
                    "waiting_payment":      hos_data["waiting_payment"]
                }
            }
            
            # ยัดข้อมูล dep_XXX เข้าไปใน summary อัตโนมัติ
            for dept in TRACKED_DEPTS:
                patch_data["summary"][f"dep_{dept}"] = hos_data[f"dep_{dept}"]

            await patch_redis_cache(patch_data)

            if (log_counter % 60 == 0):
                full_data = await get_cached_data() 
                if full_data and "opd_clinics" in full_data:
                    await save_hospital_log(full_data)
                    await cleanup_old_logs(days_to_keep=1095) 
                    print(f"[Log Analytics] บันทึก 2 Tables เรียบร้อย (รอบที่ {log_counter // 60})")

            log_counter += 1
            if(log_counter >= 3600): log_counter = 0

        except Exception as e:
            print(f"[Task HOSxP] Loop Error: {e}")
            
        await asyncio.sleep(5)

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
                        "waiting_exam":      n_data["waiting_exam"],
                        "waiting_lab":       n_data["waiting_lab"],
                        "waiting_xray":      n_data["waiting_xray"],
                        "waiting_payment":   n_data["waiting_payment"],
                        "waiting_drug":      n_data["waiting_drug"],
                        "finished_total":    n_data["finished_total"],
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
            
            # ยัดข้อมูล stats_XXX เข้าไปใน opd_clinics อัตโนมัติ
            for dept in TRACKED_DEPTS:
                patch_data["opd_clinics"][f"stats_{dept}"] = n_data["dept_stats"][dept]

            await patch_redis_cache(patch_data)
        except Exception as e:
            print(f"[Task NEOQ] Loop Error: {e}")
            
        await asyncio.sleep(5)

# ==========================================================
# Main Entry Point
# ==========================================================
async def update_redis_cache():
    print("[Cache Worker] เริ่มกระจายงาน (HOSxP และ NEOQ รันขนานกัน)...")
    
    asyncio.create_task(task_update_hos())
    asyncio.create_task(task_update_neoq())
    asyncio.create_task(task_update_graph())
    asyncio.create_task(task_update_dental())
    asyncio.create_task(task_update_death())
    asyncio.create_task(task_update_depression())
    
    while True:
        await asyncio.sleep(3600)