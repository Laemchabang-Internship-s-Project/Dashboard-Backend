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
TRACKED_DEPTS = ["010", "062", "108", "109", "110", "111","011","075","044","033","072","063","005","042","041","023","066","077"
    ,"074", "901", "902", "903", "904", "905"
]

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
    {"code": "011", "name": "ER Room"},
    {"code": "075", "name": "อาชีวเวชกรรม"},
    {"code": "044", "name": "จุดซักประวัติ PCU"},
    {"code": "033", "name": "คลีนิกโรคหัวใจ"},
    {"code": "072", "name": "คลีนิก Warfarin"},
    {"code": "063", "name": "คลีนิกโรคไต"},
    {"code": "005", "name": "คลินิกทันตกรรม"},
    {"code": "042", "name": "กายภาพ"},
    {"code": "041", "name": "แพทย์แผนไทย"},
    {"code": "074", "name": "หน่วยไตเทียม"},
    {"code": "901", "name": "เวชระเบียน (บ่อวิน)"},
    {"code": "902", "name": "ซักประวัติ (บ่อวิน)"},
    {"code": "903", "name": "ห้องตรวจแพทย์ (บ่อวิน)"},
    {"code": "904", "name": "ห้องจ่ายยา (บ่อวิน)"},
    {"code": "905", "name": "ห้องทันตกรรม (บ่อวิน)"},
]


# ==========================================================
# fetch_hos_sync() — Clinical source of truth
# State machine ทั้งหมดย้ายมาอยู่ที่นี่
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
        # KPI totals (คำนวณจาก state machine)
        "custom_opd_total": 0,
        "waiting_screening": 0,
        "waiting_exam": 0,
        "waiting_lab": 0,
        "waiting_xray": 0,
        "finished_total": 0,
        "dept_stats": {},
    }

    # init dept_stats ทุก dept
    for dept in TRACKED_DEPTS:
        hos_data["dept_stats"][dept] = {
            "total": 0,
            "waiting_screening": 0,
            "waiting_exam": 0,
            "waiting_lab": 0,
            "waiting_xray": 0,
            "waiting_payment": 0,
            "waiting_drug": 0,
            "finished": 0,
        }
        hos_data[f"dep_{dept}"] = {
            "avg_total": 0.0, "avg_wait_screening": 0.0,
            "avg_wait_exam": 0.0, "avg_wait_drug": 0.0,
            "waiting_drug": 0, "waiting_payment": 0,
        }

    try:
        with SessionHOS() as db_hos:

            # --------------------------------------------------
            # 1. Visit type counts
            # --------------------------------------------------
            hos_sql = text("""
                SELECT
                    SUM(CASE WHEN ovstist = '01' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN ovstist = '02' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN ovstist = '03' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN ovstist = '04' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN ovstist = '05' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN ovstist = '06' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN cur_dep = '999' THEN 1 ELSE 0 END)
                FROM ovst
                WHERE vstdate = CURDATE()
            """)
            r = db_hos.execute(hos_sql).fetchone()
            if r:
                hos_data["walk_in"]     = int(r[0] or 0)
                hos_data["appointment"] = int(r[1] or 0)
                hos_data["referIn"]     = int(r[2] or 0)
                hos_data["ems"]         = int(r[3] or 0)
                hos_data["telemed"]     = int(r[4] or 0)
                hos_data["kiosk"]       = int(r[5] or 0)
                hos_data["go_home"]     = int(r[6] or 0)

            # --------------------------------------------------
            # 2. Drug delivery counts
            # --------------------------------------------------
            delivery_res = db_hos.execute(text("""
                SELECT
                    COUNT(DISTINCT CASE WHEN icode IN ('3907018','3907508') THEN vn END),
                    COUNT(DISTINCT CASE WHEN icode = '3907489' THEN vn END),
                    COUNT(DISTINCT vn)
                FROM opitemrece
                WHERE icode IN ('3907018','3907508','3907489')
                  AND vstdate = CURDATE()
            """)).fetchone()
            if delivery_res:
                hos_data["drug_delivery"]        = int(delivery_res[2] or 0)
                hos_data["drug_delivery_postal"] = int(delivery_res[0] or 0)
                hos_data["drug_delivery_rider"]  = int(delivery_res[1] or 0)

            # --------------------------------------------------
            # 3. Wait time averages per dept (dynamic)
            # --------------------------------------------------
            dept_selects = []
            for dept in TRACKED_DEPTS:
                dept_selects.append(f"""
                    ROUND(AVG(CASE WHEN o.main_dep='{dept}' AND s.service7  IS NOT NULL THEN GREATEST((TIME_TO_SEC(s.service7) -TIME_TO_SEC(s.service3))/60.0,0) END),1),
                    ROUND(AVG(CASE WHEN o.main_dep='{dept}' AND s.service4  IS NOT NULL THEN GREATEST((TIME_TO_SEC(s.service4) -TIME_TO_SEC(s.service3))/60.0,0) END),1),
                    ROUND(AVG(CASE WHEN o.main_dep='{dept}' AND s.service5  IS NOT NULL AND s.service11 IS NOT NULL THEN GREATEST((TIME_TO_SEC(s.service5)-TIME_TO_SEC(s.service11))/60.0,0) END),1),
                    ROUND(AVG(CASE WHEN o.main_dep='{dept}' AND s.service16 IS NOT NULL THEN GREATEST((TIME_TO_SEC(s.service16)-TIME_TO_SEC(IFNULL(s.service6,s.service12)))/60.0,0) END),1),
                    SUM(CASE WHEN o.main_dep='{dept}' AND s.service12 IS NOT NULL AND s.service6  IS NULL THEN 1 ELSE 0 END),
                    SUM(CASE WHEN o.main_dep='{dept}' AND s.service19 IS NOT NULL AND s.service7  IS NULL THEN 1 ELSE 0 END)
                """)

            tracked_depts_str = ", ".join([f"'{d}'" for d in TRACKED_DEPTS])
            svc_res = db_hos.execute(text(f"""
                SELECT
                    ROUND(AVG(CASE WHEN s.service7  IS NOT NULL THEN GREATEST((TIME_TO_SEC(s.service7) -TIME_TO_SEC(s.service3))/60.0,0) END),1),
                    ROUND(AVG(CASE WHEN s.service4  IS NOT NULL THEN GREATEST((TIME_TO_SEC(s.service4) -TIME_TO_SEC(s.service3))/60.0,0) END),1),
                    ROUND(AVG(CASE WHEN s.service5  IS NOT NULL AND s.service11 IS NOT NULL THEN GREATEST((TIME_TO_SEC(s.service5)-TIME_TO_SEC(s.service11))/60.0,0) END),1),
                    ROUND(AVG(CASE WHEN s.service16 IS NOT NULL THEN GREATEST((TIME_TO_SEC(s.service16)-TIME_TO_SEC(IFNULL(s.service6,s.service12)))/60.0,0) END),1),
                    SUM(CASE WHEN s.service12 IS NOT NULL AND s.service6  IS NULL THEN 1 ELSE 0 END),
                    SUM(CASE WHEN s.service19 IS NOT NULL AND s.service7  IS NULL THEN 1 ELSE 0 END),
                    {", ".join(dept_selects)}
                FROM service_time s
                JOIN ovst o ON s.vn = o.vn
                WHERE s.vstdate = CURDATE()
                  AND o.main_dep IN ({tracked_depts_str})
                  AND s.service3 IS NOT NULL
            """)).fetchone()

            if svc_res:
                hos_data["avg_total"]          = float(svc_res[0] or 0)
                hos_data["avg_wait_screening"] = float(svc_res[1] or 0)
                hos_data["avg_wait_exam"]      = float(svc_res[2] or 0)
                hos_data["avg_wait_drug"]      = float(svc_res[3] or 0)
                hos_data["waiting_drug"]       = int(svc_res[4] or 0)
                hos_data["waiting_payment"]    = int(svc_res[5] or 0)
                idx = 6
                for dept in TRACKED_DEPTS:
                    hos_data[f"dep_{dept}"] = {
                        "avg_total":          float(svc_res[idx]   or 0),
                        "avg_wait_screening": float(svc_res[idx+1] or 0),
                        "avg_wait_exam":      float(svc_res[idx+2] or 0),
                        "avg_wait_drug":      float(svc_res[idx+3] or 0),
                        "waiting_drug":       int(svc_res[idx+4]   or 0),
                        "waiting_payment":    int(svc_res[idx+5]   or 0),
                    }
                    idx += 6

            # --------------------------------------------------
            # 4. STATE MACHINE — preload VN sets from HOS
            # --------------------------------------------------

            # 4a. VN → main_dep mapping (source of truth)
            # --- ส่วนที่ 4a ใน fetch_hos_sync ที่ควรจะเป็น ---
            tracked_depts_tuple = tuple(TRACKED_DEPTS)
            vn_all_rows = db_hos.execute(
                text("""
                    SELECT vn, main_dep, cur_dep
                    FROM ovst
                    WHERE vstdate = CURDATE()
                      AND (main_dep IN :depts OR cur_dep IN :depts) -- ดึงทั้งคนไข้เดิมและคนที่ถูกส่งมา
                """).bindparams(bindparam("depts", expanding=True)),
                {"depts": tracked_depts_tuple}
            ).fetchall()

            # สร้างแผนที่เก็บทั้ง main และ cur
            # vn_info_map = { 'vn': (main_dep, cur_dep) }
            vn_info_map = {r[0]: (r[1], r[2]) for r in vn_all_rows}
            hos_data["vn_info_map"] = vn_info_map

            # 4b. VN states from service_time + ovst
            finished_vn = set(r[0] for r in db_hos.execute(text("""
                SELECT DISTINCT vn FROM ovst
                WHERE vstdate = CURDATE() AND cur_dep = '999'
            """)).fetchall())

            drug_vn = set(r[0] for r in db_hos.execute(text("""
                SELECT DISTINCT o.vn
                FROM service_time s JOIN ovst o ON s.vn = o.vn
                WHERE o.vstdate = CURDATE()
                  AND s.service12 IS NOT NULL AND s.service6 IS NULL
            """)).fetchall())

            payment_vn = set(r[0] for r in db_hos.execute(text("""
                SELECT DISTINCT o.vn
                FROM service_time s JOIN ovst o ON s.vn = o.vn
                WHERE o.vstdate = CURDATE()
                  AND s.service19 IS NOT NULL AND s.service7 IS NULL
            """)).fetchall())
            
            cur_dep_rows = db_hos.execute(
                text("""
                    SELECT vn, cur_dep
                    FROM ovst
                    WHERE vstdate = CURDATE()
                      AND main_dep IN :depts
                """).bindparams(bindparam("depts", expanding=True)),
                {"depts": tracked_depts_tuple}
            ).fetchall()

            cur_dep_map = {vn: cur_dep for vn, cur_dep in cur_dep_rows}
            hos_data["cur_dep_map"] = cur_dep_map

            # 4c. Lab / Xray ยังต้องดึงจาก NEOQ — ส่งกลับเป็น set ว่างไว้ก่อน
            # (จะถูก merge เข้ามาจาก fetch_neoq_sync ทีหลัง)
            # ดูส่วน merge ใน task_update_hos()
            lab_vn   = set()
            xray_vn  = set()

            # 4d. Exam: service4 stamp แล้วยังไม่ service5 (กำลังรอตรวจ)
            exam_vn = set(r[0] for r in db_hos.execute(text("""
                SELECT DISTINCT o.vn
                FROM service_time s JOIN ovst o ON s.vn = o.vn
                WHERE o.vstdate = CURDATE()
                  AND s.service4 IS NOT NULL
                  AND s.service5 IS NULL
            """)).fetchall())

            hos_data["finished_vn"] = finished_vn
            hos_data["drug_vn"]     = drug_vn
            hos_data["payment_vn"]  = payment_vn
            hos_data["exam_vn"]     = exam_vn    
    except Exception as e:
        print(f"[Cache Worker] HOSxP Error: {e}")
    
    return hos_data


# ==========================================================
# fetch_neoq_sync() — Room occupancy + tech queues เท่านั้น
# ตัด state machine ออกทั้งหมด
# ==========================================================
def fetch_neoq_sync():
    rooms = []
    tech = {
        "xray_queue":     {"all": 0, "waiting": 0, "finished": 0},
        "lab_queue":      {"all": 0, "waiting": 0, "finished": 0},
        "pharmacy_queue": {"all": 0, "waiting": 0, "finished": 0},
        "finance_queue":  {"all": 0, "waiting": 0, "finished": 0},
    }
    department_cards = []
    # lab/xray VN sets — ส่งกลับไปให้ HOS merge
    lab_vn  = set()
    xray_vn = set()

    try:
        with SessionNEOQ() as db_neoq:

            # --------------------------------------------------
            # 1. OPD total (header counts)
            # --------------------------------------------------
            opd_total = appointment = walk_in = 0
            try:
                sys_res = db_neoq.execute(
                    text("""
                        SELECT
                            COUNT(DISTINCT vn),
                            COUNT(DISTINCT CASE WHEN room_code = '062' THEN vn END),
                            COUNT(DISTINCT CASE WHEN room_code != '062' THEN vn END)
                        FROM opd_queue
                        WHERE date = CURDATE()
                          AND room_code IN :rooms
                    """).bindparams(bindparam("rooms", expanding=True)),
                    {"rooms": OPD_TOTAL_ROOMS}
                ).fetchone()
                if sys_res:
                    opd_total   = int(sys_res[0] or 0)
                    appointment = int(sys_res[1] or 0)
                    walk_in     = int(sys_res[2] or 0)
            except Exception as e:
                print(f"[OPD TOTAL ERROR] {e}")

            # --------------------------------------------------
            # 2. Room occupancy table
            # --------------------------------------------------
            try:
                target_codes = tuple(r["code"] for r in MASTER_ROOMS)
                res = db_neoq.execute(
                    text("""
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
                    """).bindparams(bindparam("rooms", expanding=True)),
                    {"rooms": target_codes}
                ).fetchall()
                db_map = {r[0]: r for r in res}

                for m in MASTER_ROOMS:
                    r = db_map.get(m["code"])
                    rooms.append({
                        "room_code":   m["code"],
                        "room_name":   m["name"],
                        "appointment": int(r[1]) if r else 0,
                        "walk_in":     int(r[2]) if r else 0,
                        "total":       int(r[3]) if r else 0,
                        "finished":    int(r[4]) if r else 0,
                        "waiting":     int(r[5]) if r else 0,
                    })
            except Exception as e:
                print(f"[ROOM TABLE ERROR] {e}")

            # --------------------------------------------------
            # 3. Tech queues
            # --------------------------------------------------
            for tech_key, table_name in {
                "lab_queue": "lab_queue",
                "xray_queue": "xray_queue",
                "pharmacy_queue": "pharmacy_queue",
                "finance_queue": "finance_queue",
            }.items():
                try:
                    q_res = db_neoq.execute(text(f"""
                        SELECT COUNT(*),
                               SUM(CASE WHEN status_id = '3' THEN 1 ELSE 0 END),
                               SUM(CASE WHEN status_id != '3' THEN 1 ELSE 0 END)
                        FROM {table_name}
                        WHERE date = CURDATE()
                    """)).fetchone()
                    if q_res:
                        tech[tech_key] = {
                            "all":      int(q_res[0] or 0),
                            "finished": int(q_res[1] or 0),
                            "waiting":  int(q_res[2] or 0),
                        }
                except Exception as e:
                    print(f"[{table_name.upper()} QUERY ERROR] {e}")

            # --------------------------------------------------
            # 4. Lab / Xray VN sets (ส่งกลับไป merge กับ HOS state machine)
            # --------------------------------------------------
            try:
                lab_vn = set(r[0] for r in db_neoq.execute(text("""
                    SELECT DISTINCT vn FROM lab_queue
                    WHERE date = CURDATE() AND status_id != '3'
                """)).fetchall())

                xray_vn = set(r[0] for r in db_neoq.execute(text("""
                    SELECT DISTINCT vn FROM xray_queue
                    WHERE date = CURDATE() AND status_id != '3'
                """)).fetchall())
            except Exception as e:
                print(f"[LAB/XRAY VN ERROR] {e}")

    except Exception as e:
        print(f"[Cache Worker] NEOQ ERROR: {e}")

    return {
        "opd_total": opd_total,
        "appointment": appointment,
        "walk_in": walk_in,
        "rooms": rooms,
        "tech": tech,
        "department_cards": department_cards,  # ว่าง — คำนวณใน task แทน
        "lab_vn": lab_vn,
        "xray_vn": xray_vn,
    }

# ==========================================================
# Background Workers (ปรับ task_update_hos + task_update_neoq)
# ==========================================================

async def task_update_hos():
    print("[Task HOSxP] เริ่มทำงาน...")
    init_analytics_db()
    log_counter = 0
    
    CUR_DEP_STATE = {
    "999": "finished",
    "016": "waiting_payment",
    "030": "waiting_drug",
    "023": "waiting_exam",    # จุดรอตรวจ -> รอพบแพทย์
    "014": "waiting_exam",    # ห้องหลังพบแพทย์ -> รอพบแพทย์
    "047": "waiting_exam",
    "059": "waiting_exam",
    "069": "waiting_exam",
    "076": "waiting_exam",
    "046": "waiting_exam",
    "007": "waiting_lab",
    "012": "waiting_xray",

    "074": "waiting_exam",      # หน่วยไตเทียม -> รอตรวจ/รับบริการ

    "901": "waiting_screening", # เวชระเบียน -> รอซักประวัติ
    "902": "waiting_screening", # ซักประวัติ -> รอซักประวัติ
    "903": "waiting_exam",      # ห้องตรวจแพทย์ -> รอตรวจ
    "905": "waiting_exam",      # ห้องทันตกรรม -> รอตรวจ
    "904": "waiting_drug",      # ห้องจ่ายยา -> รอรับยา
    
    # เพิ่มรหัสใหม่ที่ตรวจเจอว่าหลุดคิว
    "066": "waiting_screening", # ศูนย์รับส่งต่อ -> รอซักประวัติ
    "077": "waiting_screening", # งานให้คำปรึกษา -> รอซักประวัติ
}

    DEPT_USE_CUR_DEP = {"042", "041", "005", "075", "044"}
    while True:
        try:
            hos_data, neoq_data = await asyncio.gather(
                asyncio.to_thread(fetch_hos_sync),
                asyncio.to_thread(fetch_neoq_sync),
            )

            # ดึง VN sets และสถิติที่จำเป็น (ลบตัวแปรที่ไม่ได้ใช้/ซ้ำซ้อนออก)
            finished_vn = hos_data["finished_vn"]
            drug_vn     = hos_data["drug_vn"]
            payment_vn  = hos_data["payment_vn"]
            exam_vn     = hos_data["exam_vn"]
            lab_vn      = neoq_data["lab_vn"]
            xray_vn     = neoq_data["xray_vn"]
            vn_info_map = hos_data["vn_info_map"] # ใช้ตัวนี้เป็นหลัก
            dept_stats  = hos_data["dept_stats"]

            # Reset stats ก่อนเริ่มนับใหม่
            for s in dept_stats.values():
                for key in ["total","waiting_screening","waiting_exam","waiting_lab",
                            "waiting_xray","waiting_payment","waiting_drug","finished"]:
                    s[key] = 0

            # State Machine: Track ตามตำแหน่งปัจจุบัน (cur_dept)
            for vn, (main_dept, cur_dept) in vn_info_map.items():
                target_dept = cur_dept if cur_dept in TRACKED_DEPTS else main_dept

                if target_dept not in dept_stats:
                    continue
                
                s = dept_stats[target_dept]
                s["total"] += 1 

                # เช็คสถานะตามลำดับ Priority
                if vn in finished_vn:
                    s["finished"] += 1
                elif vn in drug_vn:
                    s["waiting_drug"] += 1
                elif vn in payment_vn:
                    s["waiting_payment"] += 1
                elif vn in xray_vn:
                    s["waiting_xray"] += 1
                elif vn in lab_vn:
                    s["waiting_lab"] += 1
                elif vn in exam_vn:
                    s["waiting_exam"] += 1
                else:
                    # Fallback: ใช้สถานะตาม cur_dep
                    state = CUR_DEP_STATE.get(cur_dept, "waiting_screening")
                    s[state] += 1

            # สรุปยอดรวมส่ง Dashboard
            hos_data["custom_opd_total"]  = len(vn_info_map)
            # ... ส่วนการคำนวณ sum(d["..."]) ด้านล่างเหมือนเดิม ...

            # recalculate totals
            #hos_data["custom_opd_total"]  = len(dept_vn_map)
            hos_data["waiting_screening"] = sum(d["waiting_screening"] for d in dept_stats.values())
            hos_data["waiting_exam"]      = sum(d["waiting_exam"]      for d in dept_stats.values())
            hos_data["waiting_lab"]       = sum(d["waiting_lab"]       for d in dept_stats.values())
            hos_data["waiting_xray"]      = sum(d["waiting_xray"]      for d in dept_stats.values())
            hos_data["waiting_payment"]   = sum(d["waiting_payment"]   for d in dept_stats.values())
            hos_data["waiting_drug"]      = sum(d["waiting_drug"]      for d in dept_stats.values())
            hos_data["finished_total"]    = sum(d["finished"]          for d in dept_stats.values())
            
            # --- Build patch_data ---
            total_walkin_kiosk = hos_data["walk_in"] + hos_data["kiosk"]
            total_hos_opd = (
                hos_data["walk_in"] + hos_data["appointment"] +
                hos_data["referIn"] + hos_data["ems"] +
                hos_data["telemed"] + hos_data["kiosk"]
            )

            patch_data = {
                "system": {
                    "hos_walk_in":                hos_data["walk_in"],
                    "hos_appointment":            hos_data["appointment"],
                    "hos_referIn":                hos_data["referIn"],
                    "hos_ems":                    hos_data["ems"],
                    "hos_telemed":                hos_data["telemed"],
                    "hos_kiosk":                  hos_data["kiosk"],
                    "hos_go_home":                hos_data["go_home"],
                    "total_walkin":               total_walkin_kiosk,
                    "total_OPD":                  total_hos_opd,
                    "total_drug_delivery":        hos_data["drug_delivery"],
                    "total_drug_delivery_postal": hos_data["drug_delivery_postal"],
                    "total_drug_delivery_rider":  hos_data["drug_delivery_rider"],
                    "today_total_services":       neoq_data["opd_total"],
                },
                "summary": {
                    "avg_wait_total":       hos_data["avg_total"],
                    "avg_wait_screening":   hos_data["avg_wait_screening"],
                    "avg_wait_examination": hos_data["avg_wait_exam"],
                    "avg_wait_drug":        hos_data["avg_wait_drug"],
                    "waiting_drug":         hos_data["waiting_drug"],
                    "waiting_payment":      hos_data["waiting_payment"],
                },
                "opd_clinics": {
                    "header": {
                        "opd_total":         neoq_data["opd_total"],
                        "appointment":       neoq_data["appointment"],
                        "walk_in":           neoq_data["walk_in"],
                        "custom_opd_total":  hos_data["custom_opd_total"],
                        "waiting_screening": hos_data["waiting_screening"],
                        "waiting_exam":      hos_data["waiting_exam"],
                        "waiting_lab":       hos_data["waiting_lab"],
                        "waiting_xray":      hos_data["waiting_xray"],
                        "waiting_payment":   hos_data["waiting_payment"],
                        "waiting_drug":      hos_data["waiting_drug"],
                        "finished_total":    hos_data["finished_total"],
                    },
                    "rooms": neoq_data["rooms"],
                },
                "technical_services": {
                    "xray":     neoq_data["tech"]["xray_queue"],
                    "lab":      neoq_data["tech"]["lab_queue"],
                    "pharmacy": neoq_data["tech"]["pharmacy_queue"],
                    "finance":  neoq_data["tech"]["finance_queue"],
                },
            }

            # ยัด dep_XXX และ stats_XXX
            for dept in TRACKED_DEPTS:
                patch_data["summary"][f"dep_{dept}"] = hos_data[f"dep_{dept}"]
                patch_data["opd_clinics"][f"stats_{dept}"] = dept_stats.get(dept, {})

            await patch_redis_cache(patch_data)

            # --- Analytics log (ทุก 60 รอบ = ทุก 5 นาที) ---
            if log_counter % 60 == 0:
                full_data = await get_cached_data()
                if full_data and "opd_clinics" in full_data:
                    await save_hospital_log(full_data)
                    await cleanup_old_logs(days_to_keep=1095)
                    print(f"[Log Analytics] บันทึกเรียบร้อย (รอบที่ {log_counter // 60})")

            log_counter = (log_counter + 1) % 3600

        except Exception as e:
            print(f"[Task HOSxP] Loop Error: {e}")

        await asyncio.sleep(5)


async def task_update_neoq():
    # ลบออก — รวมการทำงานเข้า task_update_hos() แล้ว
    # เหลือไว้เป็น no-op เผื่อมี caller อื่น
    print("[Task NEOQ] ถูกรวมเข้า task_update_hos แล้ว — หยุดทำงาน")
    return

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