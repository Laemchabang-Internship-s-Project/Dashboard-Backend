import asyncio
import json
import os
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
    decode_responses=True
)

# Doctor Operations — แยก 3 ระดับ
KEY_GRAPH_RAW_FULL= "graph_doctor_raw_full"
KEY_GRAPH_DAILY   = "graph_doctor_daily"
KEY_GRAPH_MONTHLY = "graph_doctor_monthly"
KEY_GRAPH_YOY     = "graph_doctor_yoy"

# Dental — แยก 3 ระดับ
KEY_DENTAL_RAW_FULL= "graph_dental_raw_full"
KEY_DENTAL_DAILY   = "graph_dental_daily"
KEY_DENTAL_MONTHLY = "graph_dental_monthly"
KEY_DENTAL_YOY     = "graph_dental_yoy"
KEY_DENTAL_META    = "graph_dental_meta"

# Death Graph
KEY_DEATH_CAUSES  = "graph_death_causes"
KEY_DEATH_MONTHLY = "graph_death_monthly"
KEY_DEATH_PLACES  = "graph_death_places"
KEY_DEATH_HOURS   = "graph_death_hours"

# Depression Graph
KEY_DEPRESSION_RAW_FULL = "graph_depression_raw_full"
KEY_DEPRESSION_DAILY    = "graph_depression_daily"
KEY_DEPRESSION_MONTHLY  = "graph_depression_monthly"
KEY_DEPRESSION_YOY      = "graph_depression_yoy"
KEY_DEPRESSION_STATUS   = "graph_depression_status"
KEY_DEPRESSION_KPI      = "graph_depression_kpi"


# ==========================================================
# Doctor Operations
# ==========================================================
def fetch_graph_incremental_sync(existing_data):
    from datetime import date, timedelta
    today = date.today()
    
    if not existing_data:
        start_date_str = '2010-01-01'
        existing_data = {}
    else:
        start_date_obj = today - timedelta(days=30)
        start_date_str = start_date_obj.strftime('%Y-%m-%d')
        curr = start_date_obj
        while curr <= today:
            existing_data[curr.strftime('%Y-%m-%d')] = 0
            curr += timedelta(days=1)

    try:
        with SessionHOS() as db_hos:
            sql = text("""
                SELECT
                    DATE(begin_date_time)       AS op_date,
                    COUNT(*)                    AS total_operations
                FROM doctor_operation
                WHERE
                    begin_date_time IS NOT NULL
                    AND begin_date_time >= :start_date
                    AND begin_date_time <= CURDATE()
                    AND begin_date_time != '0001-01-01'
                GROUP BY DATE(begin_date_time)
            """)
            rows = db_hos.execute(sql, {"start_date": start_date_str}).fetchall()

            for r in rows:
                existing_data[str(r[0])] = int(r[1])
    except Exception as e:
        print(f"[Cache Worker] Graph HOSxP Error: {e}")

    daily, monthly_map, yoy_map = [], {}, {}
    cutoff = today - timedelta(days=90)
    
    for op_date in sorted(existing_data.keys()):
        total = existing_data[op_date]
        if total == 0:
            continue
            
        yr = op_date[:4]
        mo = op_date[5:7]

        if op_date >= cutoff.strftime('%Y-%m-%d'):
            daily.append({"op_date": op_date, "total_operations": total})

        key = (yr, mo)
        monthly_map[key] = monthly_map.get(key, 0) + total

        if yr not in yoy_map:
            yoy_map[yr] = {}
        yoy_map[yr][mo] = yoy_map[yr].get(mo, 0) + total

    monthly = [
        {"year": yr, "month": mo, "total": total}
        for (yr, mo), total in sorted(monthly_map.items())
    ]
    
    return daily, monthly, yoy_map, existing_data


async def task_update_graph():
    print("[Task Graph] เริ่มทำงาน (Incremental)...")
    while True:
        try:
            raw_full_str = await redis_client.get(KEY_GRAPH_RAW_FULL)
            existing_data = json.loads(raw_full_str) if raw_full_str else {}
            
            daily, monthly, yoy, full_data = await asyncio.to_thread(fetch_graph_incremental_sync, existing_data)
            
            pipe = redis_client.pipeline()
            pipe.set(KEY_GRAPH_RAW_FULL, json.dumps(full_data, ensure_ascii=False))
            if daily:
                pipe.set(KEY_GRAPH_DAILY,   json.dumps(daily,   ensure_ascii=False))
            if monthly:
                pipe.set(KEY_GRAPH_MONTHLY, json.dumps(monthly, ensure_ascii=False))
            if yoy:
                pipe.set(KEY_GRAPH_YOY,     json.dumps(yoy,     ensure_ascii=False))
            await pipe.execute()
            print(f"[Task Graph] อัปเดตสำเร็จ: DB load ~30 days, Cache={len(full_data)} days")
        except Exception as e:
            print(f"[Task Graph] Loop Error: {e}")
        await asyncio.sleep(604800)  # 7 วัน


async def get_graph_data(view: str = "daily", month: str = None, year: str = None):
    if view == "monthly":
        raw = await redis_client.get(KEY_GRAPH_MONTHLY)
        if not raw:
            return []
        data = json.loads(raw)
        if year:
            data = [r for r in data if r["year"] == year]
        return data
    elif view == "yoy":
        raw = await redis_client.get(KEY_GRAPH_YOY)
        return json.loads(raw) if raw else {}
    else:
        raw = await redis_client.get(KEY_GRAPH_DAILY)
        if not raw:
            return []
        data = json.loads(raw)
        if month:
            data = [r for r in data if r["op_date"].startswith(month)]
        return data


# ==========================================================
# Dental
# ==========================================================
def fetch_dental_incremental_sync(existing_data):
    from datetime import date, timedelta
    today = date.today()
    
    if not existing_data:
        start_date_str = '2010-01-01'
        existing_data = {}
    else:
        start_date_obj = today - timedelta(days=30)
        start_date_str = start_date_obj.strftime('%Y-%m-%d')
        curr = start_date_obj
        while curr <= today:
            d_str = curr.strftime('%Y-%m-%d')
            existing_data[d_str] = {"patient_count": 0, "case_count": 0, "total_revenue": 0.0, "doctor_count": 0}
            curr += timedelta(days=1)

    try:
        with SessionHOS() as db_hos:
            sql = text("""
                SELECT
                    vstdate                    AS date,
                    COUNT(DISTINCT hn)         AS patient_count,
                    COUNT(dtmain_id)           AS case_count,
                    COALESCE(SUM(fee), 0)      AS total_revenue,
                    COUNT(DISTINCT doctor)     AS doctor_count
                FROM dtmain
                WHERE vstdate >= :start_date
                  AND vstdate <= CURDATE()
                GROUP BY vstdate
            """)
            rows = db_hos.execute(sql, {"start_date": start_date_str}).fetchall()

            for r in rows:
                if r[0] is None:
                    continue
                d_str = str(r[0])
                existing_data[d_str] = {
                    "patient_count": int(r[1] or 0),
                    "case_count":    int(r[2] or 0),
                    "total_revenue": float(r[3] or 0),
                    "doctor_count":  int(r[4] or 0),
                }
    except Exception as e:
        print(f"[Cache Worker] Dental HOSxP Error: {e}")

    daily, monthly_map, yoy_map, meta = [], {}, {}, None
    cutoff = today - timedelta(days=90)
    
    for d_str in sorted(existing_data.keys()):
        row_data = existing_data[d_str]
        if row_data["case_count"] == 0:
            continue
            
        yr = d_str[:4]
        mo = d_str[5:7]

        row = {
            "date": d_str,
            "patient_count": row_data["patient_count"],
            "case_count":    row_data["case_count"],
            "total_revenue": row_data["total_revenue"],
            "doctor_count":  row_data["doctor_count"]
        }

        if d_str >= cutoff.strftime('%Y-%m-%d'):
            daily.append(row)

        key = (yr, mo)
        if key not in monthly_map:
            monthly_map[key] = {"patient_count": 0, "case_count": 0, "total_revenue": 0.0, "doctor_count": 0}
        monthly_map[key]["patient_count"] += row["patient_count"]
        monthly_map[key]["case_count"]    += row["case_count"]
        monthly_map[key]["total_revenue"] += row["total_revenue"]
        monthly_map[key]["doctor_count"]  += row["doctor_count"]

        if yr not in yoy_map:
            yoy_map[yr] = {}
        if mo not in yoy_map[yr]:
            yoy_map[yr][mo] = {"patient_count": 0, "case_count": 0, "total_revenue": 0.0, "doctor_count": 0}
        yoy_map[yr][mo]["patient_count"] += row["patient_count"]
        yoy_map[yr][mo]["case_count"]    += row["case_count"]
        yoy_map[yr][mo]["total_revenue"] += row["total_revenue"]
        yoy_map[yr][mo]["doctor_count"]  += row["doctor_count"]

        meta = row

    monthly = [
        {"year": yr, "month": mo, **vals}
        for (yr, mo), vals in sorted(monthly_map.items())
    ]
    return daily, monthly, yoy_map, meta, existing_data


async def task_update_dental():
    print("[Task Dental] เริ่มทำงาน (Incremental)...")
    while True:
        try:
            raw_full_str = await redis_client.get(KEY_DENTAL_RAW_FULL)
            existing_data = json.loads(raw_full_str) if raw_full_str else {}
            
            daily, monthly, yoy, meta, full_data = await asyncio.to_thread(fetch_dental_incremental_sync, existing_data)
            
            pipe = redis_client.pipeline()
            pipe.set(KEY_DENTAL_RAW_FULL, json.dumps(full_data, ensure_ascii=False))
            if daily:
                pipe.set(KEY_DENTAL_DAILY,   json.dumps(daily,   ensure_ascii=False))
            if monthly:
                pipe.set(KEY_DENTAL_MONTHLY, json.dumps(monthly, ensure_ascii=False))
            if yoy:
                pipe.set(KEY_DENTAL_YOY,     json.dumps(yoy,     ensure_ascii=False))
            if meta:
                pipe.set(KEY_DENTAL_META,    json.dumps(meta,    ensure_ascii=False))
            await pipe.execute()
            print(f"[Task Dental] อัปเดตสำเร็จ: DB load ~30 days, Cache={len(full_data)} days")
        except Exception as e:
            print(f"[Task Dental] Loop Error: {e}")
        await asyncio.sleep(604800)  # 7 วัน


async def get_dental_data(view: str = "daily", month: str = None, year: str = None):
    if view == "monthly":
        raw = await redis_client.get(KEY_DENTAL_MONTHLY)
        if not raw:
            return []
        data = json.loads(raw)
        if year:
            data = [r for r in data if r["year"] == year]
        return data
    elif view == "yoy":
        raw = await redis_client.get(KEY_DENTAL_YOY)
        return json.loads(raw) if raw else {}
    elif view == "meta":
        raw = await redis_client.get(KEY_DENTAL_META)
        return json.loads(raw) if raw else None
    else:
        raw = await redis_client.get(KEY_DENTAL_DAILY)
        if not raw:
            return []
        data = json.loads(raw)
        if month:
            data = [r for r in data if r["date"].startswith(month)]
        return data


# ==========================================================
# Death Analytics
# ==========================================================
def fetch_death_sync():
    data = {
        "top_causes": [],
        "monthly_trend": [],
        "places": [],
        "hours": []
    }
    try:
        with SessionHOS() as db_hos:
            # 1. Top Causes (grouped by year/month for filtering)
            sql_causes = text("""
                SELECT 
                    death_cause_text AS cause,
                    COALESCE(DATE_FORMAT(NULLIF(death_date, '0000-00-00'), '%Y'), 'Unknown') AS year,
                    COALESCE(DATE_FORMAT(NULLIF(death_date, '0000-00-00'), '%m'), 'Unknown') AS month,
                    COUNT(death_id) AS total_cases
                FROM death
                WHERE death_cause_text IS NOT NULL AND death_cause_text != ''
                GROUP BY cause, year, month;
            """)
            rows_causes = db_hos.execute(sql_causes).fetchall()
            data["top_causes"] = [{"cause": r[0], "year": r[1], "month": r[2], "total_cases": int(r[3])} for r in rows_causes]

            # 2. Monthly Trend
            sql_trend = text("""
                SELECT 
                    DATE_FORMAT(death_date, '%Y-%m') AS month_year,
                    COUNT(death_id) AS death_count
                FROM death
                WHERE death_date IS NOT NULL AND death_date != '0000-00-00'
                GROUP BY month_year
                ORDER BY month_year ASC;
            """)
            try:
                rows_trend = db_hos.execute(sql_trend).fetchall()
                data["monthly_trend"] = [{"month_year": r[0], "death_count": int(r[1])} for r in rows_trend if r[0]]
            except Exception as e:
                print(f"[Cache Worker] Death Trend Error: {e}")

            # 3. Places (เพิ่ม year/month เพื่อกรองได้)
            sql_places = text("""
                SELECT 
                    CASE death_place 
                        WHEN '1' THEN 'ในโรงพยาบาล' 
                        WHEN '2' THEN 'นอกโรงพยาบาล' 
                        ELSE 'ไม่ระบุ' 
                    END AS place_name,
                    COALESCE(DATE_FORMAT(NULLIF(death_date,'0000-00-00'),'%Y'), 'Unknown') AS year,
                    COALESCE(DATE_FORMAT(NULLIF(death_date,'0000-00-00'),'%m'), 'Unknown') AS month,
                    COUNT(death_id) AS count
                FROM death
                GROUP BY death_place, year, month;
            """)
            try:
                rows_places = db_hos.execute(sql_places).fetchall()
                data["places"] = [{"place_name": r[0], "year": r[1], "month": r[2], "count": int(r[3])} for r in rows_places]
            except Exception as e:
                print(f"[Cache Worker] Death Places Error: {e}")

            # 4. Hours
            sql_hours = text("""
                SELECT 
                    HOUR(death_time) AS death_hour,
                    COUNT(death_id) AS total_cases
                FROM death
                WHERE death_time IS NOT NULL
                GROUP BY death_hour
                ORDER BY death_hour ASC;
            """)
            try:
                rows_hours = db_hos.execute(sql_hours).fetchall()
                data["hours"] = [{"death_hour": int(r[0]) if r[0] is not None else -1, "total_cases": int(r[1])} for r in rows_hours]
            except Exception as e:
                print(f"[Cache Worker] Death Hours Error: {e}")

            return data
    except Exception as e:
        print(f"[Cache Worker] Death HOSxP Error: {e}")
        return data

async def task_update_death():
    print("[Task Death] เริ่มทำงาน...")
    while True:
        try:
            data = await asyncio.to_thread(fetch_death_sync)
            if data and data.get("top_causes"):
                pipe = redis_client.pipeline()
                pipe.set(KEY_DEATH_CAUSES,  json.dumps(data.get("top_causes", []), ensure_ascii=False))
                pipe.set(KEY_DEATH_MONTHLY, json.dumps(data.get("monthly_trend", []), ensure_ascii=False))
                pipe.set(KEY_DEATH_PLACES,  json.dumps(data.get("places", []), ensure_ascii=False))
                pipe.set(KEY_DEATH_HOURS,   json.dumps(data.get("hours", []), ensure_ascii=False))
                await pipe.execute()
                print(f"[Task Death] อัปเดตสำเร็จ: {len(data['top_causes'])} causes, {len(data['monthly_trend'])} months")
        except Exception as e:
            print(f"[Task Death] Loop Error: {e}")
        await asyncio.sleep(604800)  # 7 วัน

async def get_death_data(view: str = "causes", month: str = None, year: str = None):
    if view == "monthly":
        raw = await redis_client.get(KEY_DEATH_MONTHLY)
        return json.loads(raw) if raw else []
    elif view == "places":
        raw = await redis_client.get(KEY_DEATH_PLACES)
        if not raw:
            return [{"place_name": "ไม่ระบุ", "count": 0}, {"place_name": "ในโรงพยาบาล", "count": 0}]
        data = json.loads(raw)

        # ถ้าไม่ระบุปี/เดือน ให้ดึงปีล่าสุดอัตโนมัติ
        if not year and not month:
            available_years = sorted(set(r.get("year", "") for r in data if r.get("year") not in ("", "Unknown")), reverse=True)
            if available_years:
                year = available_years[0]
                # ดึงเดือนล่าสุดในปีนั้นด้วย
                months_in_year = sorted(set(r.get("month","") for r in data if r.get("year")==year and r.get("month") not in ("","Unknown")), reverse=True)
                if months_in_year:
                    month = f"{year}-{months_in_year[0]}"

        if year:
            data = [r for r in data if r.get("year") == year]
        if month:
            parts = month.split("-")
            if len(parts) == 2:
                data = [r for r in data if r.get("year") == parts[0] and r.get("month") == parts[1]]

        # Aggregate ตาม place_name
        agg = {}
        for r in data:
            name = r["place_name"]
            agg[name] = agg.get(name, 0) + r["count"]

        if not agg:
            return [{"place_name": "ไม่ระบุ", "count": 0}, {"place_name": "ในโรงพยาบาล", "count": 0}]
        return sorted([{"place_name": k, "count": v} for k, v in agg.items()], key=lambda x: x["count"], reverse=True)
    elif view == "hours":
        raw = await redis_client.get(KEY_DEATH_HOURS)
        return json.loads(raw) if raw else []
    else: # causes
        raw = await redis_client.get(KEY_DEATH_CAUSES)
        if not raw:
            return [{"cause": "ไม่พบข้อมูล", "total_cases": 0}]
        data = json.loads(raw)
        
        # ถ้าไม่ระบุปี ให้หาปีล่าสุดที่มีข้อมูลแล้วใช้เป็น default
        if not year and not month:
            available_years = sorted(set(r.get("year", "") for r in data if r.get("year") not in ("", "Unknown")), reverse=True)
            if available_years:
                year = available_years[0]

        # Filter by year and month
        if year:
            data = [r for r in data if r.get("year") == year]
        if month:
            parts = month.split("-")
            if len(parts) == 2:
                data = [r for r in data if r.get("year") == parts[0] and r.get("month") == parts[1]]
                
        # Aggregate
        agg = {}
        for r in data:
            cause = r["cause"]
            agg[cause] = agg.get(cause, 0) + r["total_cases"]
            
        # Sort and get top 10
        sorted_causes = sorted(
            [{"cause": k, "total_cases": v} for k, v in agg.items()], 
            key=lambda x: x["total_cases"], 
            reverse=True
        )[:10]
        
        # ถ้ายังเป็นว่าง เติมแถว 0 เพื่อให้ frontend แสดงกราฟเปล่าได้
        if not sorted_causes:
            return [{"cause": "ไม่พบข้อมูล", "total_cases": 0}]
        return sorted_causes


# ==========================================================
# Depression Analytics
# ==========================================================
def fetch_depression_incremental_sync(existing_data):
    from datetime import date, timedelta
    today = date.today()
    
    # 1. กำหนดวันเริ่มต้น
    if not existing_data:
        start_date_str = '2010-01-01' # ครั้งแรกดึงย้อนหลังไกลๆ
        existing_data = {}
    else:
        # รอบต่อๆ ไป ดึงแค่ 60 วันล่าสุดเพื่อความรวดเร็วและครอบคลุมเคสคีย์ย้อนหลัง
        start_date_obj = today - timedelta(days=60)
        start_date_str = start_date_obj.strftime('%Y-%m-%d')
        # ล้างข้อมูลช่วง 60 วันล่าสุดใน existing_data เพื่อเตรียมรับข้อมูลใหม่ที่อัปเดตแล้ว
        curr = start_date_obj
        while curr <= today:
            existing_data[curr.strftime('%Y-%m-%d')] = 0
            curr += timedelta(days=1)

    try:
        with SessionHOS() as db_hos:
            # Query ดึงจำนวนผู้ลงทะเบียนรายวัน
            sql = text("""
                SELECT 
                    register_date AS op_date,
                    COUNT(patient_depression_id) AS total_cases
                FROM patient_depression
                WHERE register_date IS NOT NULL
                  AND register_date >= :start_date
                  AND register_date <= CURDATE()
                GROUP BY op_date
            """)
            rows = db_hos.execute(sql, {"start_date": start_date_str}).fetchall()
            for r in rows:
                if r[0]:
                    existing_data[str(r[0])] = int(r[1])
    except Exception as e:
        print(f"[Cache Worker] Depression Incremental Error: {e}")

    # 2. ประมวลผลข้อมูลที่รวมกันแล้วออกเป็น Daily, Monthly, YoY
    daily, monthly_map, yoy_map = [], {}, {}
    cutoff_90d = today - timedelta(days=90)
    
    for d_str in sorted(existing_data.keys()):
        total = existing_data[d_str]
        if total == 0: continue
            
        yr, mo = d_str[:4], d_str[5:7]

        # Daily (ส่งเฉพาะ 90 วันล่าสุดไปโชว์เป็นค่าเริ่มต้น)
        if d_str >= cutoff_90d.strftime('%Y-%m-%d'):
            daily.append({"date": d_str, "total_new_cases": total})

        # Monthly
        key = (yr, mo)
        monthly_map[key] = monthly_map.get(key, 0) + total

        # YoY (แยกตามปี)
        if yr not in yoy_map: yoy_map[yr] = {}
        yoy_map[yr][mo] = yoy_map[yr].get(mo, 0) + total

    monthly = [
        {"year": yr, "month": mo, "total": total}
        for (yr, mo), total in sorted(monthly_map.items())
    ]
    
    return daily, monthly, yoy_map, existing_data

async def fetch_depression_sync(existing_trend_data):
    data = {
        "daily_trend": [],
        "monthly_trend": [],
        "yoy_trend": {},
        "status_summary": [],
        "kpi": {"today_new": 0, "total_unassessed": 0, "total_cases": 0}
    }
    
    # 1. จัดการ Trend แบบ Incremental
    daily, monthly, yoy, full_data = await asyncio.to_thread(fetch_depression_incremental_sync, existing_trend_data)
    data["daily_trend"] = daily
    data["monthly_trend"] = monthly
    data["yoy_trend"] = yoy

    try:
        with SessionHOS() as db_hos:
            # 2. Status Summary (group by year and month for filtering)
            sql_status = text("""
                SELECT 
                    COALESCE(s.depression_status_name, 'ยังไม่ได้ประเมิน') AS status_name,
                    COALESCE(DATE_FORMAT(NULLIF(p.register_date, '0000-00-00'), '%Y'), 'Unknown') AS year,
                    COALESCE(DATE_FORMAT(NULLIF(p.register_date, '0000-00-00'), '%m'), 'Unknown') AS month,
                    COUNT(p.hn) AS patient_count
                FROM patient_depression p
                LEFT JOIN depression_status s ON p.depression_status_id = s.depression_status_id
                GROUP BY status_name, year, month;
            """)
            rows_status = db_hos.execute(sql_status).fetchall()
            data["status_summary"] = [{"status_name": r[0], "year": r[1], "month": r[2], "patient_count": int(r[3])} for r in rows_status]

            # 3. KPI
            sql_kpi = text("""
                SELECT
                    COUNT(patient_depression_id) as total_cases,
                    SUM(CASE WHEN register_date = CURDATE() THEN 1 ELSE 0 END) as today_new,
                    SUM(CASE WHEN depression_status_id IS NULL OR depression_status_id = '' THEN 1 ELSE 0 END) as unassessed
                FROM patient_depression
                WHERE register_date IS NOT NULL
                  AND register_date >= DATE_SUB(CURDATE(), INTERVAL 10 YEAR);
            """)
            kpi_row = db_hos.execute(sql_kpi).fetchone()
            if kpi_row:
                data["kpi"] = {
                    "total_cases": int(kpi_row[0] or 0),
                    "today_new": int(kpi_row[1] or 0),
                    "total_unassessed": int(kpi_row[2] or 0)
                }

            return data, full_data
    except Exception as e:
        print(f"[Cache Worker] Depression Summary Error: {e}")
        return data, full_data

async def task_update_depression():
    print("[Task Depression] เริ่มทำงาน (Incremental)...")
    while True:
        try:
            # 1. ดึง Trend เก่าจาก Cache
            raw_full_str = await redis_client.get(KEY_DEPRESSION_RAW_FULL)
            existing_trend = json.loads(raw_full_str) if raw_full_str else {}
            
            # 2. Fetch ข้อมูลใหม่และประมวลผล
            summary_data, full_trend = await fetch_depression_sync(existing_trend)
            
            # 3. บันทึกกลับลง Redis
            pipe = redis_client.pipeline()
            pipe.set(KEY_DEPRESSION_RAW_FULL, json.dumps(full_trend, ensure_ascii=False))
            if summary_data:
                pipe.set(KEY_DEPRESSION_DAILY,   json.dumps(summary_data.get("daily_trend", []), ensure_ascii=False))
                pipe.set(KEY_DEPRESSION_MONTHLY, json.dumps(summary_data.get("monthly_trend", []), ensure_ascii=False))
                pipe.set(KEY_DEPRESSION_YOY,     json.dumps(summary_data.get("yoy_trend", {}), ensure_ascii=False))
                pipe.set(KEY_DEPRESSION_STATUS,  json.dumps(summary_data.get("status_summary", []), ensure_ascii=False))
                pipe.set(KEY_DEPRESSION_KPI,     json.dumps(summary_data.get("kpi", {}), ensure_ascii=False))
            
            await pipe.execute()
            print(f"[Task Depression] อัปเดตสำเร็จ: Cache={len(full_trend)} days")

        except Exception as e:
            print(f"[Task Depression] Loop Error: {e}")
        await asyncio.sleep(604800)  # 7 วัน

async def get_depression_data(view: str = "daily", month: str = None, year: str = None):
    if view == "monthly":
        raw = await redis_client.get(KEY_DEPRESSION_MONTHLY)
        if not raw: return []
        data = json.loads(raw)
        if year: data = [r for r in data if r["year"] == year]
        return data
    elif view == "yoy":
        raw = await redis_client.get(KEY_DEPRESSION_YOY)
        return json.loads(raw) if raw else {}
    elif view == "status":
        raw = await redis_client.get(KEY_DEPRESSION_STATUS)
        if not raw: return []
        data = json.loads(raw)
        
        if year:
            data = [r for r in data if r.get("year") == year]
        if month:
            parts = month.split("-")
            if len(parts) == 2:
                data = [r for r in data if r.get("year") == parts[0] and r.get("month") == parts[1]]
            
        # Aggregate after filtering
        agg = {}
        for r in data:
            status = r["status_name"]
            agg[status] = agg.get(status, 0) + r["patient_count"]
        
        result = [{"status_name": k, "patient_count": v} for k, v in sorted(agg.items(), key=lambda x: x[1], reverse=True)]
        if not result:
            return [{"status_name": "ไม่พบข้อมูล", "patient_count": 0}]
        return result
    elif view == "kpi":
        raw = await redis_client.get(KEY_DEPRESSION_KPI)
        return json.loads(raw) if raw else {}
    else: # daily
        raw = await redis_client.get(KEY_DEPRESSION_DAILY)
        if not raw: return []
        data = json.loads(raw)
        if month: data = [r for r in data if r["date"].startswith(month)]
        return data

