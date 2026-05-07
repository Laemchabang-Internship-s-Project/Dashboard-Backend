"""
cache_finance.py
================
โมดูลจัดการ Cache ข้อมูลการเงิน (Finance) สำหรับ Dashboard

ตารางที่อ้างอิง:
  - incoth      : ตารางหลัก เก็บรายการรับเงิน/ใบเสร็จ
                  (เช่น rcptamt=ยอดเงิน, paidst=สถานะชำระ, billdate=วันที่บิล, pttype=ประเภทผู้ป่วย)
  - pttype      : ตารางอ้างอิง ประเภทสิทธิ์ผู้ป่วย (pttype, name)
  - paidst      : ตารางอ้างอิง สถานะการชำระเงิน (paidst, name)
                  paidst=0 → ค้างชำระ, paidst=1,3 → จ่ายสด, paidst=2 → ลูกหนี้/เบิกสิทธิ์

Schedule อัปเดต Cache:
  - 08:00–15:59 น. → อัปเดตทุก 1 ชั่วโมง (ช่วง OPD เปิด)
  - นอกเวลาดังกล่าว  → อัปเดตทุก 3 ชั่วโมง (ลดภาระ DB)
"""

import asyncio
import json
import os
from datetime import datetime, date, timedelta

import redis.asyncio as redis
from sqlalchemy import text

# ใช้การเชื่อมต่อฐานข้อมูล HOSxP เดิม
from database_hos import SessionLocal as SessionHOS

# ==========================================================
# Redis Connection (ใช้ค่าจาก .env เหมือนกับไฟล์อื่น)
# ==========================================================
redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    password=os.getenv("REDIS_PASSWORD"),
    db=0,
    decode_responses=True
)

# ==========================================================
# Redis Keys — ชื่อ Key สำหรับเก็บข้อมูล Finance แต่ละ view
# ==========================================================
KEY_FINANCE_RAW_FULL = "graph_finance_raw_full"   # เก็บข้อมูลดิบรายวัน (Incremental store)
KEY_FINANCE_BY_PTTYPE = "graph_finance_by_pttype"  # สรุปตามประเภทผู้ป่วย (ผลลัพธ์หลัก)
KEY_FINANCE_YEARLY    = "graph_finance_yearly"     # สรุปรายปี
KEY_FINANCE_MONTHLY   = "graph_finance_monthly"    # สรุปรายเดือน
KEY_FINANCE_DAILY     = "graph_finance_daily"      # สรุปรายวัน (90 วันล่าสุด)
KEY_FINANCE_KPI       = "graph_finance_kpi"        # KPI ยอดรวมวันนี้ / เดือนนี้


# ==========================================================
# Sync Function: ดึงข้อมูลจาก DB + รวมกับ Cache เดิม
# ==========================================================
def fetch_finance_incremental_sync(existing_data: dict) -> tuple:
    """
    ดึงข้อมูลการเงินแบบ Incremental จาก HOSxP

    Args:
        existing_data (dict): ข้อมูลดิบที่มีอยู่ใน Cache แล้ว
                              รูปแบบ: {"YYYY-MM-DD": {"pttype_code": {...}}}

    Returns:
        tuple: (by_pttype_list, monthly_list, daily_list, kpi_dict, full_data)

    ตารางที่ Query:
        - incoth  : รายการรับเงิน (rcptamt, paidst, billdate, pttype, hn, vn)
        - pttype  : ประเภทสิทธิ์ผู้ป่วย (pttype, name)
        - paidst  : ตารางอ้างอิงสถานะการชำระ (join เพื่อ label แต่ไม่ได้ใช้ column)
    """
    from datetime import date, timedelta
    today = date.today()
    START_YEAR = 2022  # ปีเริ่มต้นที่จะดึงข้อมูลย้อนหลัง (ตรงกับ YEAR_OPTIONS ใน Frontend)

    # --- กำหนดช่วงวันที่จะดึง ---
    if not existing_data:
        # ครั้งแรก: ดึงย้อนหลังตั้งแต่ต้นปี START_YEAR เพื่อให้ monthly filter ทุกปีมีข้อมูล
        start_date_str = f'{START_YEAR}-01-01'
        existing_data = {}
    else:
        # รอบถัดไป: ดึงแค่ 30 วันล่าสุด เพื่อรับข้อมูลที่อาจแก้ไขย้อนหลัง
        start_date_obj = today - timedelta(days=30)
        start_date_str = start_date_obj.strftime('%Y-%m-%d')
        # รีเซ็ตค่า 30 วันล่าสุดใน existing_data เพื่อรับข้อมูลใหม่ที่อัปเดต
        curr = start_date_obj
        while curr <= today:
            d_str = curr.strftime('%Y-%m-%d')
            existing_data[d_str] = {}   # คืนค่าเป็น dict ว่างรอรับข้อมูลใหม่
            curr += timedelta(days=1)

    try:
        with SessionHOS() as db_hos:
            # ----------------------------------------------------------
            # SQL Query หลัก
            # อ้างอิงตาราง: incoth, pttype, paidst
            # ----------------------------------------------------------
            sql = text("""
                SELECT
                    i.billdate                                          AS bill_date,
                    t.pttype                                            AS pttype_code,
                    t.name                                              AS pttype_name,
                    COUNT(DISTINCT i.hn)                                AS total_patients,
                    COUNT(DISTINCT i.vn)                                AS total_visits,

                    -- ยอดเงินสด: paidst 1 (จ่ายเอง เบิกได้) หรือ 3 (จ่ายเอง ไม่ได้เบิก)
                    SUM(CASE WHEN i.paidst IN (1, 3) THEN i.rcptamt ELSE 0 END) AS cash_amount,

                    -- ยอดลูกหนี้: paidst 2 (ใช้สิทธิ์เบิก เช่น ประกันสังคม, บัตรทอง)
                    SUM(CASE WHEN i.paidst = 2     THEN i.rcptamt ELSE 0 END) AS debtor_amount,

                    -- ยอดค้างชำระ: paidst 0 (ยังไม่ชำระเงิน)
                    SUM(CASE WHEN i.paidst = 0     THEN i.rcptamt ELSE 0 END) AS unpaid_amount,

                    -- ยอดรวมทั้งหมดของใบเสร็จ
                    SUM(i.rcptamt)                                      AS total_amount

                FROM incoth i
                LEFT JOIN pttype t  ON i.pttype  = t.pttype
                LEFT JOIN paidst p  ON i.paidst  = p.paidst
                WHERE i.billdate >= :start_date
                  AND i.billdate <= CURDATE()
                GROUP BY i.billdate, t.pttype, t.name
                ORDER BY i.billdate ASC, t.pttype ASC
            """)
            rows = db_hos.execute(sql, {"start_date": start_date_str}).fetchall()

            # --- บันทึกลง existing_data รายวัน + รายสิทธิ์ ---
            for r in rows:
                if r[0] is None:
                    continue
                d_str = str(r[0])   # "YYYY-MM-DD"
                ptcode = str(r[1]) if r[1] is not None else "UNKNOWN"

                if d_str not in existing_data:
                    existing_data[d_str] = {}

                existing_data[d_str][ptcode] = {
                    "pttype_name":    r[2] or "ไม่ระบุ",
                    "total_patients": int(r[3] or 0),
                    "total_visits":   int(r[4] or 0),
                    "cash_amount":    float(r[5] or 0),
                    "debtor_amount":  float(r[6] or 0),
                    "unpaid_amount":  float(r[7] or 0),
                    "total_amount":   float(r[8] or 0),
                }

    except Exception as e:
        print(f"[Cache Finance] HOSxP Query Error: {e}")

    # ----------------------------------------------------------
    # ประมวลผล: แปลง existing_data → by_pttype, monthly, daily, kpi
    # ----------------------------------------------------------
    pttype_agg: dict = {}   # {pttype_code: {...sum fields}}
    yearly_agg: dict = {}   # {yr: {...sum fields}}
    monthly_agg: dict = {}  # {(yr, mo): {...sum fields}}
    daily_list: list = []
    kpi = {
        "today_total":  0.0,
        "today_cash":   0.0,
        "today_debtor": 0.0,
        "month_total":  0.0,
        "year_total":   0.0,
    }
    current_month = today.strftime('%Y-%m')
    current_year  = today.strftime('%Y')
    date_365_days_ago = (today - timedelta(days=365)).strftime('%Y-%m-%d')

    for d_str in sorted(existing_data.keys()):
        day_data = existing_data[d_str]
        if not day_data:
            continue  # วันที่ไม่มีข้อมูล

        yr = d_str[:4]
        mo = d_str[5:7]

        # --- รวมยอดรายวัน (สำหรับ daily view) ---
        day_total     = sum(v["total_amount"]   for v in day_data.values())
        day_cash      = sum(v["cash_amount"]    for v in day_data.values())
        day_debtor    = sum(v["debtor_amount"]  for v in day_data.values())
        day_unpaid    = sum(v["unpaid_amount"]  for v in day_data.values())
        day_patients  = sum(v["total_patients"] for v in day_data.values())
        day_visits    = sum(v["total_visits"]   for v in day_data.values())

        daily_list.append({
            "date":           d_str,
            "total_amount":   day_total,
            "cash_amount":    day_cash,
            "debtor_amount":  day_debtor,
            "unpaid_amount":  day_unpaid,
            "total_patients": day_patients,
            "total_visits":   day_visits,
        })

        # --- KPI ---
        if d_str == str(today):
            kpi["today_total"]  = day_total
            kpi["today_cash"]   = day_cash
            kpi["today_debtor"] = day_debtor
        if d_str.startswith(current_month):
            kpi["month_total"] += day_total
        if d_str.startswith(current_year):
            kpi["year_total"]  += day_total

        # --- รวม by_pttype (เฉพาะ 365 วันล่าสุด) ---
        if d_str >= date_365_days_ago:
            for ptcode, vals in day_data.items():
                if ptcode not in pttype_agg:
                    pttype_agg[ptcode] = {
                        "pttype_code":    ptcode,
                        "pttype_name":    vals["pttype_name"],
                        "total_patients": 0,
                        "total_visits":   0,
                        "cash_amount":    0.0,
                        "debtor_amount":  0.0,
                        "unpaid_amount":  0.0,
                        "total_amount":   0.0,
                    }
                pttype_agg[ptcode]["total_patients"] += vals["total_patients"]
                pttype_agg[ptcode]["total_visits"]   += vals["total_visits"]
                pttype_agg[ptcode]["cash_amount"]    += vals["cash_amount"]
                pttype_agg[ptcode]["debtor_amount"]  += vals["debtor_amount"]
                pttype_agg[ptcode]["unpaid_amount"]  += vals["unpaid_amount"]
                pttype_agg[ptcode]["total_amount"]   += vals["total_amount"]

        # --- รวม monthly ---
        key = (yr, mo)
        if key not in monthly_agg:
            monthly_agg[key] = {
                "year": yr, "month": mo,
                "total_patients": 0, "total_visits": 0,
                "cash_amount": 0.0, "debtor_amount": 0.0,
                "unpaid_amount": 0.0, "total_amount": 0.0,
            }
        monthly_agg[key]["total_patients"] += day_patients
        monthly_agg[key]["total_visits"]   += day_visits
        monthly_agg[key]["cash_amount"]    += day_cash
        monthly_agg[key]["debtor_amount"]  += day_debtor
        monthly_agg[key]["unpaid_amount"]  += day_unpaid
        monthly_agg[key]["total_amount"]   += day_total

        # --- รวม yearly ---
        if yr not in yearly_agg:
            yearly_agg[yr] = {
                "year": yr,
                "total_patients": 0, "total_visits": 0,
                "cash_amount": 0.0, "debtor_amount": 0.0,
                "unpaid_amount": 0.0, "total_amount": 0.0,
            }
        yearly_agg[yr]["total_patients"] += day_patients
        yearly_agg[yr]["total_visits"]   += day_visits
        yearly_agg[yr]["cash_amount"]    += day_cash
        yearly_agg[yr]["debtor_amount"]  += day_debtor
        yearly_agg[yr]["unpaid_amount"]  += day_unpaid
        yearly_agg[yr]["total_amount"]   += day_total

    # แปลงเป็น list เรียงตาม pttype_code
    by_pttype_list = sorted(pttype_agg.values(), key=lambda x: x["pttype_code"])
    yearly_list    = [v for _, v in sorted(yearly_agg.items())]
    monthly_list   = [v for _, v in sorted(monthly_agg.items())]

    return by_pttype_list, yearly_list, monthly_list, daily_list, kpi, existing_data


# ==========================================================
# Smart Scheduler: คำนวณเวลา sleep ถัดไปตาม Business Hours
# ==========================================================
def _get_sleep_seconds() -> int:
    """
    คำนวณระยะเวลาพักก่อน refresh ครั้งถัดไป

    ตรรกะ:
      - 08:00–15:59 น. (ช่วง OPD) → sleep 1 ชั่วโมง = 3,600 วินาที
      - นอกเวลา (ก่อน 08:00 / หลัง 16:00) → sleep 3 ชั่วโมง = 10,800 วินาที

    Returns:
        int: จำนวนวินาทีที่ควร sleep
    """
    now_hour = datetime.now().hour
    if 8 <= now_hour < 16:
        return 3_600    # 1 ชั่วโมง
    return 10_800       # 3 ชั่วโมง


# ==========================================================
# Async Task: loop หลักที่รัน background
# ==========================================================
async def task_update_finance():
    """
    Background Task สำหรับ Finance Cache (Incremental + Smart Schedule)

    วิธีทำงาน:
      1. ดึง raw_full จาก Redis (ถ้ายังไม่มีให้เริ่มจาก {})
      2. รัน fetch_finance_incremental_sync() ใน thread pool (ไม่บล็อก event loop)
      3. บันทึกผลลัพธ์ทั้งหมดกลับลง Redis ด้วย pipeline (atomic)
      4. sleep ตาม _get_sleep_seconds() แล้วกลับข้อ 1
    """
    print("[Task Finance] เริ่มทำงาน (Incremental + Smart Schedule)...")
    while True:
        try:
            # 1. โหลดข้อมูลดิบที่มีอยู่ใน Redis
            raw_full_str = await redis_client.get(KEY_FINANCE_RAW_FULL)
            existing_data = json.loads(raw_full_str) if raw_full_str else {}

            # 2. ดึงข้อมูลใหม่จาก DB (run ใน thread pool ไม่บล็อก asyncio)
            by_pttype, yearly, monthly, daily, kpi, full_data = await asyncio.to_thread(
                fetch_finance_incremental_sync, existing_data
            )

            # 3. บันทึกกลับลง Redis ด้วย pipeline
            pipe = redis_client.pipeline()
            pipe.set(KEY_FINANCE_RAW_FULL,  json.dumps(full_data,   ensure_ascii=False))
            if by_pttype:
                pipe.set(KEY_FINANCE_BY_PTTYPE, json.dumps(by_pttype,  ensure_ascii=False))
            if yearly:
                pipe.set(KEY_FINANCE_YEARLY,    json.dumps(yearly,     ensure_ascii=False))
            if monthly:
                pipe.set(KEY_FINANCE_MONTHLY,   json.dumps(monthly,    ensure_ascii=False))
            if daily:
                pipe.set(KEY_FINANCE_DAILY,     json.dumps(daily,      ensure_ascii=False))
            pipe.set(KEY_FINANCE_KPI, json.dumps(kpi, ensure_ascii=False))
            await pipe.execute()

            sleep_sec = _get_sleep_seconds()
            schedule_label = "1 ชม." if sleep_sec == 3_600 else "3 ชม."
            print(
                f"[Task Finance] อัปเดตสำเร็จ: "
                f"{len(by_pttype)} สิทธิ์, {len(monthly)} เดือน, "
                f"{len(daily)} วัน — refresh ถัดไปใน {schedule_label}"
            )

        except Exception as e:
            print(f"[Task Finance] Loop Error: {e}")

        # 4. sleep ตาม Business Hours
        await asyncio.sleep(_get_sleep_seconds())


# ==========================================================
# Getter Functions: ให้ Router เรียกใช้ (อ่านจาก Cache เท่านั้น)
# ==========================================================
async def get_finance_data(
    view: str = "by_pttype",
    month: str = None,
    year: str = None,
):
    """
    ดึงข้อมูล Finance จาก Redis Cache

    Args:
        view (str): รูปแบบข้อมูลที่ต้องการ
                    - "by_pttype" → สรุปยอดแยกตามประเภทสิทธิ์ผู้ป่วย (ตลอด 1 ปี)
                    - "monthly"   → สรุปรายเดือน (กรองด้วย ?year=YYYY ได้)
                    - "daily"     → สรุปรายวัน 90 วัน (กรองด้วย ?month=YYYY-MM ได้)
                    - "kpi"       → ยอดสรุป: วันนี้ / เดือนนี้ / ปีนี้
        month (str): กรองข้อมูล daily เช่น "2025-04"
        year  (str): กรองข้อมูล monthly เช่น "2025"

    Returns:
        dict | list: ข้อมูลตาม view ที่เลือก
    """
    if view == "yearly":
        raw = await redis_client.get(KEY_FINANCE_YEARLY)
        if not raw:
            return []
        data = json.loads(raw)
        # กรองตามปีถ้าระบุ (แม้ว่าจะเป็น yearly อยู่แล้ว แต่วางเผื่อไว้)
        if year:
            data = [r for r in data if r["year"] == year]
        return data

    elif view == "monthly":
        raw = await redis_client.get(KEY_FINANCE_MONTHLY)
        if not raw:
            return []
        data = json.loads(raw)
        # กรองตามปีถ้าระบุ
        if year:
            data = [r for r in data if r["year"] == year]
        return data

    elif view == "daily":
        raw = await redis_client.get(KEY_FINANCE_DAILY)
        if not raw:
            return []
        data = json.loads(raw)
        # กรองตามเดือนถ้าระบุ เช่น "2025-04"
        if month:
            data = [r for r in data if r["date"].startswith(month)]
        return data

    elif view == "kpi":
        raw = await redis_client.get(KEY_FINANCE_KPI)
        return json.loads(raw) if raw else {
            "today_total": 0.0,
            "today_cash": 0.0,
            "today_debtor": 0.0,
            "month_total": 0.0,
            "year_total": 0.0,
        }

    else:  # by_pttype (default)
        raw = await redis_client.get(KEY_FINANCE_BY_PTTYPE)
        if not raw:
            return []
        data = json.loads(raw)
        return data
