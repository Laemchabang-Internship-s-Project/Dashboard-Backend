import json
from datetime import date, timedelta
from fastapi import APIRouter, Query, HTTPException, Request, Depends
from sqlalchemy import text
from database_hos import SessionLocal as SessionHOS
from cache_manager import redis_client
from rate_limiter import limiter
from utils.security import get_api_key

router = APIRouter()

MAX_DATE_RANGE_DAYS = 400

@router.get("/api/dashboard/summary-range")
@limiter.limit("30/minute")
async def get_summary_range(
    request: Request,
    api_key: str = Depends(get_api_key),
    start_date: date = Query(..., description="วันที่เริ่มต้น (YYYY-MM-DD)"),
    end_date: date = Query(..., description="วันที่สิ้นสุด (YYYY-MM-DD)")
):
    if end_date < start_date:
        raise HTTPException(status_code=400, detail="end_date ต้องไม่น้อยกว่า start_date")

    range_days = (end_date - start_date).days
    if range_days > MAX_DATE_RANGE_DAYS:
        raise HTTPException(
            status_code=400,
            detail=f"ช่วงวันที่สูงสุดคือ {MAX_DATE_RANGE_DAYS} วัน (ขอมา {range_days} วัน)"
        )

    if end_date > date.today():
        raise HTTPException(status_code=400, detail="end_date ต้องไม่เกินวันปัจจุบัน")

    cache_key = f"summary:range:{start_date}:{end_date}"

    try:
        cached_data = await redis_client.get(cache_key)
        if cached_data:
            return json.loads(cached_data)

        with SessionHOS() as db:
            # ใช้ Query เดียวดึงค่าสรุปส่งยา เพื่อให้ Logic การนับเป็นไปในทิศทางเดียวกัน
            sql = text("""
                SELECT 
                    -- ส่วนของ OPD ปกติ
                    COUNT(v.vn) as total_opd,
                    SUM(CASE WHEN v.ovstist IN ('01', '06') THEN 1 ELSE 0 END) as walk_in,
                    SUM(CASE WHEN v.ovstist = '05' THEN 1 ELSE 0 END) as telemed,
                    
                    -- ส่วนของส่งยา (ดึงจาก Subquery ที่สรุปมาแล้ว)
                    delivery.total_all,
                    delivery.total_postal,
                    delivery.total_rider
                FROM ovst v
                LEFT JOIN (
                    SELECT 
                        -- นับรวมโดยเอา Postal + Rider จริงๆ
                        COUNT(DISTINCT CASE WHEN icode IN ('3907018', '3907508', '3907489') THEN vn END) as total_all,
                        COUNT(DISTINCT CASE WHEN icode IN ('3907018', '3907508') THEN vn END) as total_postal,
                        COUNT(DISTINCT CASE WHEN icode = '3907489' THEN vn END) as total_rider
                    FROM opitemrece
                    WHERE vstdate BETWEEN :start AND :end
                    AND icode IN ('3907018', '3907508', '3907489')
                ) AS delivery ON 1=1
                WHERE v.vstdate BETWEEN :start AND :end
                GROUP BY delivery.total_all, delivery.total_postal, delivery.total_rider
            """)

            res = db.execute(sql, {"start": start_date, "end": end_date}).fetchone()
            postal_val = int(res[4] or 0)
            rider_val  = int(res[5] or 0)
            result = {
                "period": {"start": str(start_date), "end": str(end_date)},
                "range_days": range_days,
                "data": {
                    "opd_total":     int(res[0] or 0),
                    "walk_in":       int(res[1] or 0),
                    "telemed":       int(res[2] or 0),
                    "drug_delivery": postal_val + rider_val,
                    "total_drug_delivery_postal": postal_val,
                    "total_drug_delivery_rider": rider_val
                },
                "source": "database"
            }

            await redis_client.set(cache_key, json.dumps(result), ex=300)
            return result

    except HTTPException:
        raise  # แก้ 4: ไม่กิน 400/403 ให้กลายเป็น 500
    except Exception as e:
        print(f"[Summary Range] Error: {e}")
        raise HTTPException(status_code=500, detail="ไม่สามารถดึงข้อมูลสรุปช่วงเวลาได้")

@router.get("/api/dashboard/opd-dept-range")
@limiter.limit("30/minute")
async def get_opd_dept_range(
    request: Request,
    api_key: str = Depends(get_api_key),
    start_date: date = Query(...),
    end_date: date   = Query(None),
):
    if end_date is None:
        end_date = start_date

    if end_date < start_date:
        raise HTTPException(status_code=400, detail="end_date ต้องไม่น้อยกว่า start_date")
    if (end_date - start_date).days > MAX_DATE_RANGE_DAYS:
        raise HTTPException(status_code=400, detail=f"ช่วงสูงสุด {MAX_DATE_RANGE_DAYS} วัน")
    if end_date > date.today():
        raise HTTPException(status_code=400, detail="end_date ต้องไม่เกินวันนี้")

    cache_key = f"opd:dept:{start_date}:{end_date}"
    cached = await redis_client.get(cache_key)
    if cached:
        return json.loads(cached)

    try:
        with SessionHOS() as db:
            # ดึงข้อมูลเฉพาะรายแผนกและ State คนไข้ตามปกติ
            res = db.execute(text("""
                SELECT
                    v.main_dep,
                    COUNT(v.vn)                                                        AS total_opd,
                    SUM(CASE WHEN v.cur_dep IN ('010','062','105','066','077','901','902') THEN 1 ELSE 0 END) AS waiting_screening,
                    SUM(CASE WHEN v.cur_dep IN ('023','047','059','069','076','046','074','903','905') THEN 1 ELSE 0 END) AS waiting_exam,
                    SUM(CASE WHEN v.cur_dep = '007'         THEN 1 ELSE 0 END)         AS waiting_lab,
                    SUM(CASE WHEN v.cur_dep IN ('012','112') THEN 1 ELSE 0 END)        AS waiting_xray,
                    SUM(CASE WHEN v.cur_dep IN ('016','053','135') THEN 1 ELSE 0 END)   AS waiting_payment,
                    SUM(CASE WHEN v.cur_dep IN ('030','014','904') THEN 1 ELSE 0 END)   AS waiting_drug,
                    SUM(CASE WHEN v.cur_dep = '999'         THEN 1 ELSE 0 END)         AS go_home,
                    SUM(CASE WHEN v.cur_dep NOT IN (
                        '010','062','105','066','077','901','902',
                        '023','047','059','069','076','046','074','903','905',
                        '007','012','112','016','053','135','030','014','904','999'
                    ) THEN 1 ELSE 0 END)                                               AS other,

                    ROUND(AVG(CASE WHEN s.service4 > s.service3 AND s.service7 IS NOT NULL AND (s.service7 <= '16:00:00' OR v.main_dep = '011') THEN GREATEST((TIME_TO_SEC(s.service7) - TIME_TO_SEC(s.service3)) / 60.0, 0) END), 1) AS avg_total,
                    ROUND(AVG(CASE WHEN s.service4 > s.service3 AND s.service4 IS NOT NULL AND (s.service7 IS NULL OR s.service7 <= '16:00:00' OR v.main_dep = '011') THEN GREATEST((TIME_TO_SEC(s.service4) - TIME_TO_SEC(s.service3)) / 60.0, 0) END), 1) AS avg_wait_screening,
                    ROUND(AVG(CASE WHEN s.service4 > s.service3 AND s.service5 IS NOT NULL AND s.service11 IS NOT NULL AND (s.service7 IS NULL OR s.service7 <= '16:00:00' OR v.main_dep = '011') THEN GREATEST((TIME_TO_SEC(s.service5) - TIME_TO_SEC(s.service11)) / 60.0, 0) END), 1) AS avg_wait_exam,
                    ROUND(AVG(CASE WHEN s.service4 > s.service3 AND s.service16 IS NOT NULL AND (s.service7 IS NULL OR s.service7 <= '16:00:00' OR v.main_dep = '011') THEN GREATEST((TIME_TO_SEC(s.service16) - TIME_TO_SEC(IFNULL(s.service6, s.service12))) / 60.0, 0) END), 1) AS avg_wait_drug
                FROM ovst v
                LEFT JOIN service_time s ON v.vn = s.vn AND s.vstdate = v.vstdate
                WHERE v.vstdate BETWEEN :start AND :end
                  AND v.main_dep IN (
                      '010','062','108','109','110','111','011','075','044','033','072','063','005','042','041','074','901','902','903','904','905'
                  )
                GROUP BY v.main_dep
                ORDER BY v.main_dep
            """), {"start": start_date, "end": end_date}).fetchall()

        def _i(v): return int(v or 0)
        def _f(v): return round(float(v or 0), 1)

        departments = []
        for row in res:
            departments.append({
                "dept_code":          row[0],
                "total_opd":          _i(row[1]),
                "waiting_screening":  _i(row[2]),
                "waiting_exam":       _i(row[3]),
                "waiting_lab":        _i(row[4]),
                "waiting_xray":       _i(row[5]),
                "waiting_payment":    _i(row[6]),
                "waiting_drug":       _i(row[7]),
                "go_home":            _i(row[8]),
                "other":              _i(row[9]),
                "avg_wait_total":     _f(row[10]),
                "avg_wait_screening": _f(row[11]),
                "avg_wait_exam":      _f(row[12]),
                "avg_wait_drug":      _f(row[13]),
            })

        # ตัดการ Query ข้อมูลเทคนิคการแพทย์ออก และส่งกลับเป็น null เพื่อไม่ให้ระบบเกิด Error 
        result = {
            "period":      {"start": str(start_date), "end": str(end_date)},
            "range_days":  (end_date - start_date).days,
            "departments": departments,
            "technical_services": None,  # กำหนดเป็น None ชัดเจน
            "source":      "hosxp"
        }

        ttl = 300 if end_date == date.today() else 3600
        await redis_client.set(cache_key, json.dumps(result), ex=ttl)
        return result

    except HTTPException:
        raise
    except Exception as e:
        print(f"[OPD Dept Range] Error: {e}")
        raise HTTPException(status_code=500, detail="ไม่สามารถดึงข้อมูลได้")