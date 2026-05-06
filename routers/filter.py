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
            sql = text("""
                SELECT 
                    COUNT(vn) as total_opd,
                    SUM(CASE WHEN ovstist IN ('01', '06') THEN 1 ELSE 0 END) as walk_in,
                    SUM(CASE WHEN ovstist = '05' THEN 1 ELSE 0 END) as telemed,
                    
                    -- 1. ยอดรวมจัดส่งยา (นับจำนวนออเดอร์ทั้งหมด)
                    (SELECT COUNT(o.vn) 
                     FROM opitemrece o 
                     WHERE o.vstdate BETWEEN :start AND :end 
                     AND o.icode IN ('3907489', '3907018', '3907508')
                    ) as drug_delivery,
                    
                    -- 2. ยอดไปรษณีย์
                    (SELECT COUNT(o.vn) 
                     FROM opitemrece o 
                     WHERE o.vstdate BETWEEN :start AND :end 
                     AND o.icode IN ('3907018', '3907508')
                    ) as drug_delivery_postal,
                    
                    -- 3. ยอด Rider
                    (SELECT COUNT(o.vn) 
                     FROM opitemrece o 
                     WHERE o.vstdate BETWEEN :start AND :end 
                     AND o.icode = '3907489'
                    ) as drug_delivery_rider
                    
                FROM ovst 
                WHERE vstdate BETWEEN :start AND :end
            """)

            res = db.execute(sql, {"start": start_date, "end": end_date}).fetchone()

            result = {
                "period": {"start": str(start_date), "end": str(end_date)},
                "range_days": range_days,
                "data": {
                    "opd_total":     int(res[0] or 0),
                    "walk_in":       int(res[1] or 0),
                    "telemed":       int(res[2] or 0),
                    "drug_delivery": int(res[3] or 0),
                    "total_drug_delivery_postal": int(res[4] or 0),
                    "total_drug_delivery_rider": int(res[5] or 0)
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