import json
from datetime import date
from fastapi import APIRouter, Query, HTTPException, Request
from sqlalchemy import text
from database_hos import SessionLocal as SessionHOS
from cache_manager import redis_client # นำเข้า redis_client จากไฟล์ที่คุณมีอยู่แล้ว
from rate_limiter import limiter

router = APIRouter()

@router.get("/api/dashboard/summary-range")
@limiter.limit("20/minute")
async def get_summary_range(
    request: Request,
    start_date: date = Query(..., description="วันที่เริ่มต้น (YYYY-MM-DD)"),
    end_date: date = Query(..., description="วันที่สิ้นสุด (YYYY-MM-DD)")
):
    # 1. สร้าง Cache Key โดยใช้ช่วงวันที่เป็นชื่อ key
    # ตัวอย่าง: summary:range:2026-04-01:2026-04-16
    cache_key = f"summary:range:{start_date}:{end_date}"
    
    try:
        # 2. พยายามดึงข้อมูลจาก Redis ก่อน (ป้องกัน DB ทำงานหนักถ้ามีคนเรียกซ้ำ)
        cached_data = await redis_client.get(cache_key)
        if cached_data:
            return json.loads(cached_data)

        # 3. ถ้าไม่มีใน Cache ให้ Query จาก Database HOSxP
        with SessionHOS() as db:
            sql = text("""
                SELECT 
                    -- OPD Total: นับทุก VN ในช่วงวันที่
                    COUNT(vn) as total_opd,
                    
                    -- Walk-in: กรองตาม ovstist '01' และ '06' (ตาม logic เดิมใน cache_manager)
                    SUM(CASE WHEN ovstist IN ('01', '06') THEN 1 ELSE 0 END) as walk_in,
                    
                    -- Telemedicine: กรองตาม ovstist '05'
                    SUM(CASE WHEN ovstist = '05' THEN 1 ELSE 0 END) as telemed,
                    
                    -- Drug Delivery: นับรายตัว (Distinct VN) จาก opitemrece ตาม icode ที่กำหนด
                    (SELECT COUNT(DISTINCT o.vn) 
                     FROM opitemrece o 
                     WHERE o.vstdate BETWEEN :start AND :end 
                     AND o.icode IN ('3907489', '3907018', '3907508')
                    ) as drug_delivery
                    
                FROM ovst 
                WHERE vstdate BETWEEN :start AND :end
            """)
            
            res = db.execute(sql, {"start": start_date, "end": end_date}).fetchone()
            
            # จัดรูปแบบข้อมูล
            result = {
                "period": {"start": str(start_date), "end": str(end_date)},
                "data": {
                    "opd_total": int(res[0] or 0),
                    "walk_in": int(res[1] or 0),
                    "telemed": int(res[2] or 0),
                    "drug_delivery": int(res[3] or 0)
                },
                "source": "database" # ระบุเพื่อให้รู้ว่าดึงจาก DB หรือ Cache
            }

            # 4. บันทึกลง Redis พร้อมตั้งเวลา Expire (เช่น 300 วินาที หรือ 5 นาที)
            # เพื่อให้คนถัดไปที่ขอช่วงเวลาเดียวกันไม่ต้องรอ Query SQL
            await redis_client.set(cache_key, json.dumps(result), ex=300)
            
            result["source"] = "cache" # ปรับสถานะสำหรับรอบหน้า
            return result

    except Exception as e:
        print(f"Error fetching summary range: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")