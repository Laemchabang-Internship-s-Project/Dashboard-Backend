from fastapi import APIRouter, Query, Depends, HTTPException, Request
from datetime import date
from typing import Optional

from cache_referout import get_referout_data, fetch_referout_cases_by_severity_sync
from utils.security import get_api_key
from routers.auth import get_current_user
from rate_limiter import limiter
import asyncio

router = APIRouter(prefix="/api/referout", tags=["Referout Data"])

MAX_DATE_RANGE_DAYS = 400


@router.get("/summary", dependencies=[Depends(get_api_key)])
@limiter.limit("30/minute")
async def get_referout_summary(
    request: Request,
    view: str = Query("today", description="รูปแบบข้อมูล: today | range"),
    start_date: Optional[date] = Query(
        None, description="วันที่เริ่มต้น (YYYY-MM-DD) ใช้เมื่อ view=range"
    ),
    end_date: Optional[date] = Query(
        None, description="วันที่สิ้นสุด (YYYY-MM-DD) ใช้เมื่อ view=range"
    ),
    _user: dict = Depends(get_current_user),
):
    if view == "range":
        if not start_date or not end_date:
            raise HTTPException(
                status_code=400,
                detail="กรุณาระบุทั้ง start_date และ end_date เมื่อเลือก view=range",
            )

        if end_date < start_date:
            raise HTTPException(
                status_code=400, detail="end_date ต้องไม่น้อยกว่า start_date"
            )

        range_days = (end_date - start_date).days
        if range_days > MAX_DATE_RANGE_DAYS:
            raise HTTPException(
                status_code=400,
                detail=f"ช่วงวันที่สูงสุดห้ามเกิน {MAX_DATE_RANGE_DAYS} วัน (ที่ขอมาคือ {range_days} วัน)",
            )

        if end_date > date.today():
            raise HTTPException(status_code=400, detail="end_date ต้องไม่เกินวันปัจจุบัน")

        data = await get_referout_data(
            view="range", start_date=str(start_date), end_date=str(end_date)
        )
        return {
            "status": "success",
            "view": "range",
            "period": {"start": str(start_date), "end": str(end_date)},
            "data": data,
        }
    data = await get_referout_data(view="today")
    return {"status": "success", "view": "today", "data": data}


@router.get("/cases", dependencies=[Depends(get_api_key)])
@limiter.limit("30/minute")
async def get_referout_cases_detail(
    request: Request,
    severity_id: int = Query(
        ..., description="ID ความรุนแรง (1=Life Threatening, 2=Emergency, etc.)"
    ),
    start_date: date = Query(..., description="วันที่เริ่มต้น (YYYY-MM-DD)"),
    end_date: date = Query(..., description="วันที่สิ้นสุด (YYYY-MM-DD)"),
    _user: dict = Depends(get_current_user),
):
    # ตรวจสอบความถูกต้องของเงื่อนไขวันที่ (ใช้ Logic เดียวกันกับที่มีอยู่เดิมในไฟล์)
    if end_date < start_date:
        raise HTTPException(status_code=400, detail="end_date ต้องไม่น้อยกว่า start_date")

    range_days = (end_date - start_date).days
    if range_days > MAX_DATE_RANGE_DAYS:
        raise HTTPException(
            status_code=400,
            detail=f"ช่วงวันที่สูงสุดห้ามเกิน {MAX_DATE_RANGE_DAYS} วัน",
        )

    if end_date > date.today():
        raise HTTPException(status_code=400, detail="end_date ต้องไม่เกินวันปัจจุบัน")

    # เรียกฟังก์ชันใน cache_referout ผ่าน asyncio.to_thread เพื่อไม่ให้บล็อก Event Loop
    cases_data = await asyncio.to_thread(
        fetch_referout_cases_by_severity_sync,
        severity_id,
        str(start_date),
        str(end_date),
    )

    return {
        "status": "success",
        "severity_id": severity_id,
        "period": {"start": str(start_date), "end": str(end_date)},
        "data": cases_data,
    }
