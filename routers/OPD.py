from fastapi import APIRouter, HTTPException

from cache_manager import get_cached_data

router = APIRouter(
    prefix="/OPD",
    tags=["opd"]
)


@router.get("/total-patients")
def get_total_patients():
    """จำนวนผู้รับบริการทั้งหมดวันนี้ (อ่านจาก Redis Cache)"""
    data = get_cached_data("system")
    if not data:
        raise HTTPException(status_code=503, detail="ข้อมูลยังไม่พร้อม")
    return {"total_patients": data.get("today_total_services", 0)}


@router.get("/summary")
def get_opd_summary():
    """ดึงข้อมูลสรุปภาพรวม OPD ทั้งหมดในครั้งเดียว (อ่านจาก Redis Cache)"""
    data = get_cached_data("opd_clinics")
    if not data:
        raise HTTPException(status_code=503, detail="ข้อมูลยังไม่พร้อม")
    return data
