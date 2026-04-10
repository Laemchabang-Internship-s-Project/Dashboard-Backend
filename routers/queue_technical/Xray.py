from fastapi import APIRouter, HTTPException

from cache_manager import get_cached_data

router = APIRouter(
    tags=["Technical Services"]
)


@router.get("/summary")
def get_xray_summary():
    """ดึงข้อมูลสรุปคิว X-ray วันนี้ (อ่านจาก Redis Cache)"""
    data = get_cached_data("technical_services")
    if not data:
        raise HTTPException(status_code=503, detail="ข้อมูลยังไม่พร้อม กรุณารอสักครู่")
    return data.get("xray", {"all": 0, "waiting": 0, "finished": 0})