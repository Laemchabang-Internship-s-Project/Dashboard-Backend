from fastapi import APIRouter, HTTPException

from cache_manager import get_cached_data

router = APIRouter(prefix="/api/fuel", tags=["Google Forms"])


@router.get("/latest")
def get_latest_fuel():
    """ดึงข้อมูลน้ำมันรถล่าสุด (อ่านจาก Redis Cache แทนยิง Google Sheets ตรง)"""
    data = get_cached_data("car")
    if not data or not data.get("fuel_latest"):
        raise HTTPException(status_code=404, detail="ไม่มีข้อมูลน้ำมันรถ")
    return data["fuel_latest"]
