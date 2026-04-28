from fastapi import APIRouter, Query
from typing import Optional
from cache_manager import get_graph_data, get_dental_data

router = APIRouter(prefix="/api/graph", tags=["Graph Data"])

@router.get("/doctor-operations")
async def get_doctor_operations_graph(
    view:  str           = Query("daily",  description="daily | monthly | yoy"),
    month: Optional[str] = Query(None,     description="กรอง daily ตามเดือน เช่น '2025-04'"),
    year:  Optional[str] = Query(None,     description="กรอง monthly ตามปี เช่น '2025'"),
):
    """
    ดึงข้อมูลกราฟการผ่าตัดของแพทย์ (อัปเดตสัปดาห์ละ 1 ครั้ง)

    - **view=daily**   → raw daily rows (90 วันล่าสุด) กรอง ?month=YYYY-MM ได้
    - **view=monthly** → aggregate รายเดือน กรอง ?year=YYYY ได้
    - **view=yoy**     → aggregate รายปี-เดือน (ทุกปี)
    """
    data = await get_graph_data(view=view, month=month, year=year)
    return {"status": "success", "view": view, "data": data}


@router.get("/dental-summary")
async def get_dental_summary_graph(
    view:  str           = Query("daily",  description="daily | monthly | yoy | meta"),
    month: Optional[str] = Query(None,     description="กรอง daily ตามเดือน เช่น '2025-04'"),
    year:  Optional[str] = Query(None,     description="กรอง monthly ตามปี เช่น '2025'"),
):
    """
    ดึงข้อมูลกราฟแผนกทันตกรรม (อัปเดตสัปดาห์ละ 1 ครั้ง)

    - **view=daily**   → raw daily rows (90 วันล่าสุด) กรอง ?month=YYYY-MM ได้
    - **view=monthly** → aggregate รายเดือน กรอง ?year=YYYY ได้
    - **view=yoy**     → aggregate รายปี-เดือน (ทุกปี)
    - **view=meta**    → latest row เดียว (สำหรับ KPI cards)
    """
    data = await get_dental_data(view=view, month=month, year=year)
    return {"status": "success", "view": view, "data": data}