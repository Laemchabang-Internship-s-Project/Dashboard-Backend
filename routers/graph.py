from fastapi import APIRouter, Query, Depends
from typing import Optional
from cache_graph import get_graph_data, get_dental_data, get_death_data, get_depression_data, get_daily_operations_drilldown
from utils.security import get_api_key

router = APIRouter(prefix="/api/graph", tags=["Graph Data"])

@router.get("/doctor-operations", dependencies=[Depends(get_api_key)])
async def get_doctor_operations_graph(
    view:  str           = Query("daily",  description="daily | monthly | yoy | operations | doctors | departments | drilldown"),
    month: Optional[str] = Query(None,     description="กรอง daily ตามเดือน เช่น '2025-04'"),
    year:  Optional[str] = Query(None,     description="กรอง monthly ตามปี เช่น '2025'"),
    doctor_name: Optional[str] = Query(None, description="ชื่อแพทย์สำหรับ view=drilldown"),
):
    """
    ดึงข้อมูลกราฟการผ่าตัดของแพทย์ (อัปเดตสัปดาห์ละ 1 ครั้ง)

    - **view=daily**       → raw daily rows (90 วันล่าสุด) กรอง ?month=YYYY-MM ได้
    - **view=monthly**     → aggregate รายเดือน กรอง ?year=YYYY ได้
    - **view=yoy**         → aggregate รายปี-เดือน (ทุกปี)
    - **view=operations**  → 10 อันดับหัตถการยอดฮิต กรอง ?year=YYYY หรือ ?month=YYYY-MM ได้
    - **view=doctors**     → 10 อันดับแพทย์ที่ทำหัตถการมากที่สุด กรอง ?year=YYYY หรือ ?month=YYYY-MM ได้
    - **view=departments** → 10 อันดับแผนกที่มีหัตถการมากที่สุด กรอง ?year=YYYY หรือ ?month=YYYY-MM ได้
    - **view=drilldown**   → 10 อันดับหัตถการของแพทย์ กรอง ?doctor_name=xxx&year=YYYY ได้
    """
    data = await get_graph_data(view=view.strip(), month=month, year=year, doctor_name=doctor_name)
    return {"status": "success", "view": view.strip(), "data": data}

@router.get("/doctor-operations/daily-drilldown", dependencies=[Depends(get_api_key)])
async def get_doctor_operations_daily_drilldown(
    date_str: str = Query(..., description="วันที่ต้องการดูลึก เช่น '2026-04-01'"),
):
    """
    ดึงข้อมูล 10 อันดับหัตถการสำหรับวันใดวันหนึ่ง
    """
    data = await get_daily_operations_drilldown(date_str)
    return {"status": "success", "date": date_str, "data": data}

@router.post("/trigger-cache-update")
async def trigger_cache_update():
    from cache_graph import fetch_doctor_operations_stats_sync, redis_client, KEY_GRAPH_STATS_OPER, KEY_GRAPH_STATS_DOC, KEY_GRAPH_STATS_DEPT, KEY_GRAPH_STATS_DRILLDOWN
    import json
    import asyncio
    stats_data = await asyncio.to_thread(fetch_doctor_operations_stats_sync)
    pipe = redis_client.pipeline()
    if stats_data:
        pipe.set(KEY_GRAPH_STATS_OPER, json.dumps(stats_data.get("operations", []), ensure_ascii=False))
        pipe.set(KEY_GRAPH_STATS_DOC,  json.dumps(stats_data.get("doctors", []), ensure_ascii=False))
        pipe.set(KEY_GRAPH_STATS_DEPT, json.dumps(stats_data.get("departments", []), ensure_ascii=False))
        pipe.set(KEY_GRAPH_STATS_DRILLDOWN, json.dumps(stats_data.get("drilldown", []), ensure_ascii=False))
    await pipe.execute()
    return {"status": "success", "message": "Cache updated!"}


@router.get("/dental-summary", dependencies=[Depends(get_api_key)])
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

@router.get("/death-summary", dependencies=[Depends(get_api_key)])
async def get_death_summary_graph(
    view: str = Query("causes", description="causes | monthly | places | hours"),
    month: Optional[str] = Query(None, description="กรองตามเดือน เช่น '2025-04'"),
    year:  Optional[str] = Query(None, description="กรองตามปี เช่น '2025'")
):
    """
    ดึงข้อมูลสถิติการเสียชีวิต
    """
    data = await get_death_data(view=view, month=month, year=year)
    return {"status": "success", "view": view, "data": data}

@router.get("/depression-summary", dependencies=[Depends(get_api_key)])
async def get_depression_summary_graph(
    view:  str           = Query("daily",  description="daily | monthly | yoy | status | kpi"),
    month: Optional[str] = Query(None,     description="กรอง daily ตามเดือน เช่น '2025-04'"),
    year:  Optional[str] = Query(None,     description="กรอง monthly ตามปี เช่น '2025'"),
):
    """
    ดึงข้อมูลกราฟผู้ป่วยจิตเวช (ซึมเศร้า) (อัปเดตสัปดาห์ละ 1 ครั้ง)
    """
    data = await get_depression_data(view=view, month=month, year=year)
    return {"status": "success", "view": view, "data": data}


from cache_graph import task_update_depression
import asyncio
@router.get("/trigger-depression", dependencies=[Depends(get_api_key)])
async def trigger_depression():
    asyncio.create_task(task_update_depression())
    return {"status": "success", "message": "Triggered"}