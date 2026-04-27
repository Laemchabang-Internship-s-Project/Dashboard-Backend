from fastapi import APIRouter
from cache_manager import get_graph_data

router = APIRouter(prefix="/api/graph", tags=["Graph Data"])

@router.get("/doctor-operations")
async def get_doctor_operations_graph():
    """ดึงข้อมูลกราฟการผ่าตัดของแพทย์ (อัปเดตสัปดาห์ละ 1 ครั้ง)"""
    data = await get_graph_data()
    return {"status": "success", "data": data}