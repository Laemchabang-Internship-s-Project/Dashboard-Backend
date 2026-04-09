from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from database import get_db

router = APIRouter(prefix="/api/v1/xray", tags=["Xray"])

@router.get("/summary")
def get_xray_summary(db: Session = Depends(get_db)):
    # SQL นี้อ้างอิงจากฟังก์ชัน CountQueueXray ใน DLL
    query = text("""
        SELECT 
            COUNT(*) AS total_all,
            SUM(CASE WHEN status_id != '3' THEN 1 ELSE 0 END) AS total_waiting,
            SUM(CASE WHEN status_id = '3' THEN 1 ELSE 0 END) AS total_finished
        FROM xray_queue 
        WHERE date = CURDATE()
    """)
    
    res = db.execute(query).fetchone()
    
    return {
        "all": res[0] or 0,
        "waiting": int(res[1] or 0),
        "finished": int(res[2] or 0)
    }