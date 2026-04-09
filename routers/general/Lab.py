from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from database import get_db

router = APIRouter(
    prefix="/api/v1/lab",
    tags=["LAB"]
)

@router.get("/summary")
def get_lab_summary(db: Session = Depends(get_db)):
    try:
        # อ้างอิงจาก CountQueueAll, CountQueueSuccess และ CountQueueWaitQty ใน DLL
        # status_id = '3' คือตรวจเสร็จแล้ว (Success)
        # status_id != '3' คือกำลังรอ (Waiting)
        query = text("""
            SELECT 
                COUNT(*) AS total_all,
                SUM(CASE WHEN status_id != '3' THEN 1 ELSE 0 END) AS total_waiting,
                SUM(CASE WHEN status_id = '3' THEN 1 ELSE 0 END) AS total_finished
            FROM lab_queue 
            WHERE date = CURDATE()
        """)
        
        res = db.execute(query).fetchone()
        
        return {
            "all": int(res[0] or 0),
            "waiting": int(res[1] or 0),
            "finished": int(res[2] or 0)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database Error: {str(e)}")