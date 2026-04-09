from fastapi import APIRouter, Depends,HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from database import get_db

router = APIRouter(prefix="/api/v1/Pharmacy", tags=["Pharmacy"])

@router.get("/CountQueueAll")
def get_queue_all(db: Session = Depends(get_db)):
    """จำนวนผู้รับบริการทั้งหมดวันนี้"""
    try:
        sql = text("""
            SELECT COUNT(*) AS cc
            FROM pharmacy_queue q
            WHERE q.date = CURDATE()
        """)
        result = db.execute(sql).fetchone()
        return {"get_queue_all": result["cc"] if result else 0}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query Error: {str(e)}")

@router.get("/CountQueueSuccess")
def get_queue_seccess(db: Session = Depends(get_db)):
    """จำนวนผู้รับบริการที่ success วันนี้"""
    try:
        sql = text("""
            SELECT COUNT(*) cc 
            FROM pharmacy_queue q 
            WHERE q.date=CURDATE() AND q.status_id='3')
        """)
        result = db.execute(sql).fetchone()
        return {"get_queue_seccess":result["CC"]if result else 0}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"quert Error: {str(e)}")
    
@router.get("/CountQueueWaitQty")
def get_queue_waitQty(db: Session = Depends(get_db)):
    """จำนวนผู้รับบริการที่ รออยู่"""
    try:
        sql = text("""
            SELECT COUNT(*) cc 
            FROM pharmacy_queue q 
            WHERE q.date=CURDATE() AND q.status_id!='3'
        """)
        result = db.execute(sql).fetchone()
        return {"get_queue_waitQty":result["cc"]if result else 0}
    except Exception as e:
        raise HTTPException(status_code=500,detail=f"quert Error:{str(e)}")