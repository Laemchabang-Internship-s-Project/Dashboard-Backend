from fastapi import APIRouter, Depends,HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from database import get_db

router = APIRouter( 
    tags=["Technical Services"]
    )

@router.get("/summary")
def get_queue_all(db: Session = Depends(get_db)):
    try:
        query = text("""
            SELECT
                COUNT(*) AS total_all,
                SUM(CASE WHEN status_id = '3' THEN 1 ELSE 0 END) AS success,
                SUM(CASE WHEN status_id != '3' THEN 1 ELSE 0 END) AS waitQty
            FROM finance_queue
            WHERE date = CURDATE()
        """)

        res = db.execute(query).fetchone()
        return {
            "all":     int(res[0] or 0),
            "finished": int(res[1] or 0),
            "waiting": int(res[2] or 0)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query Error: {str(e)}")