from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from database import get_db

app = FastAPI()

@app.get("/api/v1/total-services")
def get_total_services(db: Session = Depends(get_db)):
    # ลองเขียน SQL ดิบเพื่อ Query ข้อมูล (อิงตามโครงสร้าง HOSxP)
    sql = text("SELECT count(*) FROM ovst WHERE vstdttm >= CURDATE()")
    result = db.execute(sql).fetchone()
    
    return {
        "today_total": result[0] if result else 0
    }