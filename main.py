from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from dotenv import load_dotenv

from database import get_db

from utils.security import get_api_key

from routers import OPD, Power, Pharmacy, Car, WFH
from routers.queue_technical import Xray, Lab,Pharmacy


load_dotenv()

app = FastAPI(
    title="LCBH Dashboard API",
    version="1.0.0",
    dependencies=[Depends(get_api_key)] 
)

# --- กลุ่มที่ 1: แผนกหลักและระบบซัพพอร์ต (Base Routers) ---
app.include_router(OPD.router)
app.include_router(Power.router)
app.include_router(Pharmacy.router)
app.include_router(Car.router)
app.include_router(WFH.router)

# --- กลุ่มที่ 2: แผนกเทคนิค (Technical Services) ---
# ใช้ prefix ร่วมกันเพื่อความเป็นระเบียบใน Swagger     
app.include_router(Lab.router, prefix="/api/technical/lab", tags=["Technical Services"])
app.include_router(Xray.router, prefix="/api/technical/xray", tags=["Technical Services"])

app.include_router(Pharmacy.router, prefix="/api/technical/pharmacy", tags=["Technical Services"])
# --- System Check Endpoints ---

@app.get("/api/system/test-db", tags=["System"])
def test_db_connection(db: Session = Depends(get_db)):
    try:
        sql = text("SELECT 1")
        db.execute(sql)
        return {"status": "success", "message": "Connected to Database"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB Connection Error: {str(e)}")

@app.get("/api/system/total-services", tags=["System"])
def get_total_services(db: Session = Depends(get_db)):
    try:
        # ดึงสถิติรวมจาก HOSxP
        sql = text("SELECT count(*) FROM ovst WHERE vstdttm >= CURDATE()")
        result = db.execute(sql).fetchone()
        return {"today_total": result[0] if result else 0}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))