import os
from fastapi import FastAPI, Depends, HTTPException, Security
from fastapi.security.api_key import APIKeyHeader
from starlette import status
from sqlalchemy.orm import Session
from sqlalchemy import text
from dotenv import load_dotenv

from database import get_db
# 1. เพิ่ม Import Xray (และตรวจสอบว่าชื่อไฟล์ในโฟลเดอร์ routers ตรงกันไหม)
from routers import OPD, Power, Pharmacy, Car, WFH
from routers.general import Xray 
from routers.general import Lab

load_dotenv()

# --- ส่วนของ Security Functions ---
API_KEY_NAME = "access_token"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

async def get_api_key(api_key_header: str = Security(api_key_header)):
    expected_api_key = os.getenv("DASHBOARD_API_KEY")
    if api_key_header == expected_api_key and expected_api_key is not None:
        return api_key_header
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN, 
        detail="Forbidden: Invalid API Key"
    )

app = FastAPI(
    title="LCBH Dashboard API",
    dependencies=[Depends(get_api_key)] 
)

# --- รวม Router ทั้งหมด ---
app.include_router(OPD.router)
app.include_router(Power.router)
app.include_router(Pharmacy.router)
app.include_router(Car.router)
app.include_router(WFH.router)
app.include_router(Xray.router)
app.include_router(Lab.router)



@app.get("/api/v1/test-db")
def test_db_connection(db: Session = Depends(get_db)):
    # ไม่ต้องใส่ Depends(get_api_key) แล้วเพราะเราใส่ไว้ที่ระดับ app แล้ว
    try:
        sql = text("SELECT 1")
        db.execute(sql)
        return {"status": "success", "message": "Connected to Database"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/total-services")
def get_total_services(db: Session = Depends(get_db)):
    try:
        sql = text("SELECT count(*) FROM ovst WHERE vstdttm >= CURDATE()")
        result = db.execute(sql).fetchone()
        return {"today_total": result[0] if result else 0}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))