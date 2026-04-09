import os
from fastapi import FastAPI, Depends, HTTPException, Security
from fastapi.security.api_key import APIKeyHeader
from starlette import status
from sqlalchemy.orm import Session
from sqlalchemy import text
from dotenv import load_dotenv

from database import get_db

load_dotenv()

app = FastAPI(title="LCBH Dashboard API")


# ตั้งชื่อ Header ที่ต้องส่งมาใน Request (เช่น access_token: your_key)
API_KEY_NAME = "access_token"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

async def get_api_key(api_key_header: str = Security(api_key_header)):
    # ดึงค่ารหัสลับจาก .env มาเทียบ
    expected_api_key = os.getenv("DASHBOARD_API_KEY")
    
    if api_key_header == expected_api_key and expected_api_key is not None:
        return api_key_header
    else:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, 
            detail="Forbidden: Invalid API Key"
        )

# --- ส่วนของ Endpoints (API) ---

@app.get("/api/v1/test-db")
def test_db_connection(
    db: Session = Depends(get_db),
    api_key: str = Depends(get_api_key) # เพิ่มการเช็ค API Key
):
    try:
        sql = text("SELECT 1")
        db.execute(sql)
        print("✅ Connected to MySQL Successfully!")
        return {
            "status": "success",
            "message": "Connected to Database"
        }
    except Exception as e:
        print(f"❌ Connection Failed: {e}")
        raise HTTPException(
            status_code=500, 
            detail=f"Could not connect to database: {str(e)}"
        )

@app.get("/api/v1/total-services")
def get_total_services(
    db: Session = Depends(get_db),
    api_key: str = Depends(get_api_key) # เพิ่มการเช็ค API Key
):
    try:
        # Query เดิมที่ริวใช้ดึงข้อมูลจาก HOSxP
        sql = text("SELECT count(*) FROM ovst WHERE vstdttm >= CURDATE()")
        result = db.execute(sql).fetchone()
        
        return {
            "today_total": result[0] if result else 0
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Query Error: {str(e)}"
        )