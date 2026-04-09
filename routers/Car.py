from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from database import get_db



# routers/fuel.py
import requests
from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/api/fuel", tags=["GOOGle Forms"])

SHEET_ID = "your_google_sheet_id"
SHEET_NAME = "fuel_log"

# ต้อง Publish Sheet เป็น CSV ก่อน (File → Share → Publish to web → CSV)
SHEET_URL = f"https://docs.google.com/spreadsheets/d/e/2PACX-1vROk5cTxHtrHUXSuS7dSAQ3kdRgj7GcHs-c1YdWXsFQ52ZURrCugyRNdjyT0Qrzxj91oUQxdpRl69e2/pub?output=csv"


@router.get("/latest")
def get_latest_fuel():
    try:
        res = requests.get(SHEET_URL)
        res.encoding = "utf-8"
        lines = res.text.strip().split("\n")
        last = lines[-1].split(",")  # แถวล่าสุด
        return {
            "date":       last[1].strip('"'),
            "shift":      last[2].strip('"'),
            "type":       last[3].strip('"'),
            "fuel_level": float(last[4].strip('"')),
            "mileage":    float(last[5].strip('"')),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -- carDashbrod 
# -- carInfo
