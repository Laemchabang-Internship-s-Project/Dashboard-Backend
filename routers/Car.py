from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from database import get_db



# routers/fuel.py
import requests
from fastapi import APIRouter, HTTPException


# ควรเก็บข้อมูลลงdata base เผื่อกรณีข sheet หาย ทำให้ข้อมูลการบันทึกหายได้


router = APIRouter(prefix="/api/fuel", tags=["GOOGle Forms"])

SHEET_NAME = "fuel_log"

# ต้อง Publish Sheet เป็น CSV ก่อน (File → Share → Publish to web → CSV)
# เปลียนเป็น Sheet mail รพ ตอนนี้เป็น demo
SHEET_URL = f"https://docs.google.com/spreadsheets/d/e/2PACX-1vROk5cTxHtrHUXSuS7dSAQ3kdRgj7GcHs-c1YdWXsFQ52ZURrCugyRNdjyT0Qrzxj91oUQxdpRl69e2/pub?output=csv"

# แก้ข้อมูลหน่อยให้เป็นตามรถแต่ละคัน 
@router.get("/latest")
def get_latest_fuel():
    try:
        res = requests.get(SHEET_URL, timeout=10)
        res.encoding = "utf-8"
        lines = res.text.strip().split("\n")
        
        if len(lines) <= 1:  # มีแค่ header ไม่มีข้อมูล
            raise HTTPException(status_code=404, detail="ไม่มีข้อมูล")
        
        last = lines[-1].split(",")
        
        return {
            "date":       last[0].strip('"'),
            "time":       last[1].strip('"'),
            "shift":      last[2].strip('"'),
            "type":       last[3].strip('"'),
            "fuel_level": float(last[4].strip('"')),
            "mileage":    float(last[5].strip('"')),
        }
    except HTTPException:
        raise  # โยน HTTPException ต่อไปตามเดิม
    except requests.exceptions.Timeout:
        raise HTTPException(status_code=504, detail="Google Sheets ตอบสนองช้าเกินไป")
    except requests.exceptions.ConnectionError:
        raise HTTPException(status_code=503, detail="เชื่อมต่อ Google Sheets ไม่ได้")
    except (IndexError, ValueError) as e:
        raise HTTPException(status_code=422, detail=f"รูปแบบข้อมูลใน Sheet ผิดพลาด: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"เกิดข้อผิดพลาด: {str(e)}")

# -- carDashbrod 
# -- carInfo
# ------------- app script -----------
# // ทำงานทุกครั้งที่มีคนกรอก Form
# function onFormSubmit(e) {
#   const values = e.values;
  
#   // values[0] คือ Timestamp จาก Google (แก้ไม่ได้)
#   const timestamp = new Date(values[0]);
#   const shift     = values[1]; // กะ
#   const type      = values[2]; // เริ่มงาน / เลิกงาน
#   const fuelLevel = parseFloat(values[3]);
#   const mileage   = parseFloat(values[4]);

#   // แยกวันและเวลาออกจาก Timestamp
#   const date = Utilities.formatDate(timestamp, "Asia/Bangkok", "yyyy-MM-dd");
#   const time = Utilities.formatDate(timestamp, "Asia/Bangkok", "HH:mm:ss");

#   Logger.log(`${date}  ${time} | ${shift} | ${type} | ${fuelLevel}L`);

#   // เขียนลง Sheet แยก column
#   const sheet = SpreadsheetApp.getActiveSpreadsheet()
#                               .getSheetByName("fuel_log");
#   sheet.appendRow([date, time, shift, type, fuelLevel, mileage]);
# }

# // Trigger: ติดตั้งครั้งเดียว
# function setupTrigger() {
#   ScriptApp.newTrigger("onFormSubmit")
#     .forSpreadsheet(SpreadsheetApp.getActive())
#     .onFormSubmit()
#     .create();
# }