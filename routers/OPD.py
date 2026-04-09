from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from database import get_db

router = APIRouter(
    prefix="/OPD",
    tags=["opd"]
)


# --- จำนวนผู้รับบริการทั้งหมด (วันนี้) ---
@router.get("/total-patients")
def get_total_patients(db: Session = Depends(get_db)):
    """จำนวนผู้รับบริการทั้งหมดวันนี้"""
    try:
        sql = text("""
            SELECT COUNT(*) AS cc
            FROM ovst
            WHERE DATE(vstdttm) = CURDATE()
        """)
        result = db.execute(sql).fetchone()
        return {"total_patients": result["cc"] if result else 0}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query Error: {str(e)}")


# --- จำนวนผู้รับบริการ Walk-in ---
@router.get("/walkin-patients")
def get_walkin_patients(db: Session = Depends(get_db)):
    """จำนวนผู้รับบริการ Walk-in วันนี้"""
    try:
        sql = text("""
            SELECT COUNT(*) AS cc
            FROM ovst
            WHERE DATE(vstdttm) = CURDATE()
              AND vsttype = '1'
        """)
        result = db.execute(sql).fetchone()
        return {"walkin_patients": result["cc"] if result else 0}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query Error: {str(e)}")


# --- จำนวนผู้รับบริการทางไกล (Telemedicine) ---
@router.get("/telemedicine-patients")
def get_telemedicine_patients(db: Session = Depends(get_db)):
    """จำนวนผู้รับบริการทางไกลวันนี้"""
    try:
        sql = text("""
            SELECT COUNT(*) AS cc
            FROM ovst
            WHERE DATE(vstdttm) = CURDATE()
              AND vsttype = '4'
        """)
        result = db.execute(sql).fetchone()
        return {"telemedicine_patients": result["cc"] if result else 0}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query Error: {str(e)}")


# --- จำนวนผู้รับบริการบริการขยส่งยา ---
@router.get("/drug-delivery-patients")
def get_drug_delivery_patients(db: Session = Depends(get_db)):
    """จำนวนผู้รับบริการบริการขยส่งยาวันนี้"""
    try:
        sql = text("""
            SELECT COUNT(*) AS cc
            FROM ovst
            WHERE DATE(vstdttm) = CURDATE()
              AND vsttype = '5'
        """)
        result = db.execute(sql).fetchone()
        return {"drug_delivery_patients": result["cc"] if result else 0}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query Error: {str(e)}")


# --- ยอดรวมค่าใช้จ่ายวันนี้ ---
@router.get("/total-revenue")
def get_total_revenue(db: Session = Depends(get_db)):
    """ยอดรวมค่าใช้จ่ายของผู้รับบริการวันนี้"""
    try:
        sql = text("""
            SELECT COALESCE(SUM(income), 0) AS total
            FROM oapp
            WHERE DATE(appdate) = CURDATE()
        """)
        result = db.execute(sql).fetchone()
        return {"total_revenue": float(result["total"]) if result else 0.0}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query Error: {str(e)}")


# --- จำนวนคิว X-ray วันนี้ (ตัวอย่าง pattern จาก xray_queue) ---
@router.get("/xray-queue-count")
def get_xray_queue_count(db: Session = Depends(get_db)):
    """จำนวนคิว X-ray วันนี้"""
    try:
        sql = text("""
            SELECT COUNT(*) AS cc
            FROM xray_queue q
            WHERE q.date = CURDATE()
        """)
        result = db.execute(sql).fetchone()
        return {"xray_queue_count": result["cc"] if result else 0}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query Error: {str(e)}")


# --- สรุปภาพรวม OPD ทั้งหมดในหน้าเดียว ---
@router.get("/summary")
def get_opd_summary(db: Session = Depends(get_db)):
    """ดึงข้อมูลสรุปภาพรวม OPD ทั้งหมดในครั้งเดียว"""
    try:
        queries = {
            "total_patients": "SELECT COUNT(*) AS cc FROM ovst WHERE DATE(vstdttm) = CURDATE()",
            "walkin_patients": "SELECT COUNT(*) AS cc FROM ovst WHERE DATE(vstdttm) = CURDATE() AND vsttype = '1'",
            "telemedicine_patients": "SELECT COUNT(*) AS cc FROM ovst WHERE DATE(vstdttm) = CURDATE() AND vsttype = '4'",
            "drug_delivery_patients": "SELECT COUNT(*) AS cc FROM ovst WHERE DATE(vstdttm) = CURDATE() AND vsttype = '5'",
            "xray_queue_count": "SELECT COUNT(*) AS cc FROM xray_queue q WHERE q.date = CURDATE()",
        }

        summary = {}
        for key, sql_str in queries.items():
            result = db.execute(text(sql_str)).fetchone()
            summary[key] = result["cc"] if result else 0

        # ยอดรวมค่าใช้จ่าย (ใช้ SUM แทน COUNT)
        rev_result = db.execute(
            text("SELECT COALESCE(SUM(income), 0) AS total FROM oapp WHERE DATE(appdate) = CURDATE()")
        ).fetchone()
        summary["total_revenue"] = float(rev_result["total"]) if rev_result else 0.0

        return summary

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query Error: {str(e)}")
