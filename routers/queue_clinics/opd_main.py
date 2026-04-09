from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from database import get_db

router = APIRouter(
    tags=["OPD Clinics"]
)

@router.get("/summary")
def get_rooms_summary(db: Session = Depends(get_db)):
    try:
        # 1. กำหนดรายชื่อ 6 ห้องที่ริวต้องการ (Master List)
        master_rooms = [
            {"code": "010", "name": "จุดซักประวัติผู้ป่วยนอก"},
            {"code": "062", "name": "จุดซักประวัติผู้ป่วยนอก (นัด)"},
            {"code": "023", "name": "จุดรอตรวจ"},
            {"code": "014", "name": "ห้องหลังพบแพทย์"},
            {"code": "110", "name": "จุดซักประวัติผู้ป่วย (ศัลยกรรม)"},
            {"code": "109", "name": "จุดซักประวัติผู้ป่วย (สูติกรรม)"},
            {"code": "111", "name": "จุดซักประวัติผู้ป่วย (อายุรกรรม)"},
            {"code": "005", "name": "ห้องทันตกรรม"},
            {"code": "041", "name": "แพทย์แผนไทย"},
            {"code": "042", "name": "กายภาพ"},
            {"code": "082", "name": "จุดคัดกรอง OPD"},
            {"code": "113", "name": "ห้องตรวจโรคผิวหนัง"},
            {"code": "134", "name": "คลินิกตรวจอัลตราซาวด์"}
        ]
        
        target_codes = tuple(room["code"] for room in master_rooms)

        # 2. SQL Query: ดึงข้อมูลสรุปรายห้อง
        # Logic: ห้องกลุ่มนี้เป็น Walk-in ทั้งหมด (Appointment = 0) ตามหน้าเว็บที่ริวส่งมา
        query = text("""
            SELECT 
                room_code,
                COUNT(*) AS total,
                SUM(CASE WHEN status_id = '3' THEN 1 ELSE 0 END) AS finished,
                SUM(CASE WHEN status_id != '3' THEN 1 ELSE 0 END) AS waiting
            FROM opd_queue 
            WHERE date = CURDATE() 
              AND room_code IN :rooms
            GROUP BY room_code
        """)

        results = db.execute(query, {"rooms": target_codes}).fetchall()
        db_map = {row[0]: row for row in results}

        # 3. จัดโครงสร้างข้อมูลให้ครบทั้ง 6 ห้องตามลำดับ
        final_report = []
        for master in master_rooms:
            code = master["code"]
            if code in db_map:
                row = db_map[code]
                final_report.append({
                    "room_code": code,
                    "room_name": master["name"],
                    "appointment": 0,  # ตามหน้าเว็บกลุ่มนี้เป็น 0 ทั้งหมด
                    "walk_in": int(row[1]),
                    "total": int(row[1]),
                    "finished": int(row[2]),
                    "waiting": int(row[3])
                })
            else:
                # กรณีห้องนั้นยังไม่มีคิวในวันนี้ (เช่น จุดคัดกรอง หรือ ผิวหนัง)
                final_report.append({
                    "room_code": code,
                    "room_name": master["name"],
                    "appointment": 0,
                    "walk_in": 0,
                    "total": 0,
                    "finished": 0,
                    "waiting": 0
                })

        return final_report

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database Error: {str(e)}")