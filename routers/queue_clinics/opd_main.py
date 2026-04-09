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
        # 1. Master List 13 ห้อง
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

        # 2. SQL Query: ให้ Database แยก นัด/Walk-in ให้เลย
        query = text("""
            SELECT 
                q.room_code,
                SUM(CASE WHEN q.room_code = '062' THEN 1 ELSE 0 END) AS appointment,
                SUM(CASE WHEN q.room_code != '062' THEN 1 ELSE 0 END) AS walk_in,
                COUNT(*) AS total,
                SUM(CASE WHEN q.status_id = '3' THEN 1 ELSE 0 END) AS finished,
                SUM(CASE WHEN q.status_id != '3' THEN 1 ELSE 0 END) AS waiting
            FROM opd_queue q
            WHERE q.date = CURDATE() 
              AND q.room_code IN :rooms
            GROUP BY q.room_code
        """)

        results = db.execute(query, {"rooms": target_codes}).fetchall()
        db_map = {row[0]: row for row in results}

        # 3. ประกอบร่างข้อมูล
        final_report = []
        for master in master_rooms:
            code = master["code"]
            if code in db_map:
                row = db_map[code]
                final_report.append({
                    "room_code": code,
                    "room_name": master["name"],
                    "appointment": int(row[1]),
                    "walk_in": int(row[2]),
                    "total": int(row[3]),
                    "finished": int(row[4]),
                    "waiting": int(row[5])
                })
            else:
                final_report.append({
                    "room_code": code,
                    "room_name": master["name"],
                    "appointment": 0,
                    "walk_in": 0,
                    "total": 0,
                    "finished": 0,
                    "waiting": 0
                })

        # 4. Total OPD unique patient วันนี้
        total_query = text("""
            SELECT COUNT(DISTINCT hn) AS total_opd_patients_today
            FROM opd_queue
            WHERE date = CURDATE()
        """)
        total_result = db.execute(total_query).fetchone()
        total_opd_today = int(total_result[0]) if total_result else 0

        return {
            "total_opd_today": total_opd_today,
            "rooms": final_report
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database Error: {str(e)}")