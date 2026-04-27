from database_neoq import SessionLocal as SessionNEOQ
from repositories import neoq_repo

OPD_TOTAL_ROOMS = ('010', '062', '005', '041', '042', '109', '110', '111', '001', '002')
MASTER_ROOMS = [
    {"code": "010", "name": "จุดซักประวัติผู้ป่วยนอก"}, {"code": "062", "name": "จุดซักประวัติผู้ป่วยนอก (นัด)"},
    {"code": "023", "name": "จุดรอตรวจ"}, {"code": "014", "name": "ห้องหลังพบแพทย์"},
    {"code": "110", "name": "ศัลยกรรม"}, {"code": "109", "name": "สูติกรรม"},
    {"code": "111", "name": "อายุรกรรม"}, {"code": "005", "name": "ทันตกรรม"},
    {"code": "041", "name": "แพทย์แผนไทย"}, {"code": "042", "name": "กายภาพ"},
    {"code": "082", "name": "คัดกรอง OPD"}, {"code": "113", "name": "ผิวหนัง"},
    {"code": "134", "name": "อัลตราซาวด์"},
]

def fetch_neoq_sync():
    opd_total = appointment = walk_in = custom_opd_total = waiting_screening = waiting_exam = 0
    rooms = []
    tech = {
        "xray_queue": {"all": 0, "waiting": 0, "finished": 0}, "lab_queue": {"all": 0, "waiting": 0, "finished": 0},
        "pharmacy_queue": {"all": 0, "waiting": 0, "finished": 0}, "finance_queue": {"all": 0, "waiting": 0, "finished": 0},
    }
    dept_stats = {
        "010": {"total": 0, "waiting_screening": 0, "waiting_exam": 0, "waiting_lab": 0, "waiting_xray": 0},
        "062": {"total": 0, "waiting_screening": 0, "waiting_exam": 0, "waiting_lab": 0, "waiting_xray": 0}
    }

    try:
        with SessionNEOQ() as db_neoq:
            sys_res = neoq_repo.get_opd_totals(db_neoq, OPD_TOTAL_ROOMS)
            if sys_res:
                opd_total, appointment, walk_in = int(sys_res[0] or 0), int(sys_res[1] or 0), int(sys_res[2] or 0)

            target_codes = tuple(r["code"] for r in MASTER_ROOMS)
            res = neoq_repo.get_room_details(db_neoq, target_codes)
            db_map = {r[0]: r for r in res}

            for code in ["010", "062"]:
                row = db_map.get(code, [0, 0, 0, 0, 0, 0])
                dept_stats[code]["total"] = int(row[3])
                dept_stats[code]["waiting_screening"] = int(row[5])
                dept_stats[code]["waiting_exam"] = neoq_repo.get_waiting_exam(db_neoq, code) or 0
                dept_stats[code]["waiting_lab"] = neoq_repo.get_waiting_lab(db_neoq, code) or 0
                dept_stats[code]["waiting_xray"] = neoq_repo.get_waiting_xray(db_neoq, code) or 0

            wait_res = neoq_repo.get_wait_times(db_neoq, target_codes)
            wait_map = {r[0]: float(r[1]) for r in wait_res}

            for dept in ["xray_queue", "lab_queue"]:
                r = neoq_repo.get_tech_queue(db_neoq, dept, inverse_status=False)
                if r: tech[dept] = {"all": int(r[0] or 0), "waiting": int(r[1] or 0), "finished": int(r[2] or 0)}

            for dept in ["pharmacy_queue", "finance_queue"]:
                r = neoq_repo.get_tech_queue(db_neoq, dept, inverse_status=True)
                if r: tech[dept] = {"all": int(r[0] or 0), "finished": int(r[1] or 0), "waiting": int(r[2] or 0)}

            data_010, data_062, data_023 = db_map.get('010', [0]*6), db_map.get('062', [0]*6), db_map.get('023', [0]*6)
            custom_opd_total = int(data_010[3]) + int(data_062[3])
            waiting_screening = int(data_010[5]) + int(data_062[5])
            waiting_exam = int(data_023[5])

            for m in MASTER_ROOMS:
                r = db_map.get(m["code"])
                avg_wait = wait_map.get(m["code"], None)
                if r:
                    room_data = {
                        "room_code": m["code"], "room_name": m["name"],
                        "appointment": int(r[1]), "walk_in": int(r[2]),
                        "total": int(r[3]), "finished": int(r[4]), "waiting": int(r[5])
                    }
                    if avg_wait is not None: room_data["avg_wait_minutes"] = avg_wait 
                    rooms.append(room_data)
                else:
                    rooms.append({"room_code": m["code"], "room_name": m["name"], "appointment": 0, "walk_in": 0, "total": 0, "finished": 0, "waiting": 0})

    except Exception as e:
        print(f"[Cache Worker] NEOQ Connection Error: {e}")

    return {
        "opd_total": opd_total, "appointment": appointment, "walk_in": walk_in,
        "custom_opd_total": custom_opd_total, "waiting_screening": waiting_screening,
        "waiting_exam": waiting_exam, "rooms": rooms, "tech": tech, "dept_stats": dept_stats
    }