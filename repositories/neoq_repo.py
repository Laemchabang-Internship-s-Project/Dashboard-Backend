from sqlalchemy import text, bindparam

def get_opd_totals(db_neoq, opd_rooms):
    sql = text("""
        SELECT
            COUNT(DISTINCT hn),
            COUNT(DISTINCT CASE WHEN room_code = '062' THEN hn END),
            COUNT(DISTINCT CASE WHEN room_code != '062' THEN hn END)
        FROM opd_queue
        WHERE date = CURDATE()
        AND room_code IN :rooms
    """).bindparams(bindparam("rooms", expanding=True))
    return db_neoq.execute(sql, {"rooms": opd_rooms}).fetchone()

def get_room_details(db_neoq, target_codes):
    sql = text("""
        SELECT 
            room_code,
            SUM(CASE WHEN room_code = '062' THEN 1 ELSE 0 END),
            SUM(CASE WHEN room_code != '062' THEN 1 ELSE 0 END),
            COUNT(*),
            SUM(CASE WHEN status_id = '3' THEN 1 ELSE 0 END),
            SUM(CASE WHEN status_id != '3' THEN 1 ELSE 0 END)
        FROM opd_queue
        WHERE date = CURDATE()
        AND room_code IN :rooms
        GROUP BY room_code
    """).bindparams(bindparam("rooms", expanding=True))
    return db_neoq.execute(sql, {"rooms": target_codes}).fetchall()

def get_waiting_exam(db_neoq, code):
    sql = text("""
        SELECT COUNT(DISTINCT q.vn) 
        FROM opd_queue q 
        WHERE q.date = CURDATE() AND q.room_code = '023' AND q.status_id != '3'
        AND q.vn IN (SELECT vn FROM opd_queue WHERE room_code = :code AND date = CURDATE())
    """)
    return db_neoq.execute(sql, {"code": code}).scalar()

def get_waiting_lab(db_neoq, code):
    sql = text("""
        SELECT COUNT(DISTINCT l.vn) 
        FROM lab_queue l 
        WHERE l.date = CURDATE() AND l.status_id != '3'
        AND l.vn IN (SELECT vn FROM opd_queue WHERE room_code = :code AND date = CURDATE())
    """)
    return db_neoq.execute(sql, {"code": code}).scalar()

def get_waiting_xray(db_neoq, code):
    sql = text("""
        SELECT COUNT(DISTINCT x.vn) 
        FROM xray_queue x 
        WHERE x.date = CURDATE() AND x.status_id != '3'
        AND x.vn IN (SELECT vn FROM opd_queue WHERE room_code = :code AND date = CURDATE())
    """)
    return db_neoq.execute(sql, {"code": code}).scalar()

def get_wait_times(db_neoq, target_codes):
    sql = text("""
        SELECT
            x.room_code,
            ROUND(AVG(x.wait_minutes), 1) AS avg_wait_minutes
        FROM (
            SELECT
                c.vn, c.room_code,
                GREATEST(
                    TIMESTAMPDIFF(
                        MINUTE,
                        COALESCE(
                            CASE WHEN c.room_code IN ('010', '062', '082') THEN NULL ELSE sub.finish_time END,
                            CONCAT(q.date, ' ', q.time)
                        ),
                        CONCAT(c.date, ' ', MIN(c.time))
                    ),
                    0
                ) AS wait_minutes
            FROM opd_queue_call c
            JOIN opd_queue q ON c.vn = q.vn AND c.date = q.date
            LEFT JOIN (
                SELECT vn, date, MAX(time) as finish_time 
                FROM opd_queue_call 
                WHERE room_code IN ('010', '062')
                GROUP BY vn, date
            ) sub ON c.vn = sub.vn AND c.date = sub.date
            WHERE c.date = CURDATE()
              AND c.room_code IN :rooms
            GROUP BY c.vn, c.room_code, q.date, q.time, sub.finish_time, c.date
        ) x
        WHERE x.wait_minutes < 180 
        GROUP BY x.room_code
    """).bindparams(bindparam("rooms", expanding=True))
    return db_neoq.execute(sql, {"rooms": target_codes}).fetchall()

def get_tech_queue(db_neoq, table_name, inverse_status=False):
    sql = text(f"""
        SELECT COUNT(*),
               SUM(CASE WHEN status_id {'=' if inverse_status else '!='} '3' THEN 1 ELSE 0 END),
               SUM(CASE WHEN status_id {'!=' if inverse_status else '='} '3' THEN 1 ELSE 0 END)
        FROM {table_name}
        WHERE date = CURDATE()
    """)
    return db_neoq.execute(sql).fetchone()