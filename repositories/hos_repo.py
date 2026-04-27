from sqlalchemy import text

def get_ovst_stats(db_hos):
    sql = text("""
        SELECT 
            SUM(CASE WHEN ovstist = '01' THEN 1 ELSE 0 END) as walk_in_count,
            SUM(CASE WHEN ovstist = '02' THEN 1 ELSE 0 END) as appointment_count,
            SUM(CASE WHEN ovstist = '03' THEN 1 ELSE 0 END) as referIn_count,
            SUM(CASE WHEN ovstist = '04' THEN 1 ELSE 0 END) as ems_count,
            SUM(CASE WHEN ovstist = '05' THEN 1 ELSE 0 END) as telemed_count,
            SUM(CASE WHEN ovstist = '06' THEN 1 ELSE 0 END) as kiosk_count,
            SUM(CASE WHEN cur_dep = '999' THEN 1 ELSE 0 END) as go_home_count
        FROM ovst 
        WHERE vstdate = CURDATE()
    """)
    return db_hos.execute(sql).fetchone()

def get_drug_delivery_stats(db_hos):
    sql = text("""
        SELECT COUNT(DISTINCT vn) AS total_delivery
        FROM opitemrece
        WHERE icode IN ('3907018', '3907508') 
          AND vstdate = CURDATE()
    """)
    return db_hos.execute(sql).fetchone()

def get_service_time_stats(db_hos):
    sql = text("""
        SELECT
            ROUND(AVG(GREATEST((TIME_TO_SEC(IFNULL(s.service7, s.service12)) - TIME_TO_SEC(s.service3)) / 60.0, 0)), 1),
            ROUND(AVG(GREATEST((TIME_TO_SEC(s.service4)  - TIME_TO_SEC(s.service3))  / 60.0, 0)), 1),
            ROUND(AVG(GREATEST((TIME_TO_SEC(s.service11) - TIME_TO_SEC(s.service4))  / 60.0, 0)), 1),
            ROUND(AVG(GREATEST((TIME_TO_SEC(s.service6)  - TIME_TO_SEC(s.service12)) / 60.0, 0)), 1),
            SUM(CASE WHEN s.service12 IS NOT NULL AND s.service6  IS NULL THEN 1 ELSE 0 END),
            SUM(CASE WHEN s.service19 IS NOT NULL AND s.service7  IS NULL THEN 1 ELSE 0 END),
            ROUND(AVG(CASE WHEN o.main_dep = '010' THEN GREATEST((TIME_TO_SEC(IFNULL(s.service7, s.service12)) - TIME_TO_SEC(s.service3)) / 60.0, 0) END), 1),
            ROUND(AVG(CASE WHEN o.main_dep = '010' THEN GREATEST((TIME_TO_SEC(s.service4)  - TIME_TO_SEC(s.service3))  / 60.0, 0) END), 1),
            ROUND(AVG(CASE WHEN o.main_dep = '010' THEN GREATEST((TIME_TO_SEC(s.service11) - TIME_TO_SEC(s.service4))  / 60.0, 0) END), 1),
            ROUND(AVG(CASE WHEN o.main_dep = '010' THEN GREATEST((TIME_TO_SEC(s.service6)  - TIME_TO_SEC(s.service12)) / 60.0, 0) END), 1),
            SUM(CASE WHEN o.main_dep = '010' AND s.service12 IS NOT NULL AND s.service6 IS NULL THEN 1 ELSE 0 END),
            SUM(CASE WHEN o.main_dep = '010' AND s.service19 IS NOT NULL AND s.service7 IS NULL THEN 1 ELSE 0 END),
            ROUND(AVG(CASE WHEN o.main_dep = '062' THEN GREATEST((TIME_TO_SEC(IFNULL(s.service7, s.service12)) - TIME_TO_SEC(s.service3)) / 60.0, 0) END), 1),
            ROUND(AVG(CASE WHEN o.main_dep = '062' THEN GREATEST((TIME_TO_SEC(s.service4)  - TIME_TO_SEC(s.service3))  / 60.0, 0) END), 1),
            ROUND(AVG(CASE WHEN o.main_dep = '062' THEN GREATEST((TIME_TO_SEC(s.service11) - TIME_TO_SEC(s.service4))  / 60.0, 0) END), 1),
            ROUND(AVG(CASE WHEN o.main_dep = '062' THEN GREATEST((TIME_TO_SEC(s.service6)  - TIME_TO_SEC(s.service12)) / 60.0, 0) END), 1),
            SUM(CASE WHEN o.main_dep = '062' AND s.service12 IS NOT NULL AND s.service6 IS NULL THEN 1 ELSE 0 END),
            SUM(CASE WHEN o.main_dep = '062' AND s.service19 IS NOT NULL AND s.service7 IS NULL THEN 1 ELSE 0 END)
        FROM service_time s
        JOIN ovst o ON s.vn = o.vn
        WHERE s.vstdate = CURDATE()
          AND o.main_dep IN ('010', '062')
          AND s.service3  IS NOT NULL
          AND s.service4  IS NOT NULL
          AND s.service11 IS NOT NULL
    """)
    return db_hos.execute(sql).fetchone()

def get_doctor_operations(db_hos):
    sql = text("""
        SELECT 
            DATE(begin_date_time) AS op_date,
            COUNT(*) AS total_operations
        FROM doctor_operation
        GROUP BY DATE(begin_date_time)
        ORDER BY op_date DESC
    """)
    return db_hos.execute(sql).fetchall()