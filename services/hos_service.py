from database_hos import SessionLocal as SessionHOS
from repositories import hos_repo

def fetch_hos_sync():
    hos_data = {
        "walk_in": 0, "appointment": 0, "referIn": 0,
        "ems": 0, "telemed": 0, "kiosk": 0, "go_home": 0,
        "drug_delivery": 0, "avg_total": 0.0, "avg_wait_screening": 0.0,
        "avg_wait_exam": 0.0, "avg_wait_drug": 0.0, "waiting_drug": 0, "waiting_payment": 0,
        "dep_010": {"avg_total": 0.0, "avg_wait_screening": 0.0, "avg_wait_exam": 0.0, "avg_wait_drug": 0.0, "waiting_drug": 0, "waiting_payment": 0},
        "dep_062": {"avg_total": 0.0, "avg_wait_screening": 0.0, "avg_wait_exam": 0.0, "avg_wait_drug": 0.0, "waiting_drug": 0, "waiting_payment": 0},
    }
    try:
        with SessionHOS() as db_hos:
            hos_res = hos_repo.get_ovst_stats(db_hos)
            if hos_res:
                hos_data.update({
                    "walk_in": int(hos_res[0] or 0), "appointment": int(hos_res[1] or 0),
                    "referIn": int(hos_res[2] or 0), "ems": int(hos_res[3] or 0),
                    "telemed": int(hos_res[4] or 0), "kiosk": int(hos_res[5] or 0),
                    "go_home": int(hos_res[6] or 0)
                })

            delivery_res = hos_repo.get_drug_delivery_stats(db_hos)
            if delivery_res:
                hos_data["drug_delivery"] = int(delivery_res[0] or 0)

            svc_res = hos_repo.get_service_time_stats(db_hos)
            if svc_res:
                hos_data.update({
                    "avg_total": float(svc_res[0] or 0), "avg_wait_screening": float(svc_res[1] or 0),
                    "avg_wait_exam": float(svc_res[2] or 0), "avg_wait_drug": float(svc_res[3] or 0),
                    "waiting_drug": int(svc_res[4] or 0), "waiting_payment": int(svc_res[5] or 0),
                    "dep_010": {
                        "avg_total": float(svc_res[6] or 0), "avg_wait_screening": float(svc_res[7] or 0),
                        "avg_wait_exam": float(svc_res[8] or 0), "avg_wait_drug": float(svc_res[9] or 0),
                        "waiting_drug": int(svc_res[10] or 0), "waiting_payment": int(svc_res[11] or 0),
                    },
                    "dep_062": {
                        "avg_total": float(svc_res[12] or 0), "avg_wait_screening": float(svc_res[13] or 0),
                        "avg_wait_exam": float(svc_res[14] or 0), "avg_wait_drug": float(svc_res[15] or 0),
                        "waiting_drug": int(svc_res[16] or 0), "waiting_payment": int(svc_res[17] or 0),
                    }
                })
    except Exception as e:
        print(f"[Cache Worker] HOSxP Error: {e}")
    return hos_data

def fetch_graph_sync():
    data = []
    try:
        with SessionHOS() as db_hos:
            res = hos_repo.get_doctor_operations(db_hos)
            for r in res:
                data.append({"op_date": str(r[0]), "total_operations": int(r[1])})
    except Exception as e:
        print(f"[Cache Worker] Graph HOSxP Error: {e}")
    return data