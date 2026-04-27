import asyncio
import json
from cache.dashboard_cache import patch_redis_cache, redis_client
from core.redis_client import KEY_GRAPH_CACHE
from services.hos_service import fetch_hos_sync, fetch_graph_sync
from services.neoq_service import fetch_neoq_sync

async def task_update_hos():
    print("[Task HOSxP] เริ่มทำงาน...")
    while True:
        try:
            hos_data = await asyncio.to_thread(fetch_hos_sync)
            total_walkin_kiosk = hos_data["walk_in"] + hos_data["kiosk"]
            total_hos_opd = sum([hos_data.get(k, 0) for k in ["walk_in", "appointment", "referIn", "ems", "telemed", "kiosk"]])

            patch_data = {
                "system": {
                    "hos_walk_in": hos_data["walk_in"], "hos_appointment": hos_data["appointment"],
                    "hos_referIn": hos_data["referIn"], "hos_ems": hos_data["ems"],
                    "hos_telemed": hos_data["telemed"], "hos_kiosk": hos_data["kiosk"],
                    "hos_go_home": hos_data.get("go_home", 0), "total_walkin": total_walkin_kiosk,
                    "total_OPD": total_hos_opd, "total_drug_delivery": hos_data["drug_delivery"]
                },
                "summary": {
                    "avg_wait_total": hos_data["avg_total"], "avg_wait_screening": hos_data["avg_wait_screening"],
                    "avg_wait_examination": hos_data["avg_wait_exam"], "avg_wait_drug": hos_data["avg_wait_drug"],
                    "waiting_drug": hos_data["waiting_drug"], "waiting_payment": hos_data["waiting_payment"],
                    "dep_010": hos_data["dep_010"], "dep_062": hos_data["dep_062"],
                }
            }
            await patch_redis_cache(patch_data)
        except Exception as e:
            print(f"[Task HOSxP] Loop Error: {e}")
        await asyncio.sleep(5)

async def task_update_graph():
    print("[Task Graph] เริ่มทำงาน...")
    while True:
        try:
            graph_data = await asyncio.to_thread(fetch_graph_sync)
            if graph_data:
                await redis_client.set(KEY_GRAPH_CACHE, json.dumps(graph_data, ensure_ascii=False))
        except Exception as e:
            print(f"[Task Graph] Loop Error: {e}")
        await asyncio.sleep(604800)

async def task_update_neoq():
    print("[Task NEOQ] เริ่มทำงาน...")
    while True:
        try:
            n_data = await asyncio.to_thread(fetch_neoq_sync)
            patch_data = {
                "system": {"today_total_services": n_data["opd_total"]},
                "opd_clinics": {
                    "header": {
                        "opd_total": n_data["opd_total"], "appointment": n_data["appointment"],
                        "walk_in": n_data["walk_in"], "custom_opd_total": n_data["custom_opd_total"],
                        "waiting_screening": n_data["waiting_screening"], "waiting_exam": n_data["waiting_exam"]
                    },
                    "rooms": n_data["rooms"],
                    "stats_010": n_data["dept_stats"]["010"], "stats_062": n_data["dept_stats"]["062"]
                },
                "technical_services": {
                    "xray": n_data["tech"]["xray_queue"], "lab": n_data["tech"]["lab_queue"],
                    "pharmacy": n_data["tech"]["pharmacy_queue"], "finance": n_data["tech"]["finance_queue"],
                }
            }
            await patch_redis_cache(patch_data)
        except Exception as e:
            print(f"[Task NEOQ] Loop Error: {e}")
        await asyncio.sleep(5)

async def update_redis_cache():
    print("[Cache Worker] เริ่มกระจายงาน (HOSxP และ NEOQ รันขนานกัน)...")
    asyncio.create_task(task_update_hos())
    asyncio.create_task(task_update_neoq())
    asyncio.create_task(task_update_graph())
    while True:
        await asyncio.sleep(3600)