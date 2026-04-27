from collections import defaultdict
from typing import List, Dict, Any

def summarize_beds(rows: List[Dict[str, Any]]) -> Dict:
    summary = {
        "total": 0,
        "available": 0,
        "occupied": 0,
        "other": 0,
        "by_ward": {}
    }

    ward_map = defaultdict(lambda: {
        "total": 0,
        "available": 0,
        "occupied": 0,
        "other": 0,
        "by_room": defaultdict(lambda: {
            "total": 0,
            "available": 0,
            "occupied": 0,
            "other": 0,
        })
    })

    def classify(status_id: int) -> str:
        if status_id is None:
            return "other"
        elif status_id == 1:
            return "available"
        elif status_id == 2:
            return "occupied"
        else:
            return "other"

    for row in rows:
        ward = row.get("ward")
        room = row.get("room")
        status_id = row.get("bed_status_type_id")

        bucket = classify(status_id)

        summary["total"] += 1
        summary[bucket] += 1

        ward_map[ward]["total"] += 1
        ward_map[ward][bucket] += 1

        ward_map[ward]["by_room"][room]["total"] += 1
        ward_map[ward]["by_room"][room][bucket] += 1

    # convert defaultdict → dict
    for ward in ward_map:
        ward_map[ward]["by_room"] = dict(ward_map[ward]["by_room"])

    summary["by_ward"] = dict(ward_map)

    return summary