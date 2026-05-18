from collections import defaultdict
from typing import List, Dict, Any

def summarize_beds(rows: List[Dict[str, Any]], config: Dict = None) -> Dict:
    summary = {
        "total": 0,
        "available": 0,
        "occupied": 0,
        "other": 0,
        "by_ward": {}
    }

    # WARD_NAME_MAP เพื่อแปลงชื่อวอร์ดจาก Database เป็นชื่อที่สวยงามสำหรับ Frontend
    WARD_NAME_MAP = {
        "ห้องคลอด": "ห้องคลอด",
        "วิกฤตทารกแรกเกิด": "หอผู้ป่วยวิกฤตทารกแรกเกิด",
        "หลังคลอด": "หอผู้ป่วยหลังคลอด",
        "ผู้ป่วยเด็ก": "หอผู้ป่วยเด็ก",
        "ผู้ป่วยศัลยชาย": "หอผู้ป่วยศัลยกรรม กระดูกและข้อชาย",
        "ผู้ป่วยศัลยหญิง": "หอผู้ป่วยศัลยกรรม กระดูกและข้อหญิง",
        "ผู้ป่วยอายุรกรรมชาย": "หอผู้ป่วยอายุรกรรมชาย",
        "ผู้ป่วยอายุรกรรมหญิง": "หอผู้ป่วยอายุรกรรมหญิง",
        "ผู้ป่วยพิเศษอาคารอ่าวอุดม ชั้น 4": "หอผู้ป่วยพิเศษอาคารอ่าวอุดม ชั้น 4",
        "มินิธัญญารักษ์": "มินิธัญญารักษ์",
        "หน่วยไตเทียม": "หน่วยไตเทียม",
        "ER Observ": "ER Observ",
        "ICU": "หอผู้ป่วย ICU"
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

    target_beds = {
        "ด.18", "ด.05", "ด.06", "ด.13", "ด.15", "ด.12", "ด.02", 
        "ตู้อบ7", 
        "ม.ด2"
    }
    
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
        bed_name = row.get("bed_name") 
        status_id = row.get("bed_status_type_id")
        if room == "ห้องสามัญเด็ก":
            if bed_name not in target_beds:
                continue

        bucket = classify(status_id)
        
        display_name = WARD_NAME_MAP.get(ward, ward)

        # ข้าม ODS ward และ หน่วยไตเทียม (หรือค่าว่าง)
        if not display_name or display_name.lower() in ["other", "null", "none", "ods ward", "หน่วยไตเทียม"]:
            continue

        ward_map[display_name]["occupied" if bucket == "occupied" else "other"] += (1 if bucket == "occupied" else 0)

    # นำ Config มาประยุกต์ใช้เพื่อคิดคำนวณ Total Beds และ Available 
    config_wards = config.get("wards", {}) if config else {}
    allowed_wards = config.get("allowed_wards", []) if config else []
    total_beds_config = config.get("total_beds", 150) if config else 150

    # แปลง allowed_wards ให้ตรงกับ display_name เพื่อคำนวณยอดรวม
    allowed_wards_mapped = [WARD_NAME_MAP.get(w, w) for w in allowed_wards]
    
    total_occupied = 0

    for ward_name, stats in ward_map.items():
        # หาค่า Total ของวอร์ดจาก Config (โดยเช็คจากชื่อ Display Name หรือชื่อเดิม)
        # เนื่องจากบางทีคนบันทึก Config อาจใช้ชื่อเดิม หรือชื่อที่ถูกแมปไปแล้ว
        # ให้ลองหาทั้ง 2 แบบ
        ward_original_name = next((k for k, v in WARD_NAME_MAP.items() if v == ward_name), ward_name)
        
        if ward_original_name in config_wards:
            stats["total"] = config_wards[ward_original_name]
        elif ward_name in config_wards:
            stats["total"] = config_wards[ward_name]
        else:
            stats["total"] = stats["occupied"] # Fallback

        # คำนวณ Available = Total - Occupied
        stats["available"] = max(0, stats["total"] - stats["occupied"])
        stats["other"] = 0 # reset other, เราไม่ได้ใช้โชว์

        # รวมยอด Occupied สำหรับวอร์ดที่ Allowed
        if ward_name in allowed_wards_mapped:
            total_occupied += stats["occupied"]

    summary["total"] = total_beds_config
    summary["occupied"] = total_occupied
    summary["available"] = max(0, total_beds_config - total_occupied)
    summary["by_ward"] = dict(ward_map)

    return summary