"""
routers/finance.py
==================
API Endpoints สำหรับข้อมูลการเงิน (Finance Dashboard)

ตารางที่อ้างอิง (ผ่าน cache_finance.py):
  - incoth  : รายการรับเงิน/ใบเสร็จ
  - pttype  : ประเภทสิทธิ์ผู้ป่วย
  - paidst  : สถานะการชำระเงิน

Endpoints:
  GET /api/finance/summary   → ข้อมูลสรุปการเงิน (by_pttype / monthly / daily / kpi)
"""

from fastapi import APIRouter, Query, Depends
from typing import Optional

from cache_finance import get_finance_data
from utils.security import get_api_key

router = APIRouter(prefix="/api/finance", tags=["Finance Data"])


@router.get("/summary", dependencies=[Depends(get_api_key)])
async def get_finance_summary(
    view: str = Query(
        "by_pttype",
        description=(
            "รูปแบบข้อมูลที่ต้องการ:\n"
            "- **by_pttype** → สรุปยอดแยกตามประเภทสิทธิ์ผู้ป่วย (1 ปีย้อนหลัง)\n"
            "- **monthly**   → สรุปรายเดือน (กรองด้วย ?year=YYYY ได้)\n"
            "- **daily**     → สรุปรายวัน 90 วันล่าสุด (กรองด้วย ?month=YYYY-MM ได้)\n"
            "- **kpi**       → ยอดสรุป: วันนี้ / เดือนนี้ / ปีนี้"
        )
    ),
    month: Optional[str] = Query(
        None,
        description="กรองข้อมูล daily ตามเดือน เช่น '2025-04' (ใช้กับ view=daily)"
    ),
    year: Optional[str] = Query(
        None,
        description="กรองข้อมูล monthly ตามปี เช่น '2025' (ใช้กับ view=monthly)"
    ),
):
    """
    ## Finance Summary

    ดึงข้อมูลสรุปการเงินจาก Redis Cache (อัปเดตอัตโนมัติ)

    ### Schedule การ Refresh Cache:
    - **08:00 – 15:59 น.** → อัปเดตทุก **1 ชั่วโมง** (ช่วงเวลา OPD เปิดทำการ)
    - **นอกเวลาดังกล่าว**  → อัปเดตทุก **3 ชั่วโมง** (ลดภาระ Database)

    ### Views ที่รองรับ:
    | view       | คำอธิบาย                                          |
    |------------|---------------------------------------------------|
    | by_pttype  | สรุปยอดแยกตามสิทธิ์ผู้ป่วยทั้งหมด (1 ปีย้อนหลัง) |
    | monthly    | สรุปยอดรายเดือน (ใช้ ?year=YYYY กรองได้)          |
    | daily      | สรุปยอดรายวัน 90 วัน (ใช้ ?month=YYYY-MM กรองได้) |
    | kpi        | ยอดสรุป: วันนี้ / เดือนนี้ / ปีนี้               |

    ### ฟิลด์ที่ได้รับ (by_pttype / monthly / daily):
    - `pttype_code`    : รหัสสิทธิ์ (เฉพาะ by_pttype)
    - `pttype_name`    : ชื่อสิทธิ์ (เฉพาะ by_pttype)
    - `total_patients` : จำนวนผู้ป่วย (unique HN)
    - `total_visits`   : จำนวนครั้งที่มารับบริการ (unique VN)
    - `cash_amount`    : ยอดเงินสด (paidst 1, 3)
    - `debtor_amount`  : ยอดลูกหนี้/เบิกสิทธิ์ (paidst 2)
    - `unpaid_amount`  : ยอดค้างชำระ (paidst 0)
    - `total_amount`   : ยอดรวมทั้งหมด
    """
    data = await get_finance_data(view=view, month=month, year=year)
    return {"status": "success", "view": view, "data": data}
