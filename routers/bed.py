from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from typing import Dict, List
import json
from cache_manager import redis_client

from utils.security import get_api_key
from routers.auth import get_current_user
from rate_limiter import limiter

router = APIRouter(prefix="/api/beds", tags=["Beds"])

from utils.bed_constants import KEY_BED_CACHE, KEY_BED_CONFIG, DEFAULT_CONFIG

class BedConfigRequest(BaseModel):
    wards: Dict[str, int] = Field(..., description="ชื่อ ward → จำนวนเตียงทั้งหมด")
    allowed_wards: List[str] = Field(default=[], description="รายชื่อ ward ที่ใช้นับยอดรวม")
    total_beds: int = Field(default=150, description="ยอดรวมเตียงทั้งหมด")


@router.get("/summary", dependencies=[Depends(get_api_key)])
@limiter.limit("30/minute")
async def get_bed_summary(
    request: Request,
    _user: dict = Depends(get_current_user)
):
    try:
        raw = await redis_client.get(KEY_BED_CACHE)
        
        if not raw:
            raise HTTPException(status_code=503, detail="กำลังโหลดข้อมูลเตียง กรุณารอสักครู่...")
            
        return {"status": "success", "data": json.loads(raw)}
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"[Bed Summary] Error: {e}")
        raise HTTPException(status_code=500, detail="ไม่สามารถดึงข้อมูลสรุปเตียงได้")

@router.get("/config", dependencies=[Depends(get_api_key)])
@limiter.limit("30/minute")
async def get_bed_config(
    request: Request,
    _user: dict = Depends(get_current_user)
):
    """
    ดึงค่า config จำนวนเตียง (Fixed Wards) ปัจจุบัน
    ถ้ายังไม่มีการตั้งค่า จะคืนค่า default
    """
    try:
        raw = await redis_client.get(KEY_BED_CONFIG)
        if raw:
            parsed = json.loads(raw)
            # รองรับกรณีที่ Redis เก็บแค่ Dictionary ของ wards อย่างเดียว (โค้ดเก่า)
            if "wards" not in parsed:
                config = {
                    "wards": parsed,
                    "allowed_wards": DEFAULT_CONFIG["allowed_wards"],
                    "total_beds": DEFAULT_CONFIG["total_beds"]
                }
            else:
                config = parsed
        else:
            config = DEFAULT_CONFIG

        return {"status": "success", "data": config}

    except Exception as e:
        print(f"[Bed Config GET] Error: {e}")
        raise HTTPException(status_code=500, detail="ไม่สามารถดึง config เตียงได้")


@router.post("/config", dependencies=[Depends(get_api_key)])
@limiter.limit("10/minute")
async def update_bed_config(
    request: Request,
    body: BedConfigRequest,
    _user: dict = Depends(get_current_user)
):
    """
    อัปเดตค่า config จำนวนเตียง (Fixed Wards)
    รับ dict ชื่อ ward → จำนวนเตียงทั้งหมด
    ค่าจะถูกเก็บลง Redis และมีผลทันที (Frontend จะดึงค่าใหม่รอบถัดไป)
    """
    try:
        # Validate ว่าจำนวนเตียงต้องไม่ติดลบ
        for ward_name, bed_count in body.wards.items():
            if bed_count < 0:
                raise HTTPException(
                    status_code=400,
                    detail=f"จำนวนเตียงของ '{ward_name}' ต้องไม่ติดลบ"
                )
            if bed_count > 999:
                raise HTTPException(
                    status_code=400,
                    detail=f"จำนวนเตียงของ '{ward_name}' เกินขีดจำกัด (สูงสุด 999)"
                )

        # สร้าง Object ใหม่เพื่อบันทึก
        new_config = {
            "wards": body.wards,
            "allowed_wards": body.allowed_wards,
            "total_beds": body.total_beds
        }

        # บันทึกลง Redis (ไม่มี expiry — ถาวรจนกว่าจะแก้ใหม่)
        await redis_client.set(
            KEY_BED_CONFIG,
            json.dumps(new_config, ensure_ascii=False)
        )

        print(f"[Bed Config] อัปเดตแล้ว")

        # สั่งให้ประมวลผลข้อมูลเตียงใหม่เดี๋ยวนั้นเพื่อให้อัปเดตทันที
        try:
            from tasks.bed_worker import fetch_beds_sync
            import asyncio
            bed_data = await asyncio.to_thread(fetch_beds_sync, new_config)
            if bed_data:
                await redis_client.set(KEY_BED_CACHE, json.dumps(bed_data, ensure_ascii=False))
                print(f"[Bed Config] บังคับประมวลผล Summary ใหม่เสร็จสิ้น")
        except Exception as summary_err:
            print(f"[Bed Config] ไม่สามารถประมวลผล Summary ทันทีได้: {summary_err}")

        return {
            "status": "success",
            "message": "บันทึก config เตียงสำเร็จ",
            "data": new_config
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"[Bed Config POST] Error: {e}")
        raise HTTPException(status_code=500, detail="ไม่สามารถบันทึก config เตียงได้")