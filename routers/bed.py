from fastapi import APIRouter, Depends, HTTPException, Request
import json
from cache_manager import redis_client
from tasks.bed_worker import KEY_BED_CACHE
from utils.security import get_api_key
from routers.auth import get_current_user
from rate_limiter import limiter

router = APIRouter(prefix="/api/beds", tags=["Beds"])

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