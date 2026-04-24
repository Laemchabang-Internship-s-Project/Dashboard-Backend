"""
auth.py — ระบบ Login / Token สำหรับ Dashboard
- POST /api/auth/login  → ตรวจรหัสผ่าน → คืน JWT access token
- GET  /api/auth/verify → ตรวจว่า token ยังใช้ได้อยู่หรือไม่
"""
import os
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException, status, Depends, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
import bcrypt
from pydantic import BaseModel
from rate_limiter import limiter

router = APIRouter(prefix="/api/auth", tags=["Auth"])

# ─── Config ─────────────────────────────────────────────────────────────────
# โหลดจาก .env — ต้องตั้งค่าก่อน deploy
SECRET_KEY = os.getenv("AUTH_SECRET_KEY", "CHANGE_ME_IN_PRODUCTION_USE_STRONG_RANDOM")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("AUTH_TOKEN_EXPIRE_MINUTES", "480"))  # 8 ชั่วโมง

# ─── Bearer Token Scheme ─────────────────────────────────────────────────────
bearer_scheme = HTTPBearer(auto_error=False)


# ─── Models ──────────────────────────────────────────────────────────────────
class LoginRequest(BaseModel):
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int  # วินาที


# ─── Helpers ─────────────────────────────────────────────────────────────────
def verify_password(plain: str, hashed: str) -> bool:
    plain_bytes = plain.encode()[:72]
    return bcrypt.checkpw(plain_bytes, hashed.encode())  # ใช้ plain_bytes


def create_access_token(data: dict, expires_delta: timedelta | None = None) -> str:
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (
        expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    to_encode["exp"] = expire
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def decode_token(token: str) -> dict:
    """คืน payload หรือ raise JWTError"""
    return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])


# ─── Dependency: ใช้กับ Protected Routes ─────────────────────────────────────
async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
):
    """
    ตรวจ Bearer token — ถ้าไม่มีหรือไม่ถูกต้องให้ตอบ 401
    ใช้เป็น Depends ใน Router ที่ต้องการ Login ก่อน
    """
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="กรุณา Login ก่อน",
            headers={"WWW-Authenticate": "Bearer"},
        )
    try:
        payload = decode_token(credentials.credentials)
        role: str = payload.get("role")
        if role != "dashboard_user":
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")
        return payload
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token ไม่ถูกต้องหรือหมดอายุแล้ว",
            headers={"WWW-Authenticate": "Bearer"},
        )


# ─── Routes ──────────────────────────────────────────────────────────────────
@router.post("/login", response_model=TokenResponse)
<<<<<<< HEAD
@limiter.limit("100/minute")  # เพิ่มโควต้าเป็น 100 ครั้ง/นาที เพื่อรองรับ IP ที่ซ้ำกัน
=======
@limiter.limit("100/minute")
>>>>>>> c14a607 (fix rate limit)
async def login(request: Request, body: LoginRequest):
    """
    รับรหัสผ่านจาก Frontend → ตรวจกับ hash ที่เก็บใน .env
    ถ้าถูกต้องคืน JWT token, ถ้าผิดคืน 401
    """
    # ป้องกัน password ยาวเกินไป (DoS via bcrypt)
    if len(body.password) > 128:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="รหัสผ่านยาวเกินไป",
        )

    # โหลด hashed password จาก environment variable
    hashed_password = os.getenv("DASHBOARD_PASSWORD_HASH")

    if not hashed_password:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Server configuration error: DASHBOARD_PASSWORD_HASH ยังไม่ได้ตั้งค่า",
        )

    if not verify_password(body.password, hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="รหัสผ่านไม่ถูกต้อง",
        )

    expire = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    token = create_access_token({"sub": "dashboard", "role": "dashboard_user"}, expire)

    return TokenResponse(
        access_token=token,
        token_type="bearer",
        expires_in=int(expire.total_seconds()),
    )


@router.get("/verify")
async def verify_token(payload: dict = Depends(get_current_user)):
    """ตรวจว่า token ยังใช้ได้อยู่หรือเปล่า (Frontend เรียกตอน reload หน้า)"""
    return {"valid": True, "role": payload.get("role")}
