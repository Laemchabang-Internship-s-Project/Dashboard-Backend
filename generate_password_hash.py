"""
Script สำหรับ generate bcrypt hash ของรหัสผ่าน
วิธีใช้: python generate_password_hash.py
"""
import secrets
from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

print("=" * 60)
print("  Dashboard Password Hash Generator")
print("=" * 60)

password = input("\nใส่รหัสผ่านที่ต้องการใช้: ").strip()
if not password:
    print("❌ รหัสผ่านว่าง ออกจากโปรแกรม")
    exit(1)

hashed = pwd_context.hash(password)
secret_key = secrets.token_hex(32)  # 256-bit random key

print("\n✅ ได้ค่าสำหรับใส่ใน .env แล้ว:\n")
print(f"AUTH_SECRET_KEY={secret_key}")
print(f"DASHBOARD_PASSWORD_HASH={hashed}")
print(f"AUTH_TOKEN_EXPIRE_MINUTES=480")
print("\n⚠️  เพิ่มค่าเหล่านี้ใน Dashboard-Backend/.env แล้ว restart server")
print("=" * 60)
