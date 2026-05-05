# ============================================
# LCBH Dashboard API - Production Dockerfile
# ============================================
FROM python:3.11-slim

# 1. ตั้งค่า working directory
WORKDIR /app

# 2. สร้าง System User เพื่อความปลอดภัย (ไม่ใช้ root รันแอป)
RUN addgroup --system appgroup && adduser --system --group appuser

# 3. ติดตั้ง dependencies ก่อน (ใช้ Docker layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. คัดลอก source code ทั้งหมด
COPY . .

# 5. เปลี่ยนเจ้าของไฟล์ทั้งหมดใน /app ให้เป็น appuser
RUN chown -R appuser:appgroup /app

# 6. สลับไปใช้ User ที่สร้างขึ้น
USER appuser

# เปิด port 8000
EXPOSE 8000

# รัน FastAPI ด้วย Uvicorn
# - workers 1 เพราะใช้ asyncio background task
# - timeout-keep-alive 120 รองรับ SSE long-lived connections
# - proxy-headers และ forwarded-allow-ips เพื่อรับ IP จริงจาก Apache
CMD ["uvicorn", "main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "1", \
     "--timeout-keep-alive", "120", \
     "--proxy-headers", \
     "--forwarded-allow-ips", "*"]