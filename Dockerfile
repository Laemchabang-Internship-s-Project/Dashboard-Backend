# ============================================
# LCBH Dashboard API - Production Dockerfile
# ============================================
FROM python:3.11-slim

# ตั้งค่า working directory
WORKDIR /app

# ติดตั้ง dependencies ก่อน (ใช้ Docker layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# คัดลอก source code ทั้งหมด
COPY . .

# เปิด port 8000
EXPOSE 8000

# รัน FastAPI ด้วย Uvicorn
# - workers 1 เพราะใช้ asyncio background task (ถ้าใช้มากกว่า 1 จะมี worker ซ้ำกัน)
# - timeout-keep-alive สูงเพื่อรองรับ SSE long-lived connections
CMD ["uvicorn", "main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "1", \
     "--timeout-keep-alive", "120"]
