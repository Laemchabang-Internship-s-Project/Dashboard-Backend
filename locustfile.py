from locust import HttpUser, task, between

class DashboardUser(HttpUser):
    # ── URL ของ Backend (ที่รันอยู่ใน Docker บน port 8000) ─────────────────
    host = "http://localhost:8000"

    # หน่วงเวลาสุ่ม 1-3 วินาที ระหว่างการยิงแต่ละครั้ง (เลียนแบบคนจริงๆ)
    wait_time = between(1, 3)

    # ใส่ API Key ของคุณเข้าไปใน Header
    def on_start(self):
        self.headers = {
            "x-api-key": "dGVzdDEyMzQ1Njc4OQ=="  # API Key ตามไฟล์ .env
        }

    # ── ทดสอบ API หลักที่ใช้งานบ่อย ──────────────────────────────────────

    # หน้า OPD Dashboard (ดึงข้อมูล Real-time จาก Redis)
    @task(5)
    def test_opd_summary(self):
        self.client.get("/api/opd/summary", headers=self.headers)

    # กราฟผ่าตัดแพทย์รายวัน
    @task(3)
    def test_graph_doctor_daily(self):
        self.client.get("/api/graph/doctor-operations?view=daily", headers=self.headers)

    # สถานะเตียง
    @task(3)
    def test_bed_summary(self):
        self.client.get("/api/beds/summary", headers=self.headers)

    # Finance: สรุปรายปี
    @task(2)
    def test_finance_yearly(self):
        self.client.get("/api/finance/summary?view=yearly", headers=self.headers)

    # Finance: สรุปรายเดือน
    @task(2)
    def test_finance_monthly(self):
        self.client.get("/api/finance/summary?view=monthly&year=2026", headers=self.headers)

    # Finance: KPI วันนี้
    @task(2)
    def test_finance_kpi(self):
        self.client.get("/api/finance/summary?view=kpi", headers=self.headers)

    # กราฟสถิติการเสียชีวิต
    @task(1)
    def test_death_graph(self):
        self.client.get("/api/graph/death?view=causes&year=2026", headers=self.headers)

    # กราฟทะเบียนซึมเศร้า
    @task(1)
    def test_depression_graph(self):
        self.client.get("/api/graph/depression?view=daily", headers=self.headers)
