import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
PG_USER = os.getenv("PG_USER", "admin")
PG_PASSWORD = os.getenv("PG_PASSWORD", "@10823")
PG_DB = os.getenv("PG_DB", "hospital_analytics")
PG_HOST = os.getenv("PG_HOST", "localhost")

# Wait, the HOSxP DB is MySQL! Let's check hos_...
hos_user = os.getenv("hos_USER")
hos_pass = os.getenv("hos_PASS")
hos_host = os.getenv("hos_HOST")
hos_name = os.getenv("hos_NAME")
hos_port = os.getenv("hos_PORT", 3306)

engine = create_engine(f"mysql+pymysql://{hos_user}:{hos_pass}@{hos_host}:{hos_port}/{hos_name}")
with engine.connect() as conn:
    res = conn.execute(text("SELECT death_date FROM death WHERE death_date IS NOT NULL AND death_date != '' LIMIT 5;"))
    print("Dates:", res.fetchall())
