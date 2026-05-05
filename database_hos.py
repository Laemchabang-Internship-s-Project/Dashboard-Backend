import os
import urllib.parse
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv

load_dotenv()


def get_env(name: str):
    value = os.getenv(name)
    if value is None:
        raise RuntimeError(f"{name} is missing")
    return value


user = get_env('hos_USER')
password = get_env('hos_PASS')
host = get_env('hos_HOST')
port = get_env('hos_PORT')
name = get_env('hos_NAME')

safe_password = urllib.parse.quote_plus(str(password))

DB_URL = f"mysql+pymysql://{user}:{safe_password}@{host}:{port}/{name}"

engine = create_engine(
    DB_URL, 
    pool_pre_ping=True,  
    pool_size=5,         
    max_overflow=10,     
    pool_recycle=1800    # รีเซ็ต/สร้าง Connection ใหม่ทุกๆ 30 นาที ป้องกัน Database สั่งตัดการเชื่อมต่อที่ทิ้งไว้นานเกิน
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()