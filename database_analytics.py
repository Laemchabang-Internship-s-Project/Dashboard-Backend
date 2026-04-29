import os
import urllib.parse # เพิ่มการ import นี้
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def get_env(name: str):
    value = os.getenv(name)
    if value is None:
        raise RuntimeError(f"{name} is missing")
    return value

user     = get_env('PG_USER')
password = urllib.parse.quote_plus(get_env('PG_PASSWORD')) 
db_name  = get_env('PG_DB')
host     = get_env('PG_HOST')
port     = get_env('PG_PORT')

DB_URL = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"

engine = create_engine(
    DB_URL, 
    pool_pre_ping=True,
    pool_recycle=1800
)

SessionAnalytics = sessionmaker(autocommit=False, autoflush=False, bind=engine)