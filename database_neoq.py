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


user = get_env('neoq_USER')
password = get_env('neoq_PASS')
host = get_env('neoq_HOST')
port = get_env('neoq_PORT')
name = get_env('neoq_NAME')

safe_password = urllib.parse.quote_plus(str(password))

DB_URL = f"mysql+pymysql://{user}:{safe_password}@{host}:{port}/{name}"

engine = create_engine(DB_URL, pool_pre_ping=True)

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