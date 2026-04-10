import os
import urllib.parse
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv

load_dotenv()


user = os.getenv('neoq_USER')
password = os.getenv('neoq_PASS') 
host = os.getenv('neoq_HOST')
port = os.getenv('neoq_PORT')
name = os.getenv('neoq_NAME')


safe_password = urllib.parse.quote_plus(password)


DB_URL = f"mysql+pymysql://{user}:{safe_password}@{host}:{port}/{name}"


engine = create_engine(DB_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()