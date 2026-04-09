from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from database import get_db

router = APIRouter(
    prefix="/WFH",
    tags=["wfh"]
)

# -- WFH info