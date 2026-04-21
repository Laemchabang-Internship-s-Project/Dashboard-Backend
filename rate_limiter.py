import os
from slowapi import Limiter
from utils.network import get_client_ip

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")
REDIS_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}/1"

limiter = Limiter(
    key_func=get_client_ip,
    default_limits=["60/minute"],
    storage_uri=REDIS_URL,
    strategy="fixed-window",
)