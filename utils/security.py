import os
from fastapi import HTTPException, Security, status
from fastapi.security.api_key import APIKeyHeader
import ipaddress
from fastapi import HTTPException, Request

API_KEY_NAME = "x-api-key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

async def get_api_key(api_key_header: str = Security(api_key_header)):
    expected_api_key = os.getenv("DASHBOARD_API_KEY")
    
    if api_key_header == expected_api_key and expected_api_key is not None:
        return api_key_header
    
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN, 
        detail="Forbidden: Invalid API Key"
    )

# กำหนดวง Network ที่อนุญาต
INTERNAL_NETWORKS = [
    "10.0.0.0/24",     
    "127.0.0.1/32",  
    "192.168.0.0/24",
    "125.24.18.19/32"   
]

def is_ip_internal(client_ip: str) -> bool:
    """ตรวจสอบว่า IP อยู่ในวงที่กำหนดหรือไม่"""
    try:
        client_addr = ipaddress.ip_address(client_ip)
        for network in INTERNAL_NETWORKS:
            if client_addr in ipaddress.ip_network(network):
                return True
        return False
    except ValueError:
        return False

async def verify_ip(request: Request):
    """Dependency สำหรับตรวจสอบ IP"""
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        client_ip = forwarded_for.split(",")[0].strip()
    else:
        client_ip = request.client.host
    
    if not is_ip_internal(client_ip):
        # ถ้าไม่ใช่คนใน ให้ตอบกลับ 403 Forbidden
        raise HTTPException(status_code=403, detail="Access Denied: Internal Network Only")
    return True