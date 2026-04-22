import os
from fastapi import HTTPException, Security, status
from fastapi.security.api_key import APIKeyHeader
import ipaddress
from fastapi import HTTPException, Request
from utils.network import get_client_ip

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
    "192.168.2.0/24",
    "172.16.0.0/12",
    "122.154.113.68/32"   
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
    client_ip = get_client_ip(request)

    if not is_ip_internal(client_ip):
        raise HTTPException(status_code=403, detail=f"Access Denied: {client_ip}")

    return True