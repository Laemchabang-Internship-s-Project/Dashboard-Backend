import os
from fastapi import HTTPException, Security, status
from fastapi.security.api_key import APIKeyHeader

API_KEY_NAME = "access_token"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

async def get_api_key(api_key_header: str = Security(api_key_header)):
    expected_api_key = os.getenv("DASHBOARD_API_KEY")
    
    if api_key_header == expected_api_key and expected_api_key is not None:
        return api_key_header
    
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN, 
        detail="Forbidden: Invalid API Key"
    )