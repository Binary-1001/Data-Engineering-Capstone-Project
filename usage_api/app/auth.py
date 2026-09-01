import os
import secrets
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

security = HTTPBasic()

API_USERNAME = os.getenv('API_USERNAME', 'admin')
API_PASSWORD = os.getenv('API_PASSWORD', 'admin')


def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    """Validates Basic Auth credentials. Raises 401 if incorrect."""
    valid_username = secrets.compare_digest(credentials.username, API_USERNAME)
    valid_password = secrets.compare_digest(credentials.password, API_PASSWORD)
    if not (valid_username and valid_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username
