from config import TOKEN_EXPIRE_MINUTES, API_KEY,SECRET_KEY
from auth.utils import create_token, renew_token
from fastapi import APIRouter, Header, HTTPException
import logging

logger = logging.getLogger(__name__)

auth_router = APIRouter()

@auth_router.post("/loadconfig")
async def load_config(api_key: str = Header(None)):
    if api_key!= SECRET_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")
    token = create_token(data={"sub": "user-UI"})
    return {
        "token": token,
        "expire_min": TOKEN_EXPIRE_MINUTES
    }


@auth_router.post("/renew")
async def renew_token_route(current_token: str = Header(None)):
    result = renew_token(current_token)
    return {"token": result["token"], "expire_min": TOKEN_EXPIRE_MINUTES, "renewed": result["renewed"]}
