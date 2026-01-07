from config import ALGORITHM, TOKEN_EXPIRE_MINUTES, TOKEN_GRACE_PERIOD_MINUTES
from config import SECRET_KEY
from fastapi import HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt, JWTError, ExpiredSignatureError
from datetime import datetime, timedelta, timezone
import logging

#################
logger = logging.getLogger(__name__)

security = HTTPBearer()


def verify_token(credentials: HTTPAuthorizationCredentials = Security(security)):
    try:
        token = credentials.credentials
        payload = decode_token(token)
        current_time = datetime.now(timezone.utc)
        exp_time = datetime.fromtimestamp(payload['exp'], tz=timezone.utc)
        
        # Check if the token is within its normal validity period or grace period
        if current_time <= exp_time + timedelta(minutes=int(TOKEN_GRACE_PERIOD_MINUTES)):
            return payload
        else:
            raise HTTPException(status_code=401, detail="Verify Token has expired")
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

def create_token(data: dict):
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(minutes=int(TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt



def decode_token(token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Decode Token has expired")
    except jwt.JWTClaimsError:
        raise HTTPException(status_code=401, detail="Decode Invalid token claims")
    except jwt.JWTError as e:
        if "Signature verification failed" in str(e):
            raise HTTPException(status_code=401, detail="Decode Invalid token signature")
        elif "Invalid audience" in str(e):
            raise HTTPException(status_code=401, detail="Decode Invalid token audience")
        elif "Invalid issuer" in str(e):
            raise HTTPException(status_code=401, detail="Decode Invalid token issuer")
        else:
            print(f"Unexpected JWT error: {str(e)}")
            raise HTTPException(status_code=401, detail="Decode Invalid authentication credentials")



def renew_token(current_token: str):
    try:
        payload = decode_token(current_token)
        current_time = datetime.now(timezone.utc)
        exp_time = datetime.fromtimestamp(payload['exp'], tz=timezone.utc)
        
        if current_time > exp_time:
            raise HTTPException(status_code=401, detail="Renew Token has expired")
        
        # Allow renewal if token is still valid but close to expiring
        if (exp_time - current_time) < timedelta(minutes=int(TOKEN_GRACE_PERIOD_MINUTES)):
            # # Carry over all claims except 'exp'
            # new_payload = {k: v for k, v in payload.items() if k != 'exp'}
            new_token = create_token(data={"sub": "user-UI"})
            logger.info(f"Token renewed for user {payload.get('sub')}")
            return {"token": new_token, "renewed": True}
        else:
            # If the token is not close to expiring, return the current token
            return {"token": current_token, "renewed": False}
    except JWTError:
        raise HTTPException(status_code=401, detail="Renew Invalid token")