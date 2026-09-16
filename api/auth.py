"""Minimal JWT auth. No user table, just two hardcoded demo accounts:

    admin  / admin123   -> can read and write
    viewer / viewer123  -> can only read

Log in via POST /login to get a token, then send it back as
"Authorization: Bearer <token>" on every other request.
"""

import os
from datetime import datetime, timedelta, timezone

import bcrypt # type: ignore
import jwt # type: ignore
from fastapi import Depends, HTTPException # type: ignore
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer # type: ignore

# Set AUTH_SECRET_KEY in the environment for anything beyond local dev.
# The fallback below is 32+ bytes only to satisfy HS256's minimum key
# length, it is not a secret.
SECRET_KEY = os.environ.get("AUTH_SECRET_KEY", "dev-only-secret-do-not-use-in-production")
ALGORITHM = "HS256"
TOKEN_EXPIRE_MINUTES = 60

# ponytail: hardcoded demo users, replace with a real user table if
# this ever needs more than two logins.
USERS = {
    "admin": {
        "password_hash": bcrypt.hashpw(b"admin123", bcrypt.gensalt()),
        "role": "admin",
    },
    "viewer": {
        "password_hash": bcrypt.hashpw(b"viewer123", bcrypt.gensalt()),
        "role": "viewer",
    },
}

security = HTTPBearer()


def authenticate(username: str, password: str) -> str | None:
    """Returns the user's role if the password is correct, else None."""
    user = USERS.get(username)
    if not user or not bcrypt.checkpw(password.encode(), user["password_hash"]):
        return None
    return user["role"]


def create_access_token(username: str, role: str) -> str:
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=TOKEN_EXPIRE_MINUTES)
    payload = {"sub": username, "role": role, "exp": expires_at}
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    return {"username": payload["sub"], "role": payload["role"]}


def require_admin(user: dict = Depends(get_current_user)):
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin role required")
    return user
