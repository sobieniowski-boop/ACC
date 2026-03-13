"""JWT authentication + RBAC."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
import bcrypt as _bcrypt

from app.core.config import settings

ALGORITHM = "HS256"
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/token")


class Role(str, Enum):
    ADMIN = "admin"
    DIRECTOR = "director"       # Ecommerce Director — full access
    CATEGORY_MGR = "category_mgr"  # Category Manager — own categories
    OPS = "ops"                 # Ops — inventory, orders, shipping
    ANALYST = "analyst"         # Analyst — read-only


# Role hierarchy (higher index = more permissions)
ROLE_HIERARCHY = [Role.ANALYST, Role.OPS, Role.CATEGORY_MGR, Role.DIRECTOR, Role.ADMIN]


def hash_password(password: str) -> str:
    return _bcrypt.hashpw(password.encode("utf-8"), _bcrypt.gensalt()).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    return _bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))


def create_access_token(subject: str, role: str, expires_delta: Optional[timedelta] = None) -> str:
    expire = datetime.now(timezone.utc) + (
        expires_delta or timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    payload = {"sub": subject, "role": role, "exp": expire, "type": "access"}
    return jwt.encode(payload, settings.SECRET_KEY, algorithm=ALGORITHM)


def create_refresh_token(subject: str, role: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)
    payload = {"sub": subject, "role": role, "exp": expire, "type": "refresh"}
    return jwt.encode(payload, settings.SECRET_KEY, algorithm=ALGORITHM)


def decode_token(token: str) -> dict:
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )


async def get_current_user(token: str = Depends(oauth2_scheme)) -> dict:
    payload = decode_token(token)
    if payload.get("type") != "access":
        raise HTTPException(status_code=401, detail="Invalid token type")
    allowed_marketplaces = payload.get("allowed_marketplaces")
    allowed_brands = payload.get("allowed_brands")
    sub = payload["sub"]
    try:
        user_id = __import__("uuid").UUID(sub)
    except (ValueError, AttributeError):
        user_id = sub
    return {
        "user_id": user_id,
        "role": payload["role"],
        "allowed_marketplaces": allowed_marketplaces if isinstance(allowed_marketplaces, list) else [],
        "allowed_brands": allowed_brands if isinstance(allowed_brands, list) else [],
    }


def require_role(*allowed_roles: Role):
    """FastAPI dependency that checks user has sufficient role."""
    async def dependency(current_user: dict = Depends(get_current_user)) -> dict:
        user_role = current_user.get("role")
        user_idx = next(
            (i for i, r in enumerate(ROLE_HIERARCHY) if r.value == user_role), -1
        )
        required_idx = min(
            ROLE_HIERARCHY.index(r) for r in allowed_roles if r in ROLE_HIERARCHY
        )
        if user_idx < required_idx:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Role '{user_role}' insufficient. Required: {[r.value for r in allowed_roles]}",
            )
        return current_user
    return dependency


# Convenience dependencies
require_admin = require_role(Role.ADMIN)
require_director = require_role(Role.DIRECTOR, Role.ADMIN)
require_ops = require_role(Role.OPS, Role.DIRECTOR, Role.ADMIN)
require_analyst = require_role(Role.ANALYST, Role.OPS, Role.CATEGORY_MGR, Role.DIRECTOR, Role.ADMIN)
