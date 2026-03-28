"""
Core — JWT Authentication (Section 5)

Validates JWT tokens and extracts user_id.
JWT secret is loaded from environment variable only.
user_id is NEVER accepted from request body.
"""

import logging
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

import jwt
from fastapi import HTTPException, Request, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from app.core.config import settings

logger = logging.getLogger("umsa.auth")

security_scheme = HTTPBearer()


class AuthenticatedUser:
    """Authenticated user context extracted from JWT."""

    def __init__(self, user_id: UUID, user_role: str = "user"):
        self.user_id = user_id
        self.user_role = user_role


async def validate_jwt(
    credentials: HTTPAuthorizationCredentials = Depends(security_scheme),
) -> AuthenticatedUser:
    """
    FastAPI dependency: validates JWT and returns authenticated user.
    Rejects missing, expired, or malformed tokens with HTTP 401.

    user_id is extracted from token claims only — never from request body.
    """
    token = credentials.credentials

    if not settings.jwt_secret:
        raise HTTPException(
            status_code=500,
            detail="JWT secret not configured",
        )

    try:
        payload = jwt.decode(
            token,
            settings.jwt_secret,
            algorithms=[settings.jwt_algorithm],
            options={"require": ["user_id", "exp"]},
        )
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {str(e)}")

    # Extract user_id
    user_id_str = payload.get("user_id")
    if not user_id_str:
        raise HTTPException(status_code=401, detail="Token missing user_id claim")

    try:
        user_id = UUID(user_id_str)
    except ValueError:
        raise HTTPException(status_code=401, detail="Invalid user_id format in token")

    user_role = payload.get("user_role", "user")

    return AuthenticatedUser(user_id=user_id, user_role=user_role)


def generate_token(user_id: str, user_role: str = "user", expires_hours: int = 24) -> str:
    """
    Generate a JWT token (for development/testing only).
    In production, tokens are issued by an external auth provider.
    """
    from datetime import timedelta

    payload = {
        "user_id": user_id,
        "user_role": user_role,
        "exp": datetime.now(timezone.utc) + timedelta(hours=expires_hours),
        "iat": datetime.now(timezone.utc),
    }
    return jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)


# Alias for convenience
generate_dev_token = generate_token
