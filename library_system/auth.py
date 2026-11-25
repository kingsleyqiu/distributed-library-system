"""Stateless HMAC-signed authentication for patrons and library staff.

Branch nodes hold no session state; any branch can verify a token issued by
any other branch using the shared AUTH_SECRET, preserving stateless compute.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

UserRole = Literal["patron", "staff"]

DEFAULT_AUTH_SECRET = "sofe4790u-dev-secret"
DEFAULT_TOKEN_TTL_SECONDS = 3600

DEMO_USERS: dict[str, dict[str, str]] = {
    "patron-1": {"password": "password123", "role": "patron", "name": "Alice Patron"},
    "patron-2": {"password": "password123", "role": "patron", "name": "Bob Patron"},
    "patron-3": {"password": "password123", "role": "patron", "name": "Charlie Patron"},
    "staff-1": {"password": "staff123", "role": "staff", "name": "Dana Librarian"},
    "staff-2": {"password": "staff123", "role": "staff", "name": "Evan Librarian"},
}


class LoginRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    user_id: str = Field(min_length=1, max_length=128)
    password: str = Field(min_length=1, max_length=128)


class UserIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    user_id: str
    role: UserRole
    name: str


class LoginResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    access_token: str
    token_type: Literal["bearer"] = "bearer"
    user: UserIdentity


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)


def authenticate_user(user_id: str, password: str) -> UserIdentity | None:
    record = DEMO_USERS.get(user_id)
    if record is None:
        return None
    if not hmac.compare_digest(record["password"], password):
        return None
    return UserIdentity(user_id=user_id, role=record["role"], name=record["name"])  # type: ignore[arg-type]


def issue_token(
    user: UserIdentity,
    secret: str | None = None,
    ttl_seconds: int = DEFAULT_TOKEN_TTL_SECONDS,
) -> str:
    key = (secret or os.getenv("AUTH_SECRET", DEFAULT_AUTH_SECRET)).encode("utf-8")
    payload = {
        "sub": user.user_id,
        "role": user.role,
        "name": user.name,
        "exp": int(time.time()) + ttl_seconds,
    }
    body = _b64url_encode(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    sig = _b64url_encode(hmac.new(key, body.encode("ascii"), hashlib.sha256).digest())
    return f"{body}.{sig}"


def verify_token(token: str, secret: str | None = None) -> UserIdentity:
    key = (secret or os.getenv("AUTH_SECRET", DEFAULT_AUTH_SECRET)).encode("utf-8")
    parts = token.split(".")
    if len(parts) != 2:
        raise ValueError("Malformed token")
    body, sig = parts
    expected = _b64url_encode(hmac.new(key, body.encode("ascii"), hashlib.sha256).digest())
    if not hmac.compare_digest(sig, expected):
        raise ValueError("Invalid token signature")
    payload = json.loads(_b64url_decode(body).decode("utf-8"))
    if int(payload.get("exp", 0)) < int(time.time()):
        raise ValueError("Token expired")
    return UserIdentity(user_id=payload["sub"], role=payload["role"], name=payload["name"])