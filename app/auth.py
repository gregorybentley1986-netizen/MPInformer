"""
Авторизация пользователей сайта (логин/пароль из БД).
"""
from __future__ import annotations

from fastapi import Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from passlib.hash import sha256_crypt

from app.db.database import get_db
from app.db.models import User


def hash_password(password: str) -> str:
    """Хэш пароля для сохранения в БД."""
    return sha256_crypt.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    """Проверка пароля против хэша."""
    return sha256_crypt.verify(plain, hashed)


def _session_get(request: Request, key: str, default=None):
    try:
        session = getattr(request, "session", None)
        return session.get(key, default) if session else default
    except Exception:
        return default


async def verify_site_user(
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> User:
    """
    Зависимость: текущий пользователь сайта по сессии.
    Если не авторизован — редирект на /login (через HTTPException 303).
    """
    user_id = _session_get(request, "site_user_id")
    if not user_id:
        raise HTTPException(status_code=303, headers={"Location": "/login"})
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        raise HTTPException(status_code=303, headers={"Location": "/login"})
    result = await db.execute(select(User).where(User.id == uid))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=303, headers={"Location": "/login"})
    return user
