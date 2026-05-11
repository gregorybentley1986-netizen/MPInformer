"""
Авторизация админки по сессии (логин/пароль из .env, форма входа).
"""
from fastapi import Depends, HTTPException, Request, status


def verify_admin(request: Request) -> str:
    """
    Проверка сессии: если пользователь не авторизован — редирект на страницу входа.
    """
    try:
        session = getattr(request, "session", None)
        username = session.get("admin_user") if session is not None else None
    except Exception:
        username = None
    if not username:
        raise HTTPException(
            status_code=303,
            detail="Требуется вход",
            headers={"Location": "/admin/login"},
        )
    return username
