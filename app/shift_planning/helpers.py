"""Вспомогательные функции планирования смен."""
from __future__ import annotations

import uuid
from pathlib import Path

from starlette.datastructures import UploadFile as StarletteUploadFile
from fastapi import UploadFile

from app.config import settings
from app.db.models import User
from app.shift_planning.constants import USER_ROLE_OPERATOR

_UPLOADS_BASE = Path(__file__).resolve().parent.parent.parent / (
    getattr(settings, "uploads_dir", "uploads") or "uploads"
)
_SHIFT_ATTACH_DIR = _UPLOADS_BASE / "shift_tasks"
_ALLOWED_SHIFT_ATTACH_EXT = {
    ".jpg",
    ".jpeg",
    ".png",
    ".webp",
    ".gif",
    ".heic",
    ".pdf",
    ".doc",
    ".docx",
    ".xls",
    ".xlsx",
    ".ppt",
    ".pptx",
    ".txt",
    ".csv",
    ".zip",
    ".rar",
    ".7z",
}


def user_is_operator(user: User) -> bool:
    return (getattr(user, "role", None) or "staff").strip().lower() == USER_ROLE_OPERATOR


def normalize_user_role(raw: str | None) -> str:
    v = (raw or "").strip().lower()
    if v == USER_ROLE_OPERATOR:
        return USER_ROLE_OPERATOR
    return "staff"


async def save_shift_task_attachments(
    files: list[UploadFile | StarletteUploadFile],
) -> list[tuple[str, str]]:
    """Сохранить вложения (фото и файлы); вернуть [(stored_filename, original_filename), ...]."""
    _SHIFT_ATTACH_DIR.mkdir(parents=True, exist_ok=True)
    saved: list[tuple[str, str]] = []
    for f in files or []:
        if not isinstance(f, (UploadFile, StarletteUploadFile)):
            continue
        orig = (f.filename or "").strip()
        if not orig:
            continue
        ext = Path(orig).suffix.lower()
        if ext not in _ALLOWED_SHIFT_ATTACH_EXT:
            continue
        stored = f"{uuid.uuid4().hex}{ext}"
        dest = _SHIFT_ATTACH_DIR / stored
        content = await f.read()
        if not content:
            continue
        dest.write_bytes(content)
        saved.append((stored, orig))
    return saved


async def save_shift_task_photos(
    files: list[UploadFile | StarletteUploadFile],
) -> list[tuple[str, str]]:
    """Алиас для совместимости."""
    return await save_shift_task_attachments(files)


def shift_attachment_url(stored_filename: str) -> str:
    return f"/uploads/shift_tasks/{stored_filename}"
