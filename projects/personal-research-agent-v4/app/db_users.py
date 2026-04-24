"""User configuration helpers for Personal Research Agent v4."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app import config as app_config
from app import db


DEFAULT_USERS_PATH = "config/users.json"


def load_user_config(config_path: str | None = None) -> list[dict[str, Any]]:
    path = app_config.resolve_project_path(config_path or DEFAULT_USERS_PATH)
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, list):
        raise ValueError("User configuration must be a list of user objects")
    return data


def seed_users_from_config(
    config_path: str | None = None,
    db_path: str | None = None,
) -> list[dict[str, Any]]:
    config = app_config.load_app_config()
    runtime_db_path = db_path if db_path is not None else config.runtime_db_path
    users = []
    for item in load_user_config(config_path):
        created = db.create_user(
            chat_id=int(item["chat_id"]),
            name=str(item.get("name") or item["chat_id"]),
            language=str(item.get("language") or config.default_language),
            topics=item.get("topics") or config.default_topics,
            db_path=runtime_db_path,
        )
        db.ensure_profile_for_user(
            user_id=int(created["id"]),
            language=str(created["language"]),
            topics=list(created.get("topics") or config.default_topics),
            db_path=runtime_db_path,
        )
        users.append(created)
    return users


def ensure_user(
    chat_id: int,
    name: str | None = None,
    db_path: str | None = None,
) -> dict[str, Any]:
    config = app_config.load_app_config()
    runtime_db_path = db_path if db_path is not None else config.runtime_db_path
    existing = db.get_user_by_chat_id(chat_id, runtime_db_path)
    if existing is not None:
        db.ensure_profile_for_user(
            user_id=int(existing["id"]),
            language=str(existing["language"]),
            topics=list(existing.get("topics") or config.default_topics),
            db_path=runtime_db_path,
        )
        return existing
    created = db.create_user(
        chat_id=chat_id,
        name=name or f"User {chat_id}",
        language=config.default_language,
        topics=config.default_topics,
        db_path=runtime_db_path,
    )
    db.ensure_profile_for_user(
        user_id=int(created["id"]),
        language=str(created["language"]),
        topics=list(created.get("topics") or config.default_topics),
        db_path=runtime_db_path,
    )
    return created


def update_user_topics(
    chat_id: int,
    topics: list[str],
    db_path: str | None = None,
) -> dict[str, Any]:
    return db.update_user_preferences(chat_id=chat_id, topics=topics, db_path=db_path)


def update_user_language(
    chat_id: int,
    language: str,
    db_path: str | None = None,
) -> dict[str, Any]:
    return db.update_user_preferences(chat_id=chat_id, language=language, db_path=db_path)


def get_user_language(chat_id: int, db_path: str | None = None) -> str | None:
    user = db.get_user_by_chat_id(chat_id, db_path)
    if user is None:
        return None
    return str(user["language"])
