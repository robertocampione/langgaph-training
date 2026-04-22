"""User configuration helpers for Personal Research Agent v3."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from app import db


DEFAULT_USERS_PATH = "config/users.json"


def load_user_config(config_path: str | None = None) -> list[dict[str, Any]]:
    path = Path(config_path or DEFAULT_USERS_PATH)
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
    users = []
    for item in load_user_config(config_path):
        users.append(
            db.create_user(
                chat_id=int(item["chat_id"]),
                name=str(item.get("name") or item["chat_id"]),
                language=str(item.get("language") or os.getenv("DEFAULT_LANGUAGE", "it")),
                topics=item.get("topics") or os.getenv("DEFAULT_TOPICS", "news,events,bitcoin"),
                db_path=db_path,
            )
        )
    return users


def ensure_user(
    chat_id: int,
    name: str | None = None,
    db_path: str | None = None,
) -> dict[str, Any]:
    existing = db.get_user_by_chat_id(chat_id, db_path)
    if existing is not None:
        return existing
    return db.create_user(
        chat_id=chat_id,
        name=name or f"User {chat_id}",
        language=os.getenv("DEFAULT_LANGUAGE", "it"),
        topics=os.getenv("DEFAULT_TOPICS", "news,events,bitcoin"),
        db_path=db_path,
    )


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

