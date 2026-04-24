"""SQLite persistence helpers for Personal Research Agent v3."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


from app import config as app_config


DEFAULT_DB_PATH = app_config.DEFAULT_DB_PATH


SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        chat_id INTEGER UNIQUE NOT NULL,
        name TEXT NOT NULL,
        language TEXT NOT NULL,
        topics TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS runs (
        id INTEGER PRIMARY KEY,
        user_id INTEGER NOT NULL,
        timestamp TEXT NOT NULL,
        report_path TEXT,
        newsletter_path TEXT,
        quality_status TEXT NOT NULL,
        selected_counts TEXT NOT NULL,
        FOREIGN KEY(user_id) REFERENCES users(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS articles (
        id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        url TEXT UNIQUE NOT NULL,
        category TEXT,
        domain TEXT,
        published_at TEXT,
        summary TEXT,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS feedback (
        id INTEGER PRIMARY KEY,
        user_id INTEGER NOT NULL,
        article_id TEXT NOT NULL,
        rating INTEGER NOT NULL,
        notes TEXT,
        created_at TEXT NOT NULL,
        FOREIGN KEY(user_id) REFERENCES users(id),
        FOREIGN KEY(article_id) REFERENCES articles(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS cache (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
)


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def get_db_path(db_path: str | None = None) -> Path:
    return app_config.resolve_project_path(db_path or app_config.load_app_config().db_path)


def get_connection(db_path: str | None = None) -> sqlite3.Connection:
    path = get_db_path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def initialize_database(db_path: str | None = None) -> None:
    with get_connection(db_path) as connection:
        for statement in SCHEMA_STATEMENTS:
            connection.execute(statement)
        connection.commit()


def row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    data = dict(row)
    if "topics" in data and isinstance(data["topics"], str):
        data["topics"] = json.loads(data["topics"])
    if "selected_counts" in data and isinstance(data["selected_counts"], str):
        data["selected_counts"] = json.loads(data["selected_counts"])
    return data


def normalize_topics(topics: Iterable[str] | str | None) -> list[str]:
    if topics is None:
        return []
    if isinstance(topics, str):
        raw_topics = topics.split(",")
    else:
        raw_topics = topics
    return [topic.strip() for topic in raw_topics if topic and topic.strip()]


def create_user(
    chat_id: int,
    name: str,
    language: str,
    topics: Iterable[str] | str,
    db_path: str | None = None,
) -> dict[str, Any]:
    initialize_database(db_path)
    topics_json = json.dumps(normalize_topics(topics), sort_keys=True)
    with get_connection(db_path) as connection:
        connection.execute(
            """
            INSERT OR IGNORE INTO users (chat_id, name, language, topics, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (chat_id, name, language, topics_json, utc_now()),
        )
        connection.commit()
    user = get_user_by_chat_id(chat_id, db_path)
    if user is None:
        raise RuntimeError(f"Unable to create or load user for chat_id={chat_id}")
    return user


def get_user_by_chat_id(chat_id: int, db_path: str | None = None) -> dict[str, Any] | None:
    initialize_database(db_path)
    with get_connection(db_path) as connection:
        row = connection.execute(
            "SELECT * FROM users WHERE chat_id = ?",
            (chat_id,),
        ).fetchone()
    return row_to_dict(row)


def update_user_preferences(
    chat_id: int,
    language: str | None = None,
    topics: Iterable[str] | str | None = None,
    db_path: str | None = None,
) -> dict[str, Any]:
    user = get_user_by_chat_id(chat_id, db_path)
    if user is None:
        raise ValueError(f"No user found for chat_id={chat_id}")

    new_language = language or user["language"]
    new_topics = normalize_topics(topics) if topics is not None else user["topics"]
    with get_connection(db_path) as connection:
        connection.execute(
            """
            UPDATE users
            SET language = ?, topics = ?
            WHERE chat_id = ?
            """,
            (new_language, json.dumps(new_topics, sort_keys=True), chat_id),
        )
        connection.commit()
    updated = get_user_by_chat_id(chat_id, db_path)
    if updated is None:
        raise RuntimeError(f"Unable to reload user for chat_id={chat_id}")
    return updated


def log_run(
    user_id: int,
    timestamp: str | None = None,
    report_path: str | None = None,
    newsletter_path: str | None = None,
    quality_status: str = "stub",
    selected_counts: dict[str, int] | None = None,
    db_path: str | None = None,
) -> int:
    initialize_database(db_path)
    counts_json = json.dumps(selected_counts or {}, sort_keys=True)
    with get_connection(db_path) as connection:
        cursor = connection.execute(
            """
            INSERT INTO runs (
                user_id, timestamp, report_path, newsletter_path, quality_status, selected_counts
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                timestamp or utc_now(),
                report_path,
                newsletter_path,
                quality_status,
                counts_json,
            ),
        )
        connection.commit()
        return int(cursor.lastrowid)


def update_run_summary(
    run_id: int,
    report_path: str | None = None,
    newsletter_path: str | None = None,
    quality_status: str | None = None,
    selected_counts: dict[str, int] | None = None,
    db_path: str | None = None,
) -> None:
    initialize_database(db_path)
    fields: list[str] = []
    values: list[Any] = []
    if report_path is not None:
        fields.append("report_path = ?")
        values.append(report_path)
    if newsletter_path is not None:
        fields.append("newsletter_path = ?")
        values.append(newsletter_path)
    if quality_status is not None:
        fields.append("quality_status = ?")
        values.append(quality_status)
    if selected_counts is not None:
        fields.append("selected_counts = ?")
        values.append(json.dumps(selected_counts, sort_keys=True))
    if not fields:
        return
    values.append(run_id)
    with get_connection(db_path) as connection:
        connection.execute(
            f"UPDATE runs SET {', '.join(fields)} WHERE id = ?",
            values,
        )
        connection.commit()


def list_runs_for_user(
    user_id: int,
    limit: int = 10,
    db_path: str | None = None,
) -> list[dict[str, Any]]:
    initialize_database(db_path)
    with get_connection(db_path) as connection:
        rows = connection.execute(
            """
            SELECT * FROM runs
            WHERE user_id = ?
            ORDER BY timestamp DESC, id DESC
            LIMIT ?
            """,
            (user_id, limit),
        ).fetchall()
    return [row_to_dict(row) or {} for row in rows]


def article_id_for_url(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:24]


def cache_article(article: dict[str, Any], db_path: str | None = None) -> str:
    initialize_database(db_path)
    url = str(article["url"])
    article_id = str(article.get("id") or article_id_for_url(url))
    with get_connection(db_path) as connection:
        connection.execute(
            """
            INSERT INTO articles (
                id, title, url, category, domain, published_at, summary, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                title = excluded.title,
                url = excluded.url,
                category = excluded.category,
                domain = excluded.domain,
                published_at = excluded.published_at,
                summary = excluded.summary,
                updated_at = excluded.updated_at
            """,
            (
                article_id,
                str(article.get("title", "")),
                url,
                article.get("category"),
                article.get("domain"),
                article.get("published_at"),
                article.get("summary"),
                utc_now(),
            ),
        )
        connection.commit()
    return article_id


def get_article_by_url(url: str, db_path: str | None = None) -> dict[str, Any] | None:
    initialize_database(db_path)
    with get_connection(db_path) as connection:
        row = connection.execute("SELECT * FROM articles WHERE url = ?", (url,)).fetchone()
    return row_to_dict(row)


def set_cache_value(key: str, value: dict[str, Any], db_path: str | None = None) -> None:
    initialize_database(db_path)
    now = utc_now()
    with get_connection(db_path) as connection:
        connection.execute(
            """
            INSERT INTO cache (key, value, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            """,
            (key, json.dumps(value, sort_keys=True), now, now),
        )
        connection.commit()


def get_cache_value(key: str, db_path: str | None = None) -> dict[str, Any] | None:
    initialize_database(db_path)
    with get_connection(db_path) as connection:
        row = connection.execute("SELECT value FROM cache WHERE key = ?", (key,)).fetchone()
    if row is None:
        return None
    return json.loads(row["value"])


def create_feedback(
    user_id: int,
    article_id: str,
    rating: int,
    notes: str = "",
    db_path: str | None = None,
) -> int:
    initialize_database(db_path)
    if rating < 1 or rating > 5:
        raise ValueError("Feedback rating must be between 1 and 5")
    with get_connection(db_path) as connection:
        cursor = connection.execute(
            """
            INSERT INTO feedback (user_id, article_id, rating, notes, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (user_id, article_id, rating, notes, utc_now()),
        )
        connection.commit()
        return int(cursor.lastrowid)


def create_run_feedback(
    user_id: int,
    run_id: int,
    rating: int,
    notes: str = "",
    db_path: str | None = None,
) -> int:
    article_id = f"run:{run_id}"
    cache_article(
        {
            "id": article_id,
            "title": f"Feedback for run {run_id}",
            "url": f"run://{run_id}",
            "category": "run_feedback",
            "domain": "local-run",
            "summary": "Synthetic article record used for run-level feedback.",
        },
        db_path,
    )
    return create_feedback(user_id, article_id, rating, notes, db_path)


def latest_run_for_user(user_id: int, db_path: str | None = None) -> dict[str, Any] | None:
    runs = list_runs_for_user(user_id, limit=1, db_path=db_path)
    return runs[0] if runs else None


def list_feedback_for_user(
    user_id: int,
    limit: int = 500,
    db_path: str | None = None,
) -> list[dict[str, Any]]:
    initialize_database(db_path)
    with get_connection(db_path) as connection:
        rows = connection.execute(
            """
            SELECT f.id, f.user_id, f.article_id, f.rating, f.notes, f.created_at,
                   a.category, a.domain, a.url, a.title
            FROM feedback f
            LEFT JOIN articles a ON a.id = f.article_id
            WHERE f.user_id = ?
            ORDER BY f.created_at DESC, f.id DESC
            LIMIT ?
            """,
            (user_id, limit),
        ).fetchall()
    return [row_to_dict(row) or {} for row in rows]


def feedback_profile_for_user(
    user_id: int,
    limit: int = 500,
    db_path: str | None = None,
) -> dict[str, Any]:
    feedback_rows = list_feedback_for_user(user_id=user_id, limit=limit, db_path=db_path)
    topic_stats: dict[str, dict[str, int]] = {}
    domain_stats: dict[str, dict[str, int]] = {}
    total_like = 0
    total_dislike = 0
    total_neutral = 0

    for row in feedback_rows:
        category = str(row.get("category") or "").strip().lower()
        domain = str(row.get("domain") or "").strip().lower()
        if category == "run_feedback":
            continue
        rating = int(row.get("rating") or 0)
        signal = "neutral"
        if rating >= 4:
            signal = "like"
            total_like += 1
        elif rating <= 2:
            signal = "dislike"
            total_dislike += 1
        else:
            total_neutral += 1

        if category:
            stats = topic_stats.setdefault(category, {"like": 0, "dislike": 0, "neutral": 0})
            stats[signal] += 1
        if domain:
            stats = domain_stats.setdefault(domain, {"like": 0, "dislike": 0, "neutral": 0})
            stats[signal] += 1

    return {
        "user_id": user_id,
        "sample_size": len(feedback_rows),
        "totals": {"like": total_like, "dislike": total_dislike, "neutral": total_neutral},
        "topics": topic_stats,
        "domains": domain_stats,
    }
