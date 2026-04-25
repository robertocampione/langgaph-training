"""Database persistence helpers for Personal Research Agent v4.

Postgres is used when DATABASE_URL is configured; SQLite remains available
for lightweight local simulations and notebooks.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from app import config as app_config

try:
    import psycopg
    from psycopg.rows import dict_row
except ModuleNotFoundError:  # pragma: no cover - optional dependency in sqlite-only mode
    psycopg = None
    dict_row = None


DEFAULT_DB_PATH = app_config.DEFAULT_DB_PATH

JSON_COLUMNS = {
    "topics",
    "selected_counts",
    "payload",
    "value",
    "preferred_channels",
    "explicit_preferences",
    "inferred_preferences",
    "active_context",
    "fact_value",
}

SQLITE_SCHEMA_STATEMENTS = (
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
        article_text_excerpt TEXT,
        article_body_markdown TEXT,
        published_at_confidence REAL DEFAULT 0,
        source_trust_tier INTEGER DEFAULT 0,
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
    """
    CREATE TABLE IF NOT EXISTS user_profiles (
        user_id INTEGER PRIMARY KEY,
        language TEXT NOT NULL,
        home_location TEXT,
        desired_depth TEXT NOT NULL DEFAULT 'standard',
        preferred_channels TEXT NOT NULL DEFAULT '["telegram"]',
        explicit_preferences TEXT NOT NULL DEFAULT '{}',
        inferred_preferences TEXT NOT NULL DEFAULT '{}',
        active_context TEXT NOT NULL DEFAULT '{}',
        profile_version INTEGER NOT NULL DEFAULT 1,
        last_reviewed_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY(user_id) REFERENCES users(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS profile_facts (
        id INTEGER PRIMARY KEY,
        user_id INTEGER NOT NULL,
        fact_key TEXT NOT NULL,
        fact_value TEXT NOT NULL,
        confidence REAL NOT NULL DEFAULT 1.0,
        source TEXT NOT NULL DEFAULT 'user',
        is_explicit INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(user_id, fact_key),
        FOREIGN KEY(user_id) REFERENCES users(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS profile_events (
        id INTEGER PRIMARY KEY,
        user_id INTEGER NOT NULL,
        event_type TEXT NOT NULL,
        payload TEXT NOT NULL,
        created_at TEXT NOT NULL,
        FOREIGN KEY(user_id) REFERENCES users(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS user_topic_graph (
        id INTEGER PRIMARY KEY,
        user_id INTEGER NOT NULL,
        topic TEXT NOT NULL,
        subtopic TEXT NOT NULL,
        weight REAL NOT NULL DEFAULT 0.5,
        enabled INTEGER NOT NULL DEFAULT 1,
        source TEXT NOT NULL DEFAULT 'default',
        updated_at TEXT NOT NULL,
        UNIQUE(user_id, topic, subtopic),
        FOREIGN KEY(user_id) REFERENCES users(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS user_source_prefs (
        id INTEGER PRIMARY KEY,
        user_id INTEGER NOT NULL,
        domain TEXT NOT NULL,
        preference TEXT NOT NULL DEFAULT 'neutral',
        trust_tier INTEGER NOT NULL DEFAULT 0,
        updated_at TEXT NOT NULL,
        UNIQUE(user_id, domain),
        FOREIGN KEY(user_id) REFERENCES users(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS temporary_contexts (
        id INTEGER PRIMARY KEY,
        user_id INTEGER NOT NULL,
        context_type TEXT NOT NULL,
        payload TEXT NOT NULL,
        starts_at TEXT NOT NULL,
        expires_at TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY(user_id) REFERENCES users(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS onboarding_sessions (
        user_id INTEGER PRIMARY KEY,
        step TEXT NOT NULL,
        answers TEXT NOT NULL DEFAULT '{}',
        pending_question TEXT,
        updated_at TEXT NOT NULL,
        created_at TEXT NOT NULL,
        FOREIGN KEY(user_id) REFERENCES users(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS execution_logs (
        id INTEGER PRIMARY KEY,
        user_id INTEGER NOT NULL,
        run_id INTEGER,
        stage TEXT NOT NULL,
        status TEXT NOT NULL,
        message TEXT,
        payload TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL,
        FOREIGN KEY(user_id) REFERENCES users(id),
        FOREIGN KEY(run_id) REFERENCES runs(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS workflow_logs (
        id INTEGER PRIMARY KEY,
        user_id INTEGER NOT NULL,
        run_id INTEGER,
        workflow_name TEXT NOT NULL,
        step TEXT NOT NULL,
        status TEXT NOT NULL,
        payload TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL,
        FOREIGN KEY(user_id) REFERENCES users(id),
        FOREIGN KEY(run_id) REFERENCES runs(id)
    )
    """,
)

POSTGRES_SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS users (
        id BIGSERIAL PRIMARY KEY,
        chat_id BIGINT UNIQUE NOT NULL,
        name TEXT NOT NULL,
        language TEXT NOT NULL,
        topics JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS runs (
        id BIGSERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL REFERENCES users(id),
        timestamp TIMESTAMPTZ NOT NULL,
        report_path TEXT,
        newsletter_path TEXT,
        quality_status TEXT NOT NULL,
        selected_counts JSONB NOT NULL
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
        article_text_excerpt TEXT,
        article_body_markdown TEXT,
        published_at_confidence DOUBLE PRECISION DEFAULT 0,
        source_trust_tier INTEGER DEFAULT 0,
        updated_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS feedback (
        id BIGSERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL REFERENCES users(id),
        article_id TEXT NOT NULL REFERENCES articles(id),
        rating INTEGER NOT NULL,
        notes TEXT,
        created_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS cache (
        key TEXT PRIMARY KEY,
        value JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS user_profiles (
        user_id BIGINT PRIMARY KEY REFERENCES users(id),
        language TEXT NOT NULL,
        home_location TEXT,
        desired_depth TEXT NOT NULL DEFAULT 'standard',
        preferred_channels JSONB NOT NULL DEFAULT '["telegram"]'::jsonb,
        explicit_preferences JSONB NOT NULL DEFAULT '{}'::jsonb,
        inferred_preferences JSONB NOT NULL DEFAULT '{}'::jsonb,
        active_context JSONB NOT NULL DEFAULT '{}'::jsonb,
        profile_version INTEGER NOT NULL DEFAULT 1,
        last_reviewed_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS profile_facts (
        id BIGSERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL REFERENCES users(id),
        fact_key TEXT NOT NULL,
        fact_value JSONB NOT NULL,
        confidence DOUBLE PRECISION NOT NULL DEFAULT 1.0,
        source TEXT NOT NULL DEFAULT 'user',
        is_explicit BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL,
        UNIQUE(user_id, fact_key)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS profile_events (
        id BIGSERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL REFERENCES users(id),
        event_type TEXT NOT NULL,
        payload JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS user_topic_graph (
        id BIGSERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL REFERENCES users(id),
        topic TEXT NOT NULL,
        subtopic TEXT NOT NULL,
        weight DOUBLE PRECISION NOT NULL DEFAULT 0.5,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        source TEXT NOT NULL DEFAULT 'default',
        updated_at TIMESTAMPTZ NOT NULL,
        UNIQUE(user_id, topic, subtopic)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS user_source_prefs (
        id BIGSERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL REFERENCES users(id),
        domain TEXT NOT NULL,
        preference TEXT NOT NULL DEFAULT 'neutral',
        trust_tier INTEGER NOT NULL DEFAULT 0,
        updated_at TIMESTAMPTZ NOT NULL,
        UNIQUE(user_id, domain)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS temporary_contexts (
        id BIGSERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL REFERENCES users(id),
        context_type TEXT NOT NULL,
        payload JSONB NOT NULL,
        starts_at TIMESTAMPTZ NOT NULL,
        expires_at TIMESTAMPTZ NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS onboarding_sessions (
        user_id BIGINT PRIMARY KEY REFERENCES users(id),
        step TEXT NOT NULL,
        answers JSONB NOT NULL DEFAULT '{}'::jsonb,
        pending_question TEXT,
        updated_at TIMESTAMPTZ NOT NULL,
        created_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS execution_logs (
        id BIGSERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL REFERENCES users(id),
        run_id BIGINT REFERENCES runs(id),
        stage TEXT NOT NULL,
        status TEXT NOT NULL,
        message TEXT,
        payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS workflow_logs (
        id BIGSERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL REFERENCES users(id),
        run_id BIGINT REFERENCES runs(id),
        workflow_name TEXT NOT NULL,
        step TEXT NOT NULL,
        status TEXT NOT NULL,
        payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL
    )
    """,
)


@dataclass(frozen=True)
class BackendInfo:
    kind: str  # sqlite|postgres
    db_path: Path | None
    database_url: str | None


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True)


def _json_loads_if_needed(value: Any) -> Any:
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("{") or stripped.startswith("["):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
    return value


def _parse_row(data: dict[str, Any] | None) -> dict[str, Any] | None:
    if data is None:
        return None
    parsed = dict(data)
    for key in list(parsed.keys()):
        if key in JSON_COLUMNS:
            parsed[key] = _json_loads_if_needed(parsed[key])
    return parsed


def get_db_path(db_path: str | None = None) -> Path:
    return app_config.resolve_project_path(db_path or app_config.load_app_config().db_path)


def resolve_backend(db_path: str | None = None, database_url: str | None = None) -> BackendInfo:
    config = app_config.load_app_config()
    effective_database_url = (database_url or config.database_url or "").strip() or None
    if db_path:
        lowered = db_path.strip().lower()
        if lowered.startswith("postgresql://") or lowered.startswith("postgres://"):
            return BackendInfo(kind="postgres", db_path=None, database_url=db_path.strip())
        return BackendInfo(kind="sqlite", db_path=get_db_path(db_path), database_url=None)
    if effective_database_url:
        return BackendInfo(kind="postgres", db_path=None, database_url=effective_database_url)
    return BackendInfo(kind="sqlite", db_path=get_db_path(config.db_path), database_url=None)


def get_connection(db_path: str | None = None, database_url: str | None = None):
    backend = resolve_backend(db_path=db_path, database_url=database_url)
    if backend.kind == "sqlite":
        assert backend.db_path is not None
        backend.db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(backend.db_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    if psycopg is None:
        raise RuntimeError("DATABASE_URL is configured but psycopg is not installed.")
    assert backend.database_url is not None
    return psycopg.connect(backend.database_url, row_factory=dict_row)


def _execute(
    query_sqlite: str,
    query_postgres: str,
    params: tuple[Any, ...] = (),
    db_path: str | None = None,
    database_url: str | None = None,
    fetchone: bool = False,
    fetchall: bool = False,
):
    backend = resolve_backend(db_path=db_path, database_url=database_url)
    with get_connection(db_path=db_path, database_url=database_url) as connection:
        if backend.kind == "sqlite":
            cursor = connection.execute(query_sqlite, params)
            if fetchone:
                row = cursor.fetchone()
                return _parse_row(dict(row) if row is not None else None)
            if fetchall:
                rows = cursor.fetchall()
                return [_parse_row(dict(row)) or {} for row in rows]
            connection.commit()
            return cursor

        with connection.cursor() as cursor:
            cursor.execute(query_postgres, params)
            if fetchone:
                row = cursor.fetchone()
                return _parse_row(dict(row) if row is not None else None)
            if fetchall:
                rows = cursor.fetchall()
                return [_parse_row(dict(row)) or {} for row in rows]
        connection.commit()
        return None


def initialize_database(db_path: str | None = None, database_url: str | None = None) -> None:
    backend = resolve_backend(db_path=db_path, database_url=database_url)
    statements = SQLITE_SCHEMA_STATEMENTS if backend.kind == "sqlite" else POSTGRES_SCHEMA_STATEMENTS
    with get_connection(db_path=db_path, database_url=database_url) as connection:
        if backend.kind == "sqlite":
            for statement in statements:
                connection.execute(statement)
            _run_sqlite_schema_migrations(connection)
            connection.commit()
            return
        with connection.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)
            _run_postgres_schema_migrations(cursor)
        connection.commit()


def _run_sqlite_schema_migrations(connection: sqlite3.Connection) -> None:
    existing_columns = {
        row["name"]
        for row in connection.execute("PRAGMA table_info(articles)").fetchall()
    }
    if "article_text_excerpt" not in existing_columns:
        connection.execute("ALTER TABLE articles ADD COLUMN article_text_excerpt TEXT")
    if "article_body_markdown" not in existing_columns:
        connection.execute("ALTER TABLE articles ADD COLUMN article_body_markdown TEXT")
    if "published_at_confidence" not in existing_columns:
        connection.execute("ALTER TABLE articles ADD COLUMN published_at_confidence REAL DEFAULT 0")
    if "source_trust_tier" not in existing_columns:
        connection.execute("ALTER TABLE articles ADD COLUMN source_trust_tier INTEGER DEFAULT 0")


def _run_postgres_schema_migrations(cursor: Any) -> None:
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'articles'
        """
    )
    existing_columns = {row["column_name"] for row in cursor.fetchall()}
    if "article_text_excerpt" not in existing_columns:
        cursor.execute("ALTER TABLE articles ADD COLUMN article_text_excerpt TEXT")
    if "article_body_markdown" not in existing_columns:
        cursor.execute("ALTER TABLE articles ADD COLUMN article_body_markdown TEXT")
    if "published_at_confidence" not in existing_columns:
        cursor.execute("ALTER TABLE articles ADD COLUMN published_at_confidence DOUBLE PRECISION DEFAULT 0")
    if "source_trust_tier" not in existing_columns:
        cursor.execute("ALTER TABLE articles ADD COLUMN source_trust_tier INTEGER DEFAULT 0")


def normalize_topics(topics: Iterable[str] | str | None) -> list[str]:
    if topics is None:
        return []
    raw_topics = topics.split(",") if isinstance(topics, str) else topics
    return [topic.strip() for topic in raw_topics if topic and topic.strip()]


def create_user(
    chat_id: int,
    name: str,
    language: str,
    topics: Iterable[str] | str,
    db_path: str | None = None,
    database_url: str | None = None,
) -> dict[str, Any]:
    initialize_database(db_path=db_path, database_url=database_url)
    topics_json = _json_dumps(normalize_topics(topics))
    _execute(
        """
        INSERT OR IGNORE INTO users (chat_id, name, language, topics, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        """
        INSERT INTO users (chat_id, name, language, topics, created_at)
        VALUES (%s, %s, %s, %s::jsonb, %s::timestamptz)
        ON CONFLICT (chat_id) DO NOTHING
        """,
        (chat_id, name, language, topics_json, utc_now()),
        db_path=db_path,
        database_url=database_url,
    )
    user = get_user_by_chat_id(chat_id, db_path=db_path, database_url=database_url)
    if user is None:
        raise RuntimeError(f"Unable to create or load user for chat_id={chat_id}")
    return user


def get_user_by_chat_id(
    chat_id: int,
    db_path: str | None = None,
    database_url: str | None = None,
) -> dict[str, Any] | None:
    initialize_database(db_path=db_path, database_url=database_url)
    return _execute(
        "SELECT * FROM users WHERE chat_id = ?",
        "SELECT * FROM users WHERE chat_id = %s",
        (chat_id,),
        db_path=db_path,
        database_url=database_url,
        fetchone=True,
    )


def update_user_preferences(
    chat_id: int,
    language: str | None = None,
    topics: Iterable[str] | str | None = None,
    db_path: str | None = None,
    database_url: str | None = None,
) -> dict[str, Any]:
    user = get_user_by_chat_id(chat_id, db_path=db_path, database_url=database_url)
    if user is None:
        raise ValueError(f"No user found for chat_id={chat_id}")
    new_language = language or str(user["language"])
    new_topics = normalize_topics(topics) if topics is not None else list(user.get("topics") or [])
    _execute(
        """
        UPDATE users
        SET language = ?, topics = ?
        WHERE chat_id = ?
        """,
        """
        UPDATE users
        SET language = %s, topics = %s::jsonb
        WHERE chat_id = %s
        """,
        (new_language, _json_dumps(new_topics), chat_id),
        db_path=db_path,
        database_url=database_url,
    )
    updated = get_user_by_chat_id(chat_id, db_path=db_path, database_url=database_url)
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
    database_url: str | None = None,
) -> int:
    initialize_database(db_path=db_path, database_url=database_url)
    counts_json = _json_dumps(selected_counts or {})
    backend = resolve_backend(db_path=db_path, database_url=database_url)
    with get_connection(db_path=db_path, database_url=database_url) as connection:
        if backend.kind == "sqlite":
            cursor = connection.execute(
                """
                INSERT INTO runs (
                    user_id, timestamp, report_path, newsletter_path, quality_status, selected_counts
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (user_id, timestamp or utc_now(), report_path, newsletter_path, quality_status, counts_json),
            )
            connection.commit()
            return int(cursor.lastrowid)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO runs (
                    user_id, timestamp, report_path, newsletter_path, quality_status, selected_counts
                )
                VALUES (%s, %s::timestamptz, %s, %s, %s, %s::jsonb)
                RETURNING id
                """,
                (user_id, timestamp or utc_now(), report_path, newsletter_path, quality_status, counts_json),
            )
            row = cursor.fetchone()
        connection.commit()
        return int(row["id"]) if row else 0


def update_run_summary(
    run_id: int,
    report_path: str | None = None,
    newsletter_path: str | None = None,
    quality_status: str | None = None,
    selected_counts: dict[str, int] | None = None,
    db_path: str | None = None,
    database_url: str | None = None,
) -> None:
    fields: list[str] = []
    values: list[Any] = []
    backend = resolve_backend(db_path=db_path, database_url=database_url)
    if report_path is not None:
        fields.append("report_path = " + ("?" if backend.kind == "sqlite" else "%s"))
        values.append(report_path)
    if newsletter_path is not None:
        fields.append("newsletter_path = " + ("?" if backend.kind == "sqlite" else "%s"))
        values.append(newsletter_path)
    if quality_status is not None:
        fields.append("quality_status = " + ("?" if backend.kind == "sqlite" else "%s"))
        values.append(quality_status)
    if selected_counts is not None:
        if backend.kind == "sqlite":
            fields.append("selected_counts = ?")
            values.append(_json_dumps(selected_counts))
        else:
            fields.append("selected_counts = %s::jsonb")
            values.append(_json_dumps(selected_counts))
    if not fields:
        return
    values.append(run_id)
    placeholder = "?" if backend.kind == "sqlite" else "%s"
    query = f"UPDATE runs SET {', '.join(fields)} WHERE id = {placeholder}"
    _execute(
        query,
        query,
        tuple(values),
        db_path=db_path,
        database_url=database_url,
    )


def list_runs_for_user(
    user_id: int,
    limit: int = 10,
    db_path: str | None = None,
    database_url: str | None = None,
) -> list[dict[str, Any]]:
    initialize_database(db_path=db_path, database_url=database_url)
    return _execute(
        """
        SELECT * FROM runs
        WHERE user_id = ?
        ORDER BY timestamp DESC, id DESC
        LIMIT ?
        """,
        """
        SELECT * FROM runs
        WHERE user_id = %s
        ORDER BY timestamp DESC, id DESC
        LIMIT %s
        """,
        (user_id, limit),
        db_path=db_path,
        database_url=database_url,
        fetchall=True,
    )


def article_id_for_url(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:24]


def cache_article(article: dict[str, Any], db_path: str | None = None, database_url: str | None = None) -> str:
    initialize_database(db_path=db_path, database_url=database_url)
    url = str(article["url"])
    article_id = str(article.get("id") or article_id_for_url(url))
    _execute(
        """
        INSERT INTO articles (
            id, title, url, category, domain, published_at, summary, article_text_excerpt, article_body_markdown,
            published_at_confidence, source_trust_tier, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            title = excluded.title,
            url = excluded.url,
            category = excluded.category,
            domain = excluded.domain,
            published_at = excluded.published_at,
            summary = excluded.summary,
            article_text_excerpt = excluded.article_text_excerpt,
            article_body_markdown = excluded.article_body_markdown,
            published_at_confidence = excluded.published_at_confidence,
            source_trust_tier = excluded.source_trust_tier,
            updated_at = excluded.updated_at
        """,
        """
        INSERT INTO articles (
            id, title, url, category, domain, published_at, summary, article_text_excerpt, article_body_markdown,
            published_at_confidence, source_trust_tier, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::timestamptz)
        ON CONFLICT(id) DO UPDATE SET
            title = EXCLUDED.title,
            url = EXCLUDED.url,
            category = EXCLUDED.category,
            domain = EXCLUDED.domain,
            published_at = EXCLUDED.published_at,
            summary = EXCLUDED.summary,
            article_text_excerpt = EXCLUDED.article_text_excerpt,
            article_body_markdown = EXCLUDED.article_body_markdown,
            published_at_confidence = EXCLUDED.published_at_confidence,
            source_trust_tier = EXCLUDED.source_trust_tier,
            updated_at = EXCLUDED.updated_at
        """,
        (
            article_id,
            str(article.get("title", "")),
            url,
            article.get("category"),
            article.get("domain"),
            article.get("published_at"),
            article.get("summary"),
            article.get("article_text_excerpt"),
            article.get("article_body_markdown"),
            float(article.get("published_at_confidence", 0) or 0),
            int(article.get("source_trust_tier", 0) or 0),
            utc_now(),
        ),
        db_path=db_path,
        database_url=database_url,
    )
    return article_id


def get_article_by_url(url: str, db_path: str | None = None, database_url: str | None = None) -> dict[str, Any] | None:
    initialize_database(db_path=db_path, database_url=database_url)
    return _execute(
        "SELECT * FROM articles WHERE url = ?",
        "SELECT * FROM articles WHERE url = %s",
        (url,),
        db_path=db_path,
        database_url=database_url,
        fetchone=True,
    )


def set_cache_value(key: str, value: dict[str, Any], db_path: str | None = None, database_url: str | None = None) -> None:
    initialize_database(db_path=db_path, database_url=database_url)
    now = utc_now()
    value_json = _json_dumps(value)
    _execute(
        """
        INSERT INTO cache (key, value, created_at, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            updated_at = excluded.updated_at
        """,
        """
        INSERT INTO cache (key, value, created_at, updated_at)
        VALUES (%s, %s::jsonb, %s::timestamptz, %s::timestamptz)
        ON CONFLICT(key) DO UPDATE SET
            value = EXCLUDED.value,
            updated_at = EXCLUDED.updated_at
        """,
        (key, value_json, now, now),
        db_path=db_path,
        database_url=database_url,
    )


def get_cache_value(key: str, db_path: str | None = None, database_url: str | None = None) -> dict[str, Any] | None:
    initialize_database(db_path=db_path, database_url=database_url)
    row = _execute(
        "SELECT value FROM cache WHERE key = ?",
        "SELECT value FROM cache WHERE key = %s",
        (key,),
        db_path=db_path,
        database_url=database_url,
        fetchone=True,
    )
    if row is None:
        return None
    value = row.get("value")
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return None
    return None


def create_feedback(
    user_id: int,
    article_id: str,
    rating: int,
    notes: str = "",
    db_path: str | None = None,
    database_url: str | None = None,
) -> int:
    initialize_database(db_path=db_path, database_url=database_url)
    if rating < 1 or rating > 5:
        raise ValueError("Feedback rating must be between 1 and 5")
    backend = resolve_backend(db_path=db_path, database_url=database_url)
    with get_connection(db_path=db_path, database_url=database_url) as connection:
        if backend.kind == "sqlite":
            cursor = connection.execute(
                """
                INSERT INTO feedback (user_id, article_id, rating, notes, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (user_id, article_id, rating, notes, utc_now()),
            )
            connection.commit()
            return int(cursor.lastrowid)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO feedback (user_id, article_id, rating, notes, created_at)
                VALUES (%s, %s, %s, %s, %s::timestamptz)
                RETURNING id
                """,
                (user_id, article_id, rating, notes, utc_now()),
            )
            row = cursor.fetchone()
        connection.commit()
        return int(row["id"]) if row else 0


def create_run_feedback(
    user_id: int,
    run_id: int,
    rating: int,
    notes: str = "",
    db_path: str | None = None,
    database_url: str | None = None,
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
        db_path=db_path,
        database_url=database_url,
    )
    return create_feedback(user_id, article_id, rating, notes, db_path=db_path, database_url=database_url)


def latest_run_for_user(
    user_id: int,
    db_path: str | None = None,
    database_url: str | None = None,
) -> dict[str, Any] | None:
    runs = list_runs_for_user(user_id, limit=1, db_path=db_path, database_url=database_url)
    return runs[0] if runs else None


def list_feedback_for_user(
    user_id: int,
    limit: int = 500,
    db_path: str | None = None,
    database_url: str | None = None,
) -> list[dict[str, Any]]:
    initialize_database(db_path=db_path, database_url=database_url)
    return _execute(
        """
        SELECT f.id, f.user_id, f.article_id, f.rating, f.notes, f.created_at,
               a.category, a.domain, a.url, a.title
        FROM feedback f
        LEFT JOIN articles a ON a.id = f.article_id
        WHERE f.user_id = ?
        ORDER BY f.created_at DESC, f.id DESC
        LIMIT ?
        """,
        """
        SELECT f.id, f.user_id, f.article_id, f.rating, f.notes, f.created_at,
               a.category, a.domain, a.url, a.title
        FROM feedback f
        LEFT JOIN articles a ON a.id = f.article_id
        WHERE f.user_id = %s
        ORDER BY f.created_at DESC, f.id DESC
        LIMIT %s
        """,
        (user_id, limit),
        db_path=db_path,
        database_url=database_url,
        fetchall=True,
    )


def feedback_profile_for_user(
    user_id: int,
    limit: int = 500,
    db_path: str | None = None,
    database_url: str | None = None,
) -> dict[str, Any]:
    feedback_rows = list_feedback_for_user(user_id=user_id, limit=limit, db_path=db_path, database_url=database_url)
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


def ensure_profile_for_user(
    user_id: int,
    language: str,
    topics: list[str],
    db_path: str | None = None,
    database_url: str | None = None,
) -> None:
    initialize_database(db_path=db_path, database_url=database_url)
    now = utc_now()
    _execute(
        """
        INSERT OR IGNORE INTO user_profiles (
            user_id, language, desired_depth, preferred_channels, explicit_preferences, inferred_preferences,
            active_context, profile_version, created_at, updated_at
        )
        VALUES (?, ?, 'standard', ?, ?, ?, ?, 1, ?, ?)
        """,
        """
        INSERT INTO user_profiles (
            user_id, language, desired_depth, preferred_channels, explicit_preferences, inferred_preferences,
            active_context, profile_version, created_at, updated_at
        )
        VALUES (%s, %s, 'standard', %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, 1, %s::timestamptz, %s::timestamptz)
        ON CONFLICT (user_id) DO NOTHING
        """,
        (
            user_id,
            language,
            _json_dumps(["telegram"]),
            _json_dumps({"topics": topics}),
            _json_dumps({}),
            _json_dumps({}),
            now,
            now,
        ),
        db_path=db_path,
        database_url=database_url,
    )


def get_profile(user_id: int, db_path: str | None = None, database_url: str | None = None) -> dict[str, Any] | None:
    return _execute(
        "SELECT * FROM user_profiles WHERE user_id = ?",
        "SELECT * FROM user_profiles WHERE user_id = %s",
        (user_id,),
        db_path=db_path,
        database_url=database_url,
        fetchone=True,
    )


def upsert_profile(
    user_id: int,
    language: str | None = None,
    home_location: str | None = None,
    desired_depth: str | None = None,
    preferred_channels: list[str] | None = None,
    explicit_preferences: dict[str, Any] | None = None,
    inferred_preferences: dict[str, Any] | None = None,
    active_context: dict[str, Any] | None = None,
    db_path: str | None = None,
    database_url: str | None = None,
) -> dict[str, Any]:
    current = get_profile(user_id, db_path=db_path, database_url=database_url) or {}
    now = utc_now()
    merged_language = language or str(current.get("language") or "en")
    merged_home_location = home_location if home_location is not None else current.get("home_location")
    merged_depth = desired_depth or str(current.get("desired_depth") or "standard")
    merged_channels = preferred_channels if preferred_channels is not None else list(current.get("preferred_channels") or ["telegram"])
    merged_explicit = explicit_preferences if explicit_preferences is not None else dict(current.get("explicit_preferences") or {})
    merged_inferred = inferred_preferences if inferred_preferences is not None else dict(current.get("inferred_preferences") or {})
    merged_active_context = active_context if active_context is not None else dict(current.get("active_context") or {})
    next_version = int(current.get("profile_version") or 0) + 1
    _execute(
        """
        INSERT INTO user_profiles (
            user_id, language, home_location, desired_depth, preferred_channels,
            explicit_preferences, inferred_preferences, active_context,
            profile_version, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            language = excluded.language,
            home_location = excluded.home_location,
            desired_depth = excluded.desired_depth,
            preferred_channels = excluded.preferred_channels,
            explicit_preferences = excluded.explicit_preferences,
            inferred_preferences = excluded.inferred_preferences,
            active_context = excluded.active_context,
            profile_version = excluded.profile_version,
            updated_at = excluded.updated_at
        """,
        """
        INSERT INTO user_profiles (
            user_id, language, home_location, desired_depth, preferred_channels,
            explicit_preferences, inferred_preferences, active_context,
            profile_version, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s, %s::timestamptz, %s::timestamptz)
        ON CONFLICT(user_id) DO UPDATE SET
            language = EXCLUDED.language,
            home_location = EXCLUDED.home_location,
            desired_depth = EXCLUDED.desired_depth,
            preferred_channels = EXCLUDED.preferred_channels,
            explicit_preferences = EXCLUDED.explicit_preferences,
            inferred_preferences = EXCLUDED.inferred_preferences,
            active_context = EXCLUDED.active_context,
            profile_version = EXCLUDED.profile_version,
            updated_at = EXCLUDED.updated_at
        """,
        (
            user_id,
            merged_language,
            merged_home_location,
            merged_depth,
            _json_dumps(merged_channels),
            _json_dumps(merged_explicit),
            _json_dumps(merged_inferred),
            _json_dumps(merged_active_context),
            next_version,
            str(current.get("created_at") or now),
            now,
        ),
        db_path=db_path,
        database_url=database_url,
    )
    return get_profile(user_id, db_path=db_path, database_url=database_url) or {}


def append_profile_event(
    user_id: int,
    event_type: str,
    payload: dict[str, Any],
    db_path: str | None = None,
    database_url: str | None = None,
) -> None:
    _execute(
        """
        INSERT INTO profile_events (user_id, event_type, payload, created_at)
        VALUES (?, ?, ?, ?)
        """,
        """
        INSERT INTO profile_events (user_id, event_type, payload, created_at)
        VALUES (%s, %s, %s::jsonb, %s::timestamptz)
        """,
        (user_id, event_type, _json_dumps(payload), utc_now()),
        db_path=db_path,
        database_url=database_url,
    )


def upsert_profile_fact(
    user_id: int,
    fact_key: str,
    fact_value: Any,
    confidence: float = 1.0,
    source: str = "user",
    is_explicit: bool = True,
    db_path: str | None = None,
    database_url: str | None = None,
) -> None:
    now = utc_now()
    _execute(
        """
        INSERT INTO profile_facts (user_id, fact_key, fact_value, confidence, source, is_explicit, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id, fact_key) DO UPDATE SET
            fact_value = excluded.fact_value,
            confidence = excluded.confidence,
            source = excluded.source,
            is_explicit = excluded.is_explicit,
            updated_at = excluded.updated_at
        """,
        """
        INSERT INTO profile_facts (user_id, fact_key, fact_value, confidence, source, is_explicit, created_at, updated_at)
        VALUES (%s, %s, %s::jsonb, %s, %s, %s, %s::timestamptz, %s::timestamptz)
        ON CONFLICT(user_id, fact_key) DO UPDATE SET
            fact_value = EXCLUDED.fact_value,
            confidence = EXCLUDED.confidence,
            source = EXCLUDED.source,
            is_explicit = EXCLUDED.is_explicit,
            updated_at = EXCLUDED.updated_at
        """,
        (user_id, fact_key, _json_dumps(fact_value), confidence, source, bool(is_explicit), now, now),
        db_path=db_path,
        database_url=database_url,
    )


def list_profile_facts(
    user_id: int,
    db_path: str | None = None,
    database_url: str | None = None,
) -> list[dict[str, Any]]:
    return _execute(
        "SELECT id, fact_key, fact_value, confidence, source, is_explicit, updated_at FROM profile_facts WHERE user_id = ? ORDER BY id ASC",
        "SELECT id, fact_key, fact_value, confidence, source, is_explicit, updated_at FROM profile_facts WHERE user_id = %s ORDER BY id ASC",
        (user_id,),
        db_path=db_path,
        database_url=database_url,
        fetchall=True,
    )


def delete_profile_fact(
    user_id: int,
    fact_id: int,
    db_path: str | None = None,
    database_url: str | None = None,
) -> None:
    _execute(
        "DELETE FROM profile_facts WHERE user_id = ? AND id = ?",
        "DELETE FROM profile_facts WHERE user_id = %s AND id = %s",
        (user_id, fact_id),
        db_path=db_path,
        database_url=database_url,
    )


def list_profile_versions(user_id: int, limit: int = 50, db_path: str | None = None, database_url: str | None = None) -> list[dict[str, Any]]:
    return _execute(
        """
        SELECT id, event_type, payload, created_at
        FROM profile_events
        WHERE user_id = ?
        ORDER BY id DESC
        LIMIT ?
        """,
        """
        SELECT id, event_type, payload, created_at
        FROM profile_events
        WHERE user_id = %s
        ORDER BY id DESC
        LIMIT %s
        """,
        (user_id, limit),
        db_path=db_path,
        database_url=database_url,
        fetchall=True,
    )


def set_topic_weight(
    user_id: int,
    topic: str,
    subtopic: str,
    weight: float,
    enabled: bool = True,
    source: str = "manual",
    db_path: str | None = None,
    database_url: str | None = None,
) -> None:
    _execute(
        """
        INSERT INTO user_topic_graph (user_id, topic, subtopic, weight, enabled, source, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id, topic, subtopic) DO UPDATE SET
            weight = excluded.weight,
            enabled = excluded.enabled,
            source = excluded.source,
            updated_at = excluded.updated_at
        """,
        """
        INSERT INTO user_topic_graph (user_id, topic, subtopic, weight, enabled, source, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s::timestamptz)
        ON CONFLICT(user_id, topic, subtopic) DO UPDATE SET
            weight = EXCLUDED.weight,
            enabled = EXCLUDED.enabled,
            source = EXCLUDED.source,
            updated_at = EXCLUDED.updated_at
        """,
        (user_id, topic, subtopic, weight, bool(enabled), source, utc_now()),
        db_path=db_path,
        database_url=database_url,
    )


def list_topic_weights(user_id: int, topic: str | None = None, db_path: str | None = None, database_url: str | None = None) -> list[dict[str, Any]]:
    if topic:
        return _execute(
            """
            SELECT * FROM user_topic_graph
            WHERE user_id = ? AND topic = ?
            ORDER BY weight DESC, subtopic ASC
            """,
            """
            SELECT * FROM user_topic_graph
            WHERE user_id = %s AND topic = %s
            ORDER BY weight DESC, subtopic ASC
            """,
            (user_id, topic),
            db_path=db_path,
            database_url=database_url,
            fetchall=True,
        )
    return _execute(
        """
        SELECT * FROM user_topic_graph
        WHERE user_id = ?
        ORDER BY topic ASC, weight DESC, subtopic ASC
        """,
        """
        SELECT * FROM user_topic_graph
        WHERE user_id = %s
        ORDER BY topic ASC, weight DESC, subtopic ASC
        """,
        (user_id,),
        db_path=db_path,
        database_url=database_url,
        fetchall=True,
    )


def set_source_preference(
    user_id: int,
    domain: str,
    preference: str,
    trust_tier: int = 0,
    db_path: str | None = None,
    database_url: str | None = None,
) -> None:
    normalized_preference = preference.strip().lower()
    if normalized_preference not in {"allow", "deny", "neutral"}:
        raise ValueError("Source preference must be one of: allow, deny, neutral")
    _execute(
        """
        INSERT INTO user_source_prefs (user_id, domain, preference, trust_tier, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(user_id, domain) DO UPDATE SET
            preference = excluded.preference,
            trust_tier = excluded.trust_tier,
            updated_at = excluded.updated_at
        """,
        """
        INSERT INTO user_source_prefs (user_id, domain, preference, trust_tier, updated_at)
        VALUES (%s, %s, %s, %s, %s::timestamptz)
        ON CONFLICT(user_id, domain) DO UPDATE SET
            preference = EXCLUDED.preference,
            trust_tier = EXCLUDED.trust_tier,
            updated_at = EXCLUDED.updated_at
        """,
        (user_id, domain.strip().lower(), normalized_preference, trust_tier, utc_now()),
        db_path=db_path,
        database_url=database_url,
    )


def list_source_preferences(user_id: int, db_path: str | None = None, database_url: str | None = None) -> list[dict[str, Any]]:
    return _execute(
        """
        SELECT * FROM user_source_prefs
        WHERE user_id = ?
        ORDER BY domain ASC
        """,
        """
        SELECT * FROM user_source_prefs
        WHERE user_id = %s
        ORDER BY domain ASC
        """,
        (user_id,),
        db_path=db_path,
        database_url=database_url,
        fetchall=True,
    )


def create_temporary_context(
    user_id: int,
    context_type: str,
    payload: dict[str, Any],
    starts_at: str,
    expires_at: str,
    db_path: str | None = None,
    database_url: str | None = None,
) -> int:
    backend = resolve_backend(db_path=db_path, database_url=database_url)
    with get_connection(db_path=db_path, database_url=database_url) as connection:
        if backend.kind == "sqlite":
            cursor = connection.execute(
                """
                INSERT INTO temporary_contexts (
                    user_id, context_type, payload, starts_at, expires_at, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'active', ?, ?)
                """,
                (user_id, context_type, _json_dumps(payload), starts_at, expires_at, utc_now(), utc_now()),
            )
            connection.commit()
            return int(cursor.lastrowid)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO temporary_contexts (
                    user_id, context_type, payload, starts_at, expires_at, status, created_at, updated_at
                ) VALUES (%s, %s, %s::jsonb, %s::timestamptz, %s::timestamptz, 'active', %s::timestamptz, %s::timestamptz)
                RETURNING id
                """,
                (user_id, context_type, _json_dumps(payload), starts_at, expires_at, utc_now(), utc_now()),
            )
            row = cursor.fetchone()
        connection.commit()
        return int(row["id"]) if row else 0


def expire_temporary_contexts(
    user_id: int,
    at_time_iso: str | None = None,
    db_path: str | None = None,
    database_url: str | None = None,
) -> None:
    reference = at_time_iso or utc_now()
    _execute(
        """
        UPDATE temporary_contexts
        SET status = 'expired', updated_at = ?
        WHERE user_id = ? AND status = 'active' AND expires_at < ?
        """,
        """
        UPDATE temporary_contexts
        SET status = 'expired', updated_at = %s::timestamptz
        WHERE user_id = %s AND status = 'active' AND expires_at < %s::timestamptz
        """,
        (utc_now(), user_id, reference),
        db_path=db_path,
        database_url=database_url,
    )


def clear_temporary_contexts(
    user_id: int,
    context_type: str | None = None,
    db_path: str | None = None,
    database_url: str | None = None,
) -> int:
    backend = resolve_backend(db_path=db_path, database_url=database_url)
    with get_connection(db_path=db_path, database_url=database_url) as connection:
        if backend.kind == "sqlite":
            if context_type:
                cursor = connection.execute(
                    """
                    UPDATE temporary_contexts
                    SET status = 'cancelled', updated_at = ?
                    WHERE user_id = ? AND status = 'active' AND context_type = ?
                    """,
                    (utc_now(), user_id, context_type),
                )
            else:
                cursor = connection.execute(
                    """
                    UPDATE temporary_contexts
                    SET status = 'cancelled', updated_at = ?
                    WHERE user_id = ? AND status = 'active'
                    """,
                    (utc_now(), user_id),
                )
            connection.commit()
            return int(cursor.rowcount)

        with connection.cursor() as cursor:
            if context_type:
                cursor.execute(
                    """
                    UPDATE temporary_contexts
                    SET status = 'cancelled', updated_at = %s::timestamptz
                    WHERE user_id = %s AND status = 'active' AND context_type = %s
                    """,
                    (utc_now(), user_id, context_type),
                )
            else:
                cursor.execute(
                    """
                    UPDATE temporary_contexts
                    SET status = 'cancelled', updated_at = %s::timestamptz
                    WHERE user_id = %s AND status = 'active'
                    """,
                    (utc_now(), user_id),
                )
            count = int(cursor.rowcount)
        connection.commit()
        return count


def list_active_temporary_contexts(user_id: int, db_path: str | None = None, database_url: str | None = None) -> list[dict[str, Any]]:
    expire_temporary_contexts(user_id=user_id, db_path=db_path, database_url=database_url)
    return _execute(
        """
        SELECT * FROM temporary_contexts
        WHERE user_id = ? AND status = 'active'
        ORDER BY starts_at DESC, id DESC
        """,
        """
        SELECT * FROM temporary_contexts
        WHERE user_id = %s AND status = 'active'
        ORDER BY starts_at DESC, id DESC
        """,
        (user_id,),
        db_path=db_path,
        database_url=database_url,
        fetchall=True,
    )


def get_onboarding_session(user_id: int, db_path: str | None = None, database_url: str | None = None) -> dict[str, Any] | None:
    return _execute(
        "SELECT * FROM onboarding_sessions WHERE user_id = ?",
        "SELECT * FROM onboarding_sessions WHERE user_id = %s",
        (user_id,),
        db_path=db_path,
        database_url=database_url,
        fetchone=True,
    )


def upsert_onboarding_session(
    user_id: int,
    step: str,
    answers: dict[str, Any],
    pending_question: str | None,
    db_path: str | None = None,
    database_url: str | None = None,
) -> None:
    now = utc_now()
    _execute(
        """
        INSERT INTO onboarding_sessions (user_id, step, answers, pending_question, updated_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            step = excluded.step,
            answers = excluded.answers,
            pending_question = excluded.pending_question,
            updated_at = excluded.updated_at
        """,
        """
        INSERT INTO onboarding_sessions (user_id, step, answers, pending_question, updated_at, created_at)
        VALUES (%s, %s, %s::jsonb, %s, %s::timestamptz, %s::timestamptz)
        ON CONFLICT(user_id) DO UPDATE SET
            step = EXCLUDED.step,
            answers = EXCLUDED.answers,
            pending_question = EXCLUDED.pending_question,
            updated_at = EXCLUDED.updated_at
        """,
        (user_id, step, _json_dumps(answers), pending_question, now, now),
        db_path=db_path,
        database_url=database_url,
    )


def clear_onboarding_session(user_id: int, db_path: str | None = None, database_url: str | None = None) -> None:
    _execute(
        "DELETE FROM onboarding_sessions WHERE user_id = ?",
        "DELETE FROM onboarding_sessions WHERE user_id = %s",
        (user_id,),
        db_path=db_path,
        database_url=database_url,
    )


def append_execution_log(
    user_id: int,
    run_id: int | None,
    stage: str,
    status: str,
    message: str = "",
    payload: dict[str, Any] | None = None,
    db_path: str | None = None,
    database_url: str | None = None,
) -> None:
    _execute(
        """
        INSERT INTO execution_logs (user_id, run_id, stage, status, message, payload, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        """
        INSERT INTO execution_logs (user_id, run_id, stage, status, message, payload, created_at)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::timestamptz)
        """,
        (user_id, run_id, stage, status, message, _json_dumps(payload or {}), utc_now()),
        db_path=db_path,
        database_url=database_url,
    )


def list_execution_logs(
    user_id: int,
    limit: int = 100,
    db_path: str | None = None,
    database_url: str | None = None,
) -> list[dict[str, Any]]:
    return _execute(
        """
        SELECT * FROM execution_logs
        WHERE user_id = ?
        ORDER BY id DESC
        LIMIT ?
        """,
        """
        SELECT * FROM execution_logs
        WHERE user_id = %s
        ORDER BY id DESC
        LIMIT %s
        """,
        (user_id, limit),
        db_path=db_path,
        database_url=database_url,
        fetchall=True,
    )


def append_workflow_log(
    user_id: int,
    run_id: int | None,
    workflow_name: str,
    step: str,
    status: str,
    payload: dict[str, Any] | None = None,
    db_path: str | None = None,
    database_url: str | None = None,
) -> None:
    _execute(
        """
        INSERT INTO workflow_logs (user_id, run_id, workflow_name, step, status, payload, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        """
        INSERT INTO workflow_logs (user_id, run_id, workflow_name, step, status, payload, created_at)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::timestamptz)
        """,
        (user_id, run_id, workflow_name, step, status, _json_dumps(payload or {}), utc_now()),
        db_path=db_path,
        database_url=database_url,
    )


def list_workflow_logs(
    user_id: int,
    run_id: int | None = None,
    limit: int = 100,
    db_path: str | None = None,
    database_url: str | None = None,
) -> list[dict[str, Any]]:
    if run_id is None:
        return _execute(
            """
            SELECT * FROM workflow_logs
            WHERE user_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            """
            SELECT * FROM workflow_logs
            WHERE user_id = %s
            ORDER BY id DESC
            LIMIT %s
            """,
            (user_id, limit),
            db_path=db_path,
            database_url=database_url,
            fetchall=True,
        )
    return _execute(
        """
        SELECT * FROM workflow_logs
        WHERE user_id = ? AND run_id = ?
        ORDER BY id DESC
        LIMIT ?
        """,
        """
        SELECT * FROM workflow_logs
        WHERE user_id = %s AND run_id = %s
        ORDER BY id DESC
        LIMIT %s
        """,
        (user_id, run_id, limit),
        db_path=db_path,
        database_url=database_url,
        fetchall=True,
    )
