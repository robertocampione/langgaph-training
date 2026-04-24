#!/usr/bin/env python3
"""Migrate v4 data from SQLite to Postgres with idempotent semantics."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app import config as app_config  # noqa: E402
from app import db  # noqa: E402

try:
    import psycopg
    from psycopg.rows import dict_row
except ModuleNotFoundError as exc:  # pragma: no cover
    raise SystemExit("psycopg is required for SQLite->Postgres migration.") from exc


def sqlite_connection(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    return connection


def fetchall_sqlite(connection: sqlite3.Connection, query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    rows = connection.execute(query, params).fetchall()
    return [dict(row) for row in rows]


def json_or_default(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default
    return default


def row_exists_pg(connection: psycopg.Connection, query: str, params: tuple[Any, ...]) -> bool:
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(query, params)
        row = cursor.fetchone()
    return bool(row and row.get("exists"))


def migrate(sqlite_path: Path, database_url: str) -> dict[str, int]:
    db.initialize_database(db_path=None, database_url=database_url)
    migrated = {"users": 0, "runs": 0, "articles": 0, "feedback": 0, "cache": 0}

    with sqlite_connection(sqlite_path) as sqlite_conn, psycopg.connect(database_url, row_factory=dict_row) as pg_conn:
        sqlite_users = fetchall_sqlite(sqlite_conn, "SELECT * FROM users ORDER BY id ASC")
        user_id_map: dict[int, int] = {}
        for row in sqlite_users:
            topics = json_or_default(row.get("topics"), [])
            existed_before = row_exists_pg(
                pg_conn,
                "SELECT EXISTS(SELECT 1 FROM users WHERE chat_id = %s) AS exists",
                (int(row["chat_id"]),),
            )
            with pg_conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO users (chat_id, name, language, topics, created_at)
                    VALUES (%s, %s, %s, %s::jsonb, %s::timestamptz)
                    ON CONFLICT (chat_id) DO UPDATE SET
                        name = EXCLUDED.name,
                        language = EXCLUDED.language,
                        topics = EXCLUDED.topics
                    RETURNING id
                    """,
                    (
                        int(row["chat_id"]),
                        str(row["name"]),
                        str(row["language"]),
                        json.dumps(topics, sort_keys=True),
                        str(row["created_at"]),
                    ),
                )
                pg_user = cursor.fetchone()
            if pg_user is None:
                continue
            user_id_map[int(row["id"])] = int(pg_user["id"])
            if not existed_before:
                migrated["users"] += 1

        sqlite_runs = fetchall_sqlite(sqlite_conn, "SELECT * FROM runs ORDER BY id ASC")
        for row in sqlite_runs:
            mapped_user_id = user_id_map.get(int(row["user_id"]))
            if mapped_user_id is None:
                continue
            exists = row_exists_pg(
                pg_conn,
                """
                SELECT EXISTS(
                    SELECT 1 FROM runs
                    WHERE user_id = %s
                      AND timestamp = %s::timestamptz
                      AND COALESCE(report_path, '') = COALESCE(%s, '')
                      AND COALESCE(newsletter_path, '') = COALESCE(%s, '')
                      AND quality_status = %s
                ) AS exists
                """,
                (
                    mapped_user_id,
                    str(row["timestamp"]),
                    row["report_path"],
                    row["newsletter_path"],
                    str(row["quality_status"]),
                ),
            )
            if exists:
                continue
            with pg_conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO runs (user_id, timestamp, report_path, newsletter_path, quality_status, selected_counts)
                    VALUES (%s, %s::timestamptz, %s, %s, %s, %s::jsonb)
                    """,
                    (
                        mapped_user_id,
                        str(row["timestamp"]),
                        row["report_path"],
                        row["newsletter_path"],
                        str(row["quality_status"]),
                        json.dumps(json_or_default(row.get("selected_counts"), {}), sort_keys=True),
                    ),
                )
            migrated["runs"] += 1

        sqlite_articles = fetchall_sqlite(sqlite_conn, "SELECT * FROM articles ORDER BY id ASC")
        for row in sqlite_articles:
            existed_before = row_exists_pg(
                pg_conn,
                "SELECT EXISTS(SELECT 1 FROM articles WHERE id = %s) AS exists",
                (str(row["id"]),),
            )
            with pg_conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO articles (
                        id, title, url, category, domain, published_at, summary,
                        article_text_excerpt, article_body_markdown, published_at_confidence,
                        source_trust_tier, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::timestamptz)
                    ON CONFLICT (id) DO UPDATE SET
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
                        str(row["id"]),
                        str(row.get("title") or ""),
                        str(row.get("url") or ""),
                        row.get("category"),
                        row.get("domain"),
                        row.get("published_at"),
                        row.get("summary"),
                        row.get("article_text_excerpt"),
                        row.get("article_body_markdown"),
                        float(row.get("published_at_confidence") or 0),
                        int(row.get("source_trust_tier") or 0),
                        str(row.get("updated_at") or db.utc_now()),
                    ),
                )
            if not existed_before:
                migrated["articles"] += 1

        sqlite_feedback = fetchall_sqlite(sqlite_conn, "SELECT * FROM feedback ORDER BY id ASC")
        for row in sqlite_feedback:
            mapped_user_id = user_id_map.get(int(row["user_id"]))
            if mapped_user_id is None:
                continue
            exists = row_exists_pg(
                pg_conn,
                """
                SELECT EXISTS(
                    SELECT 1 FROM feedback
                    WHERE user_id = %s
                      AND article_id = %s
                      AND rating = %s
                      AND COALESCE(notes, '') = COALESCE(%s, '')
                      AND created_at = %s::timestamptz
                ) AS exists
                """,
                (
                    mapped_user_id,
                    str(row["article_id"]),
                    int(row["rating"]),
                    row.get("notes") or "",
                    str(row["created_at"]),
                ),
            )
            if exists:
                continue
            with pg_conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO feedback (user_id, article_id, rating, notes, created_at)
                    VALUES (%s, %s, %s, %s, %s::timestamptz)
                    """,
                    (
                        mapped_user_id,
                        str(row["article_id"]),
                        int(row["rating"]),
                        row.get("notes"),
                        str(row["created_at"]),
                    ),
                )
            migrated["feedback"] += 1

        sqlite_cache = fetchall_sqlite(sqlite_conn, "SELECT * FROM cache ORDER BY key ASC")
        for row in sqlite_cache:
            existed_before = row_exists_pg(
                pg_conn,
                "SELECT EXISTS(SELECT 1 FROM cache WHERE key = %s) AS exists",
                (str(row["key"]),),
            )
            with pg_conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO cache (key, value, created_at, updated_at)
                    VALUES (%s, %s::jsonb, %s::timestamptz, %s::timestamptz)
                    ON CONFLICT (key) DO UPDATE SET
                        value = EXCLUDED.value,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        str(row["key"]),
                        json.dumps(json_or_default(row.get("value"), {}), sort_keys=True),
                        str(row.get("created_at") or db.utc_now()),
                        str(row.get("updated_at") or db.utc_now()),
                    ),
                )
            if not existed_before:
                migrated["cache"] += 1

        pg_conn.commit()
    return migrated


def parity_report(sqlite_path: Path, database_url: str) -> dict[str, dict[str, int]]:
    with sqlite_connection(sqlite_path) as sqlite_conn, psycopg.connect(database_url, row_factory=dict_row) as pg_conn:
        sqlite_counts = {
            "users": int(sqlite_conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]),
            "runs": int(sqlite_conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0]),
            "articles": int(sqlite_conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]),
            "feedback": int(sqlite_conn.execute("SELECT COUNT(*) FROM feedback").fetchone()[0]),
            "cache": int(sqlite_conn.execute("SELECT COUNT(*) FROM cache").fetchone()[0]),
        }
        with pg_conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) AS c FROM users")
            users = int(cursor.fetchone()["c"])
            cursor.execute("SELECT COUNT(*) AS c FROM runs")
            runs = int(cursor.fetchone()["c"])
            cursor.execute("SELECT COUNT(*) AS c FROM articles")
            articles = int(cursor.fetchone()["c"])
            cursor.execute("SELECT COUNT(*) AS c FROM feedback")
            feedback = int(cursor.fetchone()["c"])
            cursor.execute("SELECT COUNT(*) AS c FROM cache")
            cache = int(cursor.fetchone()["c"])
        postgres_counts = {"users": users, "runs": runs, "articles": articles, "feedback": feedback, "cache": cache}
    return {"sqlite": sqlite_counts, "postgres": postgres_counts}


def main() -> None:
    config = app_config.load_app_config()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sqlite-path", default=config.db_path, help="SQLite source path.")
    parser.add_argument("--database-url", default=config.database_url or "", help="Postgres destination URL.")
    args = parser.parse_args()

    if not args.database_url.strip():
        raise SystemExit("DATABASE_URL is required for migration.")

    sqlite_path = app_config.resolve_project_path(args.sqlite_path)
    if not sqlite_path.exists():
        raise SystemExit(f"SQLite source does not exist: {sqlite_path}")

    migrated_first = migrate(sqlite_path=sqlite_path, database_url=args.database_url.strip())
    migrated_second = migrate(sqlite_path=sqlite_path, database_url=args.database_url.strip())
    parity = parity_report(sqlite_path=sqlite_path, database_url=args.database_url.strip())

    print("migration_first_pass=" + json.dumps(migrated_first, sort_keys=True))
    print("migration_second_pass=" + json.dumps(migrated_second, sort_keys=True))
    print("migration_parity=" + json.dumps(parity, sort_keys=True))


if __name__ == "__main__":
    main()
