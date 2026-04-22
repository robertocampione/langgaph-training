#!/usr/bin/env python3
"""Deterministic validation for the v3 SQLite persistence layer."""

from __future__ import annotations

import argparse
import tempfile
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app import db  # noqa: E402
from app import db_users  # noqa: E402


def validate(db_path: str) -> None:
    db.initialize_database(db_path)
    db.initialize_database(db_path)
    db_users.seed_users_from_config(db_path=db_path)

    user = db.create_user(
        chat_id=424242,
        name="Validation User",
        language="en",
        topics=["news", "bitcoin"],
        db_path=db_path,
    )
    updated = db.update_user_preferences(
        chat_id=424242,
        language="nl",
        topics=["events"],
        db_path=db_path,
    )
    if updated["language"] != "nl" or updated["topics"] != ["events"]:
        raise AssertionError("User preference update failed")

    run_id = db.log_run(
        user_id=int(user["id"]),
        quality_status="validated",
        selected_counts={"events": 1},
        db_path=db_path,
    )
    if run_id < 1:
        raise AssertionError("Run logging failed")

    article_id = db.cache_article(
        {
            "title": "Validation Article",
            "url": "https://example.com/validation",
            "category": "events",
            "domain": "example.com",
            "summary": "Validation summary.",
        },
        db_path=db_path,
    )
    cached = db.get_article_by_url("https://example.com/validation", db_path=db_path)
    if cached is None or cached["id"] != article_id:
        raise AssertionError("Article cache lookup failed")

    db.set_cache_value("validation:key", {"ok": True}, db_path=db_path)
    if db.get_cache_value("validation:key", db_path=db_path) != {"ok": True}:
        raise AssertionError("Generic cache round trip failed")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", default=None, help="Optional DB path. A temp DB is used by default.")
    args = parser.parse_args()

    if args.db_path:
        validate(args.db_path)
        print(f"db_validation=pass path={args.db_path}")
        return

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_db = str(Path(temp_dir) / "validation.sqlite")
        validate(temp_db)
        print("db_validation=pass temp_db=true")


if __name__ == "__main__":
    main()

