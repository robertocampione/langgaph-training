"""Entry point for Personal Research Agent v3."""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path


if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import db  # noqa: E402
from app import db_users  # noqa: E402


@dataclass(frozen=True)
class AppConfig:
    db_path: str
    default_language: str
    default_topics: tuple[str, ...]


def load_config() -> AppConfig:
    """Load the minimal project configuration from environment variables."""
    topics_raw = os.getenv("DEFAULT_TOPICS", "news,events,bitcoin")
    topics = tuple(topic.strip() for topic in topics_raw.split(",") if topic.strip())
    return AppConfig(
        db_path=os.getenv("DB_PATH", "db/personal_research_agent.sqlite"),
        default_language=os.getenv("DEFAULT_LANGUAGE", "it"),
        default_topics=topics,
    )


def run_for_chat(chat_id: int | None = None) -> str:
    """Run the current deterministic placeholder pipeline for a user."""
    config = load_config()
    effective_chat_id = chat_id if chat_id is not None else 0
    user = db_users.ensure_user(
        chat_id=effective_chat_id,
        name="Local User" if chat_id is None else None,
        db_path=config.db_path,
    )
    topics = ", ".join(user["topics"])
    run_id = db.log_run(
        user_id=int(user["id"]),
        quality_status="stub",
        selected_counts={topic: 0 for topic in user["topics"]},
        db_path=config.db_path,
    )
    return (
        "Personal Research Agent v3 ready "
        f"for {user['name']} (chat {effective_chat_id}). "
        f"Language={user['language']}; topics={topics}; db={config.db_path}; run_id={run_id}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chat-id", type=int, default=None, help="External chat identifier for the run.")
    parser.add_argument("--init-db", action="store_true", help="Initialize the DB and seed configured users before running.")
    args = parser.parse_args()

    config = load_config()
    if args.init_db:
        db.initialize_database(config.db_path)
        db_users.seed_users_from_config(db_path=config.db_path)
    print(run_for_chat(args.chat_id))


if __name__ == "__main__":
    main()
