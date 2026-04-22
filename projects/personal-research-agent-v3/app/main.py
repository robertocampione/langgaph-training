"""Entry point for Personal Research Agent v3."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import db  # noqa: E402
from app import db_users  # noqa: E402
from app import config as app_config  # noqa: E402
from app import pipeline  # noqa: E402


def load_config() -> app_config.AppConfig:
    """Load the minimal project configuration from environment variables."""
    return app_config.load_app_config()


def readiness_stub(chat_id: int | None = None) -> str:
    """Return the deterministic readiness fallback for a user."""
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


def run_for_chat(
    chat_id: int | None = None,
    mode: str = "auto",
    max_results_per_query: int = pipeline.DEFAULT_MAX_RESULTS_PER_QUERY,
    fallback_to_stub: bool = True,
) -> str:
    """Run the v3 digest pipeline for a chat user."""
    effective_chat_id = chat_id if chat_id is not None else 0
    try:
        result = pipeline.run_research_digest(
            chat_id=effective_chat_id,
            mode=mode,
            max_results_per_query=max_results_per_query,
        )
        return pipeline.format_console_summary(result)
    except Exception as exc:
        if not fallback_to_stub:
            raise
        return readiness_stub(effective_chat_id) + f" Fallback reason: {exc}"


def run_for_chat_detailed(
    chat_id: int | None = None,
    mode: str = "auto",
    max_results_per_query: int = pipeline.DEFAULT_MAX_RESULTS_PER_QUERY,
    fallback_to_stub: bool = True,
) -> dict:
    """Run the v3 digest pipeline and return newsletter and report content."""
    effective_chat_id = chat_id if chat_id is not None else 0
    try:
        result = pipeline.run_research_digest(
            chat_id=effective_chat_id,
            mode=mode,
            max_results_per_query=max_results_per_query,
        )
        return {
            "newsletter": result.newsletter,
            "report": result.report,
            "summary": pipeline.format_console_summary(result),
        }
    except Exception as exc:
        if not fallback_to_stub:
            raise
        fallback_msg = readiness_stub(effective_chat_id) + f" Fallback reason: {exc}"
        return {
            "newsletter": "",
            "report": "",
            "summary": fallback_msg,
            "error": True,
        }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chat-id", type=int, default=None, help="External chat identifier for the run.")
    parser.add_argument("--init-db", action="store_true", help="Initialize the DB and seed configured users before running.")
    parser.add_argument(
        "--mode",
        choices=("auto", "live", "web_fallback", "fixture"),
        default="auto",
        help="Retrieval mode.",
    )
    parser.add_argument("--max-results-per-query", type=int, default=pipeline.DEFAULT_MAX_RESULTS_PER_QUERY, help="Bounded retrieval cap.")
    parser.add_argument("--no-fallback", action="store_true", help="Raise pipeline errors instead of returning the readiness stub.")
    args = parser.parse_args()

    config = load_config()
    if args.init_db:
        db.initialize_database(config.db_path)
        db_users.seed_users_from_config(db_path=config.db_path)
    print(
        run_for_chat(
            args.chat_id,
            mode=args.mode,
            max_results_per_query=args.max_results_per_query,
            fallback_to_stub=not args.no_fallback,
        )
    )


if __name__ == "__main__":
    main()
