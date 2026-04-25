"""Entry point for Personal Research Agent v4."""

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
from app.graphs import research_graph # noqa: E402
from app.state.research_state import ResearchGraphState # noqa: E402


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
        db_path=config.runtime_db_path,
    )
    topics = ", ".join(user["topics"])
    run_id = db.log_run(
        user_id=int(user["id"]),
        quality_status="stub",
        selected_counts={topic: 0 for topic in user["topics"]},
        db_path=config.runtime_db_path,
    )
    db_label = config.database_url if config.db_backend == "postgres" else config.db_path
    return (
        "Personal Research Agent v4 ready "
        f"for {user['name']} (chat {effective_chat_id}). "
        f"Language={user['language']}; topics={topics}; db={db_label}; run_id={run_id}"
    )


async def run_for_chat(
    chat_id: int | None = None,
    mode: str = "auto",
    max_results_per_query: int = pipeline.DEFAULT_MAX_RESULTS_PER_QUERY,
    fallback_to_stub: bool = True,
) -> str:
    """Run the v4 digest pipeline for a chat user."""
    effective_chat_id = chat_id if chat_id is not None else 0
    try:
        initial_state = ResearchGraphState(chat_id=effective_chat_id, mode=mode, max_results_per_query=max_results_per_query)
        final_state = await research_graph.graph.ainvoke(initial_state)
        # Format simple summary natively
        run_id = final_state.get("run_id", "unknown")
        quality = final_state.get("quality_status", "unknown")
        return f"Run {run_id} completed via Graph. Quality: {quality} Mode: {mode}"
    except Exception as exc:
        if not fallback_to_stub:
            raise
        return readiness_stub(effective_chat_id) + f" Fallback reason: {exc}"


async def run_for_chat_detailed(
    chat_id: int | None = None,
    mode: str = "auto",
    max_results_per_query: int = pipeline.DEFAULT_MAX_RESULTS_PER_QUERY,
    fallback_to_stub: bool = True,
    override_topics: list[str] | None = None,
) -> dict:
    """Run the v4 digest pipeline and return newsletter and report content."""
    effective_chat_id = chat_id if chat_id is not None else 0
    try:
        initial_state = ResearchGraphState(chat_id=effective_chat_id, mode=mode, max_results_per_query=max_results_per_query)
        final_state = await research_graph.graph.ainvoke(initial_state)
        
        return {
            "newsletter": final_state.get("newsletter"),
            "report": final_state.get("report"),
            "summary": f"Run {final_state.get('run_id')} completed via Graph. Quality: {final_state.get('quality_status')}",
            "run_id": final_state.get("run_id"),
            "mode": mode,
            "language": (final_state.get("user") or {}).get("language", "en"),
            "quality_status": final_state.get("quality_status"),
            "quality_flags": [],
            "debug_dir": final_state.get("debug_dir"),
            "newsletter_path": final_state.get("newsletter_path", ""),
            "report_path": final_state.get("report_path", ""),
            "selected_counts": final_state.get("selected_counts", {}),
            "enriched_items": final_state.get("enriched_items", []),
            "telegram_compact": final_state.get("telegram_compact", ""),
            "cost_trace": final_state.get("cost_trace", {}),
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
    import asyncio
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
        db.initialize_database(db_path=config.runtime_db_path)
        db_users.seed_users_from_config(db_path=config.runtime_db_path)
    
    res = asyncio.run(run_for_chat(
        args.chat_id,
        mode=args.mode,
        max_results_per_query=args.max_results_per_query,
        fallback_to_stub=not args.no_fallback,
    ))
    print(res)


if __name__ == "__main__":
    main()
