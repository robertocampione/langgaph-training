"""Telegram adapter for Personal Research Agent v3."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import Any


if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app import db  # noqa: E402
from app import db_users  # noqa: E402
from app import config as app_config  # noqa: E402
from app import main as agent_main  # noqa: E402


LOGGER = logging.getLogger(__name__)
SUPPORTED_LANGUAGES = {"en", "it", "nl"}
TELEGRAM_MESSAGE_LIMIT = 4096


def split_message(text: str, limit: int = TELEGRAM_MESSAGE_LIMIT) -> list[str]:
    if len(text) <= limit:
        return [text]
    chunks: list[str] = []
    remaining = text
    while len(remaining) > limit:
        split_at = remaining.rfind("\n", 0, limit)
        if split_at < limit // 2:
            split_at = limit
        chunks.append(remaining[:split_at].strip())
        remaining = remaining[split_at:].strip()
    if remaining:
        chunks.append(remaining)
    return chunks


def greeting_for(user: dict[str, Any]) -> str:
    topics = ", ".join(user["topics"])
    return (
        f"Hi {user['name']}. Personal Research Agent v3 is ready.\n"
        f"Language: {user['language']}\n"
        f"Topics: {topics}\n\n"
        "Commands: /ping, /run, /topics juventus, /topics juventus, bitcoin, /language en, /feedback 5 useful notes"
    )


def parse_topics_args(args: list[str]) -> list[str]:
    if not args:
        return []
    cleaned_args = [item.strip().lower() for item in args if item and item.strip()]
    if not cleaned_args:
        return []
    if any("," in item for item in cleaned_args):
        parts: list[str] = []
        for item in cleaned_args:
            parts.extend(part.strip() for part in item.split(","))
    else:
        parts = cleaned_args
    topics: list[str] = []
    seen: set[str] = set()
    for part in parts:
        if not part or part in seen:
            continue
        seen.add(part)
        topics.append(part)
    return topics


async def send_text(update: Any, text: str) -> None:
    if update.effective_chat is None:
        return
    for chunk in split_message(text):
        await update.effective_chat.send_message(chunk)


async def start_handler(update: Any, context: Any) -> None:
    chat = update.effective_chat
    if chat is None:
        return
    user_name = None
    if update.effective_user is not None:
        user_name = update.effective_user.full_name
    user = db_users.ensure_user(chat_id=int(chat.id), name=user_name)
    await send_text(update, greeting_for(user))


async def ping_handler(update: Any, context: Any) -> None:
    config = app_config.load_app_config()
    await send_text(
        update,
        f"pong db={config.db_path} token_configured={config.telegram_token_configured}",
    )


async def run_handler(update: Any, context: Any) -> None:
    chat = update.effective_chat
    if chat is None:
        return
    db_users.ensure_user(chat_id=int(chat.id))
    await send_text(update, "Running your research digest now.")
    mode = str(context.application.bot_data.get("run_mode", "auto"))
    max_results_per_query = int(context.application.bot_data.get("max_results_per_query", 2))
    fallback_to_stub = bool(context.application.bot_data.get("fallback_to_stub", True))
    try:
        result = await asyncio.to_thread(
            agent_main.run_for_chat_detailed,
            int(chat.id),
            mode,
            max_results_per_query,
            fallback_to_stub,
        )
    except Exception:
        LOGGER.exception("Pipeline run failed for chat_id=%s", chat.id)
        await send_text(update, "Sorry, I could not process that request.")
        return
    
    # Send summary first
    if result.get("summary"):
        await send_text(update, result["summary"])
    
    # Send newsletter
    if result.get("newsletter"):
        await send_text(update, f"📰 **Newsletter**\n\n{result['newsletter']}")
    
    # Send report
    if result.get("report"):
        await send_text(update, f"📋 **Report**\n\n{result['report']}")


async def topics_handler(update: Any, context: Any) -> None:
    chat = update.effective_chat
    if chat is None:
        return
    topics = parse_topics_args(context.args)
    if not topics:
        user = db_users.ensure_user(chat_id=int(chat.id))
        await send_text(update, "Current topics: " + ", ".join(user["topics"]))
        return
    user = db_users.ensure_user(chat_id=int(chat.id))
    updated = db_users.update_user_topics(chat_id=int(chat.id), topics=topics)
    await send_text(update, f"Updated topics for {user['name']}: " + ", ".join(updated["topics"]))


async def language_handler(update: Any, context: Any) -> None:
    chat = update.effective_chat
    if chat is None:
        return
    if not context.args:
        language = db_users.get_user_language(chat_id=int(chat.id)) or app_config.load_app_config().default_language
        await send_text(update, f"Current language: {language}")
        return
    requested = context.args[0].strip().lower()
    if requested not in SUPPORTED_LANGUAGES:
        await send_text(update, "Supported languages: en, it, nl")
        return
    db_users.ensure_user(chat_id=int(chat.id))
    updated = db_users.update_user_language(chat_id=int(chat.id), language=requested)
    await send_text(update, f"Updated language: {updated['language']}")


async def feedback_handler(update: Any, context: Any) -> None:
    chat = update.effective_chat
    if chat is None:
        return
    if not context.args:
        await send_text(update, "Use /feedback <rating 1-5> <notes>.")
        return
    try:
        rating = int(context.args[0])
    except ValueError:
        await send_text(update, "Feedback rating must be a number from 1 to 5.")
        return
    if rating < 1 or rating > 5:
        await send_text(update, "Feedback rating must be between 1 and 5.")
        return

    config = app_config.load_app_config()
    user = db_users.ensure_user(chat_id=int(chat.id), db_path=config.db_path)
    latest_run = db.latest_run_for_user(int(user["id"]), db_path=config.db_path)
    if latest_run is None:
        await send_text(update, "Run /run before sending feedback.")
        return

    notes = " ".join(context.args[1:]).strip()
    feedback_id = db.create_run_feedback(
        user_id=int(user["id"]),
        run_id=int(latest_run["id"]),
        rating=rating,
        notes=notes,
        db_path=config.db_path,
    )
    await send_text(update, f"Thanks. Feedback saved with id {feedback_id}.")


async def fallback_handler(update: Any, context: Any) -> None:
    text = update.message.text.strip() if update.message and update.message.text else ""
    if text:
        await send_text(update, "Send /run to generate a digest, /feedback to rate it, or /topics and /language to update preferences.")


def build_application(token: str) -> Any:
    from telegram.ext import Application, CommandHandler, MessageHandler, filters

    application = Application.builder().token(token).build()
    application.add_handler(CommandHandler("start", start_handler))
    application.add_handler(CommandHandler("ping", ping_handler))
    application.add_handler(CommandHandler(["run", "news"], run_handler))
    application.add_handler(CommandHandler(["topics", "settopics"], topics_handler))
    application.add_handler(CommandHandler("language", language_handler))
    application.add_handler(CommandHandler("feedback", feedback_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, fallback_handler))
    return application


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Validate configuration without starting Telegram polling.")
    parser.add_argument("--env-file", default=".env", help="Path to a dotenv-style file.")
    parser.add_argument("--mode", choices=("auto", "live", "web_fallback", "fixture"), default="auto", help="Retrieval mode used by /run.")
    parser.add_argument("--max-results-per-query", type=int, default=2, help="Bounded retrieval cap used by /run.")
    parser.add_argument("--no-fallback", action="store_true", help="Raise pipeline errors instead of returning the readiness stub.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    config = app_config.load_app_config(args.env_file)
    db.initialize_database(config.db_path)
    db_users.seed_users_from_config(db_path=config.db_path)

    if args.dry_run:
        print(f"telegram_dry_run=pass db={config.db_path} token_configured={config.telegram_token_configured}")
        return
    if not config.telegram_token_configured:
        raise SystemExit("TELEGRAM_TOKEN is required to start polling.")

    application = build_application(app_config.get_telegram_token())
    application.bot_data["run_mode"] = args.mode
    application.bot_data["max_results_per_query"] = args.max_results_per_query
    application.bot_data["fallback_to_stub"] = not args.no_fallback
    LOGGER.info(
        "telegram_bot_ready mode=%s max_results_per_query=%s fallback_to_stub=%s db=%s",
        args.mode,
        args.max_results_per_query,
        not args.no_fallback,
        config.db_path,
    )
    application.run_polling()


if __name__ == "__main__":
    main()
