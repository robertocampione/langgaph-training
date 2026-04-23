"""Telegram adapter for Personal Research Agent v4."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app import db  # noqa: E402
from app import db_users  # noqa: E402
from app import config as app_config  # noqa: E402
from app import llm  # noqa: E402
from app import main as agent_main  # noqa: E402


LOGGER = logging.getLogger(__name__)
SUPPORTED_LANGUAGES = {"en", "it", "nl"}
TELEGRAM_MESSAGE_LIMIT = 4096
MAX_TELEGRAM_ITEMS = 5
DEFAULT_TRAVEL_DAYS = 7
MAX_DETAIL_BODY_CHARS = 1400

REFERENCE_STOPWORDS = {
    "the",
    "this",
    "that",
    "with",
    "from",
    "about",
    "more",
    "info",
    "detail",
    "details",
    "please",
    "give",
    "show",
    "tell",
    "dammi",
    "piu",
    "più",
    "info",
    "dettagli",
    "notizia",
    "notizie",
    "evento",
    "evento",
    "eventi",
    "sulla",
    "sulle",
    "sulla",
    "sul",
    "su",
    "della",
    "delle",
    "del",
    "dei",
    "van",
    "voor",
    "meer",
    "informatie",
    "details",
    "over",
    "het",
    "een",
}

VOTE_TO_RATING = {
    "dislike": 1,
    "star": 4,
    "like": 5,
}


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
        f"Hi {user['name']}. Personal Research Agent v4 is ready.\n"
        f"Language: {user['language']}\n"
        f"Topics: {topics}\n\n"
        "Commands: /run, /detail, /profile, /location, /travel, /sources, /subtopics, /memory, /topics, /language, /feedback"
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


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _tokenize_reference_text(value: str) -> set[str]:
    raw_tokens = re.findall(r"[A-Za-zÀ-ÿ0-9_-]{3,}", (value or "").lower())
    return {token for token in raw_tokens if token not in REFERENCE_STOPWORDS}


def _chat_digest_store(application: Any) -> dict[int, dict[str, Any]]:
    store = application.bot_data.get("last_digest_by_chat")
    if isinstance(store, dict):
        return store
    store = {}
    application.bot_data["last_digest_by_chat"] = store
    return store


def _project_item_for_reference(item: dict[str, Any], index: int) -> dict[str, Any]:
    return {
        "index": index,
        "item_id": str(item.get("item_id") or ""),
        "title": str(item.get("title") or "Untitled"),
        "url": str(item.get("url") or ""),
        "track_type": str(item.get("track_type") or "item"),
        "source": str(item.get("source") or ""),
        "short_summary": str(item.get("short_summary") or item.get("summary") or "").strip(),
        "why_it_matters": str(item.get("why_it_matters") or "").strip(),
        "suggested_action": str(item.get("suggested_action") or "").strip(),
        "article_text_excerpt": str(item.get("article_text_excerpt") or "").strip(),
        "article_body_markdown": str(item.get("article_body_markdown") or "").strip(),
    }


def _save_last_digest_for_chat(context: Any, chat_id: int, result: dict[str, Any]) -> None:
    items = result.get("enriched_items") or []
    if not isinstance(items, list):
        items = []
    projected = [_project_item_for_reference(item, idx) for idx, item in enumerate(items[:MAX_TELEGRAM_ITEMS], start=1)]
    store = _chat_digest_store(context.application)
    store[chat_id] = {
        "run_id": int(result.get("run_id") or 0),
        "mode": str(result.get("mode") or ""),
        "language": str(result.get("language") or "en").strip().lower(),
        "saved_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "items": projected,
    }


def _latest_digest_for_chat(context: Any, chat_id: int) -> dict[str, Any] | None:
    store = _chat_digest_store(context.application)
    payload = store.get(chat_id)
    if isinstance(payload, dict):
        return payload
    return None


def _build_index_message(items: list[dict[str, Any]], language: str) -> str:
    if not items:
        if language == "it":
            return "Nessun item disponibile per approfondimenti."
        if language == "nl":
            return "Geen items beschikbaar voor verdieping."
        return "No items available for drill-down."
    if language == "it":
        header = "Indice item (usa /detail <numero> oppure chiedi in linguaggio naturale):"
    elif language == "nl":
        header = "Itemindex (gebruik /detail <nummer> of vraag in natuurlijke taal):"
    else:
        header = "Item index (use /detail <number> or ask in natural language):"
    lines = [header]
    for item in items:
        lines.append(f"{item['index']}. [{item['track_type']}] {item['title']}")
    return "\n".join(lines)


def _extract_numeric_reference(query: str, max_items: int) -> int | None:
    match = re.search(r"\b(\d{1,2})\b", query or "")
    if not match:
        return None
    index = int(match.group(1))
    if 1 <= index <= max_items:
        return index
    return None


def _looks_like_drilldown_request(text: str) -> bool:
    normalized = _normalize_text(text).lower()
    if not normalized:
        return False
    if _extract_numeric_reference(normalized, 99) is not None:
        return True
    if "?" in normalized:
        return True
    cues = {
        "detail",
        "details",
        "more",
        "info",
        "information",
        "explain",
        "tell",
        "about",
        "approfond",
        "spiega",
        "dimmi",
        "dettagli",
        "meer",
        "uitleg",
        "vertel",
        "su",
        "sull",
        "sulla",
        "notizia",
        "evento",
        "item",
    }
    tokens = _tokenize_reference_text(normalized)
    return bool(tokens & cues)


def _best_text_reference_match(query: str, items: list[dict[str, Any]]) -> int | None:
    query_tokens = _tokenize_reference_text(query)
    if not query_tokens:
        return None
    best_index: int | None = None
    best_score = 0.0
    query_norm = _normalize_text(query).lower()
    for item in items:
        text_blob = " ".join(
            [
                str(item.get("title") or ""),
                str(item.get("short_summary") or ""),
                str(item.get("article_text_excerpt") or ""),
                str(item.get("track_type") or ""),
            ]
        )
        item_norm = _normalize_text(text_blob).lower()
        item_tokens = _tokenize_reference_text(item_norm)
        overlap = len(query_tokens & item_tokens)
        if overlap == 0:
            continue
        phrase_bonus = 0.0
        if len(query_norm) >= 8 and query_norm in item_norm:
            phrase_bonus = 1.5
        score = overlap + phrase_bonus
        if score > best_score:
            best_score = score
            best_index = int(item.get("index") or 0)
    if best_score >= 1.8 and best_index:
        return best_index
    return None


def _resolve_item_reference(query: str, items: list[dict[str, Any]]) -> int | None:
    numeric = _extract_numeric_reference(query, len(items))
    if numeric is not None:
        return numeric
    return _best_text_reference_match(query, items)


def _resolve_item_reference_llm(query: str, items: list[dict[str, Any]]) -> int | None:
    if not llm.llm_enabled() or not llm.configured_providers():
        return None
    compact_items = []
    for item in items:
        compact_items.append(
            {
                "index": int(item.get("index") or 0),
                "title": str(item.get("title") or ""),
                "summary": str(item.get("short_summary") or ""),
                "track_type": str(item.get("track_type") or ""),
            }
        )
    prompt = {
        "query": _normalize_text(query),
        "items": compact_items,
        "task": "Choose the single best matching item index for this user query, or 0 if there is no clear match.",
        "output_schema": {"index": "int", "confidence": "0..1"},
    }
    response = llm.call_llm(
        role="utility",
        system_prompt="Return strict compact JSON only.",
        user_prompt=json.dumps(prompt, ensure_ascii=False),
        temperature=0.0,
        timeout_seconds=12,
    )
    if not response.get("ok"):
        return None
    content = str(response.get("content") or "").strip()
    if not content:
        return None
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        return None
    index = int(parsed.get("index") or 0)
    confidence = float(parsed.get("confidence") or 0.0)
    if not (1 <= index <= len(items)):
        return None
    if confidence < 0.55:
        return None
    return index


def _format_detail_response(item: dict[str, Any], language: str) -> str:
    title = str(item.get("title") or "Untitled")
    index = int(item.get("index") or 0)
    source = str(item.get("source") or "unknown")
    track_type = str(item.get("track_type") or "item")
    summary = str(item.get("short_summary") or "").strip()
    why = str(item.get("why_it_matters") or "").strip()
    action = str(item.get("suggested_action") or "").strip()
    detail_body = str(item.get("article_body_markdown") or item.get("article_text_excerpt") or "").strip()
    detail_body = _normalize_text(detail_body)
    if len(detail_body) > MAX_DETAIL_BODY_CHARS:
        detail_body = detail_body[: MAX_DETAIL_BODY_CHARS - 3].rstrip() + "..."
    url = str(item.get("url") or "")

    if language == "it":
        lines = [
            f"Dettaglio item {index}",
            f"{title}",
            f"- Tipo: {track_type}",
            f"- Fonte: {source}",
        ]
        if summary:
            lines.append(f"- Sintesi: {summary}")
        if detail_body:
            lines.append(f"- Approfondimento: {detail_body}")
        if why:
            lines.append(f"- Perché conta: {why}")
        if action:
            lines.append(f"- Azione suggerita: {action}")
        if url:
            lines.append(f"- Link: {url}")
        return "\n".join(lines)
    if language == "nl":
        lines = [
            f"Detail item {index}",
            f"{title}",
            f"- Type: {track_type}",
            f"- Bron: {source}",
        ]
        if summary:
            lines.append(f"- Samenvatting: {summary}")
        if detail_body:
            lines.append(f"- Verdieping: {detail_body}")
        if why:
            lines.append(f"- Waarom relevant: {why}")
        if action:
            lines.append(f"- Aanbevolen actie: {action}")
        if url:
            lines.append(f"- Link: {url}")
        return "\n".join(lines)
    lines = [
        f"Item detail {index}",
        title,
        f"- Type: {track_type}",
        f"- Source: {source}",
    ]
    if summary:
        lines.append(f"- Summary: {summary}")
    if detail_body:
        lines.append(f"- Deep dive: {detail_body}")
    if why:
        lines.append(f"- Why it matters: {why}")
    if action:
        lines.append(f"- Suggested action: {action}")
    if url:
        lines.append(f"- Link: {url}")
    return "\n".join(lines)


def _detail_usage(language: str) -> str:
    if language == "it":
        return "Usa /detail <numero> oppure scrivi: dammi più info su item 2."
    if language == "nl":
        return "Gebruik /detail <nummer> of schrijf: geef meer info over item 2."
    return "Use /detail <number> or write: give me more info about item 2."


def runtime_db_path(config: app_config.AppConfig) -> str | None:
    return config.runtime_db_path


async def send_text(update: Any, text: str, disable_preview: bool = True) -> None:
    if update.effective_chat is None:
        return
    for chunk in split_message(text):
        await update.effective_chat.send_message(chunk, disable_web_page_preview=disable_preview)


def _feedback_labels(language: str) -> tuple[str, str]:
    if language == "it":
        return ("Valuta questo item:", "Grazie, feedback per-item salvato.")
    if language == "nl":
        return ("Beoordeel dit item:", "Bedankt, item-feedback opgeslagen.")
    return ("Rate this item:", "Thanks, item feedback saved.")


def _profile_incomplete(profile: dict[str, Any] | None) -> bool:
    if not profile:
        return True
    language = str(profile.get("language") or "").strip().lower()
    home_location = str(profile.get("home_location") or "").strip()
    explicit = profile.get("explicit_preferences") or {}
    topics = []
    if isinstance(explicit, dict):
        topics = explicit.get("topics") or []
    return not language or not home_location or not topics


def _topics_from_text(raw: str) -> list[str]:
    return parse_topics_args([raw])


def _next_onboarding_question(step: str, language: str) -> str:
    lang = language if language in SUPPORTED_LANGUAGES else "en"
    prompts = {
        "en": {
            "language": "Choose language: en / it / nl",
            "location": "Where are you based now? (example: Maastricht)",
            "topics": "Main topics? (comma-separated, e.g. news, events, bitcoin, juventus)",
            "depth": "Depth preference? brief / standard / deep",
        },
        "it": {
            "language": "Scegli lingua: en / it / nl",
            "location": "Dove sei basato ora? (esempio: Maastricht)",
            "topics": "Topic principali? (separati da virgola, es: news, events, bitcoin, juventus)",
            "depth": "Profondità preferita? brief / standard / deep",
        },
        "nl": {
            "language": "Kies taal: en / it / nl",
            "location": "Waar ben je nu gevestigd? (bijv. Maastricht)",
            "topics": "Belangrijkste topics? (komma-gescheiden, bijv. news, events, bitcoin, juventus)",
            "depth": "Voorkeursdiepte? brief / standard / deep",
        },
    }
    return prompts[lang].get(step, prompts[lang]["location"])


def _advance_onboarding_step(current: str) -> str | None:
    order = ["language", "location", "topics", "depth"]
    if current not in order:
        return "language"
    idx = order.index(current)
    if idx >= len(order) - 1:
        return None
    return order[idx + 1]


def _parse_travel_args(args: list[str]) -> tuple[str, int]:
    if not args:
        return "", DEFAULT_TRAVEL_DAYS
    location = str(args[0]).strip()
    days = DEFAULT_TRAVEL_DAYS
    if len(args) > 1:
        raw = str(args[1]).strip().lower().removesuffix("d")
        if raw.isdigit():
            days = max(1, min(30, int(raw)))
    return location, days


async def send_markdown_file(update: Any, path: str, caption: str) -> bool:
    chat = update.effective_chat
    if chat is None:
        return False
    file_path = Path(path)
    if not file_path.exists():
        return False
    try:
        with file_path.open("rb") as handle:
            await chat.send_document(document=handle, filename=file_path.name, caption=caption)
        return True
    except Exception:
        LOGGER.exception("Unable to send file %s", file_path)
        return False


def item_feedback_keyboard(item_id: str) -> Any:
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("👎", callback_data=f"fb:{item_id}:dislike"),
                InlineKeyboardButton("⭐", callback_data=f"fb:{item_id}:star"),
                InlineKeyboardButton("👍", callback_data=f"fb:{item_id}:like"),
            ]
        ]
    )


async def start_handler(update: Any, context: Any) -> None:
    chat = update.effective_chat
    if chat is None:
        return
    user_name = None
    if update.effective_user is not None:
        user_name = update.effective_user.full_name
    config = app_config.load_app_config()
    user = db_users.ensure_user(chat_id=int(chat.id), name=user_name, db_path=runtime_db_path(config))
    await send_text(update, greeting_for(user))
    if str(os.getenv("PRA_FLAG_ONBOARDING", "true")).strip().lower() not in {"1", "true", "yes", "on"}:
        return
    profile = db.get_profile(user_id=int(user["id"]), db_path=runtime_db_path(config))
    if _profile_incomplete(profile):
        await onboard_handler(update, context)


async def ping_handler(update: Any, context: Any) -> None:
    config = app_config.load_app_config()
    db_label = config.database_url if config.db_backend == "postgres" else config.db_path
    await send_text(
        update,
        f"pong db={db_label} token_configured={config.telegram_token_configured}",
    )


async def run_handler(update: Any, context: Any) -> None:
    chat = update.effective_chat
    if chat is None:
        return
    config = app_config.load_app_config()
    db_users.ensure_user(chat_id=int(chat.id), db_path=runtime_db_path(config))
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

    if result.get("summary"):
        await send_text(update, result["summary"])

    language = str(result.get("language") or "en").strip().lower()
    compact = str(result.get("telegram_compact") or "").strip()
    if compact:
        await send_text(update, compact)

    enriched_items = result.get("enriched_items", [])
    _save_last_digest_for_chat(context, int(chat.id), result)
    digest = _latest_digest_for_chat(context, int(chat.id))
    if digest:
        await send_text(update, _build_index_message(digest.get("items") or [], language))
    prompt_label, _ = _feedback_labels(language)
    for item in enriched_items[:MAX_TELEGRAM_ITEMS]:
        item_id = str(item.get("item_id") or "").strip()
        title = str(item.get("title") or "Untitled")
        url = str(item.get("url") or "")
        if not item_id:
            continue
        text = f"{prompt_label}\n{title}\n{url}".strip()
        await chat.send_message(text=text, reply_markup=item_feedback_keyboard(item_id), disable_web_page_preview=True)

    newsletter_sent = await send_markdown_file(update, str(result.get("newsletter_path") or ""), "Newsletter")
    report_sent = await send_markdown_file(update, str(result.get("report_path") or ""), "Report")
    if not newsletter_sent and result.get("newsletter"):
        await send_text(update, result["newsletter"])
    if not report_sent and result.get("report"):
        await send_text(update, result["report"])


async def detail_handler(update: Any, context: Any) -> None:
    chat = update.effective_chat
    if chat is None:
        return
    digest = _latest_digest_for_chat(context, int(chat.id))
    config = app_config.load_app_config()
    language = db_users.get_user_language(chat_id=int(chat.id), db_path=runtime_db_path(config)) or config.default_language
    if not digest or not digest.get("items"):
        await send_text(update, "Run /run first. " + _detail_usage(str(language).strip().lower()))
        return
    query = " ".join(context.args).strip()
    if not query:
        await send_text(update, _build_index_message(digest["items"], str(language).strip().lower()))
        await send_text(update, _detail_usage(str(language).strip().lower()))
        return
    index = _resolve_item_reference(query, digest["items"])
    if index is None:
        index = _resolve_item_reference_llm(query, digest["items"])
    if index is None:
        await send_text(update, _detail_usage(str(language).strip().lower()))
        return
    selected = next((item for item in digest["items"] if int(item.get("index") or 0) == index), None)
    if selected is None:
        await send_text(update, _detail_usage(str(language).strip().lower()))
        return
    await send_text(update, _format_detail_response(selected, str(language).strip().lower()))


async def topics_handler(update: Any, context: Any) -> None:
    chat = update.effective_chat
    if chat is None:
        return
    config = app_config.load_app_config()
    topics = parse_topics_args(context.args)
    if not topics:
        user = db_users.ensure_user(chat_id=int(chat.id), db_path=runtime_db_path(config))
        await send_text(update, "Current topics: " + ", ".join(user["topics"]))
        return
    user = db_users.ensure_user(chat_id=int(chat.id), db_path=runtime_db_path(config))
    updated = db_users.update_user_topics(chat_id=int(chat.id), topics=topics, db_path=runtime_db_path(config))
    profile = db.get_profile(user_id=int(user["id"]), db_path=runtime_db_path(config)) or {}
    explicit = dict(profile.get("explicit_preferences") or {})
    explicit["topics"] = topics
    db.upsert_profile(user_id=int(user["id"]), explicit_preferences=explicit, db_path=runtime_db_path(config))
    await send_text(update, f"Updated topics for {user['name']}: " + ", ".join(updated["topics"]))


async def language_handler(update: Any, context: Any) -> None:
    chat = update.effective_chat
    if chat is None:
        return
    config = app_config.load_app_config()
    if not context.args:
        language = db_users.get_user_language(chat_id=int(chat.id), db_path=runtime_db_path(config)) or config.default_language
        await send_text(update, f"Current language: {language}")
        return
    requested = context.args[0].strip().lower()
    if requested not in SUPPORTED_LANGUAGES:
        await send_text(update, "Supported languages: en, it, nl")
        return
    user = db_users.ensure_user(chat_id=int(chat.id), db_path=runtime_db_path(config))
    updated = db_users.update_user_language(chat_id=int(chat.id), language=requested, db_path=runtime_db_path(config))
    db.upsert_profile(user_id=int(user["id"]), language=requested, db_path=runtime_db_path(config))
    await send_text(update, f"Updated language: {updated['language']}")


async def profile_handler(update: Any, context: Any) -> None:
    chat = update.effective_chat
    if chat is None:
        return
    config = app_config.load_app_config()
    user = db_users.ensure_user(chat_id=int(chat.id), db_path=runtime_db_path(config))
    profile = db.get_profile(user_id=int(user["id"]), db_path=runtime_db_path(config)) or {}
    temporary_contexts = db.list_active_temporary_contexts(user_id=int(user["id"]), db_path=runtime_db_path(config))
    source_prefs = db.list_source_preferences(user_id=int(user["id"]), db_path=runtime_db_path(config))
    topic_graph = db.list_topic_weights(user_id=int(user["id"]), db_path=runtime_db_path(config))
    lines = [
        f"Profile for {user['name']}",
        f"- language: {profile.get('language') or user.get('language')}",
        f"- home_location: {profile.get('home_location') or '(not set)'}",
        f"- desired_depth: {profile.get('desired_depth') or 'standard'}",
        f"- topics: {', '.join(user.get('topics') or [])}",
        f"- active_temporary_contexts: {len(temporary_contexts)}",
        f"- source_preferences: {len(source_prefs)}",
        f"- subtopics: {len(topic_graph)}",
    ]
    await send_text(update, "\n".join(lines))


async def location_handler(update: Any, context: Any) -> None:
    chat = update.effective_chat
    if chat is None:
        return
    config = app_config.load_app_config()
    user = db_users.ensure_user(chat_id=int(chat.id), db_path=runtime_db_path(config))
    if not context.args:
        profile = db.get_profile(user_id=int(user["id"]), db_path=runtime_db_path(config)) or {}
        await send_text(update, f"Current location: {profile.get('home_location') or '(not set)'}")
        return
    location = " ".join(context.args).strip()
    if not location:
        await send_text(update, "Use /location <city or country>.")
        return
    profile = db.upsert_profile(
        user_id=int(user["id"]),
        language=str(user.get("language") or config.default_language),
        home_location=location,
        db_path=runtime_db_path(config),
    )
    db.upsert_profile_fact(
        user_id=int(user["id"]),
        fact_key="home_location",
        fact_value={"value": location},
        source="user",
        is_explicit=True,
        db_path=runtime_db_path(config),
    )
    db.append_profile_event(
        user_id=int(user["id"]),
        event_type="profile_location_updated",
        payload={"location": location, "profile_version": profile.get("profile_version")},
        db_path=runtime_db_path(config),
    )
    await send_text(update, f"Location updated: {location}")


async def travel_handler(update: Any, context: Any) -> None:
    chat = update.effective_chat
    if chat is None:
        return
    config = app_config.load_app_config()
    user = db_users.ensure_user(chat_id=int(chat.id), db_path=runtime_db_path(config))
    location, days = _parse_travel_args(context.args)
    if not location:
        active = db.list_active_temporary_contexts(user_id=int(user["id"]), db_path=runtime_db_path(config))
        travel = [item for item in active if str(item.get("context_type") or "").lower() == "travel"]
        if not travel:
            await send_text(update, "Use /travel <destination> [days]. Example: /travel Madrid 7")
            return
        lines = ["Active travel overrides:"]
        for item in travel:
            payload = item.get("payload") or {}
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except json.JSONDecodeError:
                    payload = {}
            lines.append(f"- {payload.get('location', '(unknown)')} until {item.get('expires_at')}")
        await send_text(update, "\n".join(lines))
        return
    now = datetime.now(timezone.utc)
    starts_at = now.replace(microsecond=0).isoformat()
    expires_at = (now + timedelta(days=days)).replace(microsecond=0).isoformat()
    context_id = db.create_temporary_context(
        user_id=int(user["id"]),
        context_type="travel",
        payload={"location": location, "days": days},
        starts_at=starts_at,
        expires_at=expires_at,
        db_path=runtime_db_path(config),
    )
    db.append_profile_event(
        user_id=int(user["id"]),
        event_type="temporary_travel_context_created",
        payload={"context_id": context_id, "location": location, "days": days},
        db_path=runtime_db_path(config),
    )
    await send_text(update, f"Travel override set: {location} for {days} days.")


async def sources_handler(update: Any, context: Any) -> None:
    chat = update.effective_chat
    if chat is None:
        return
    config = app_config.load_app_config()
    user = db_users.ensure_user(chat_id=int(chat.id), db_path=runtime_db_path(config))
    if not context.args:
        prefs = db.list_source_preferences(user_id=int(user["id"]), db_path=runtime_db_path(config))
        if not prefs:
            await send_text(update, "No source preferences yet. Use /sources add <domain> or /sources deny <domain>.")
            return
        lines = ["Source preferences:"]
        for row in prefs:
            lines.append(f"- {row.get('domain')}: {row.get('preference')} (tier={row.get('trust_tier')})")
        await send_text(update, "\n".join(lines))
        return
    action = str(context.args[0]).strip().lower()
    if action == "list":
        context.args = []
        await sources_handler(update, context)
        return
    if len(context.args) < 2:
        await send_text(update, "Use /sources add|deny|remove <domain>")
        return
    domain = str(context.args[1]).strip().lower().removeprefix("https://").removeprefix("http://").strip("/")
    if not domain:
        await send_text(update, "Invalid domain.")
        return
    preference = "neutral"
    tier = 0
    if action == "add":
        preference = "allow"
        tier = 2
    elif action == "deny":
        preference = "deny"
        tier = -1
    elif action in {"remove", "clear"}:
        preference = "neutral"
        tier = 0
    else:
        await send_text(update, "Use /sources add|deny|remove <domain>")
        return
    db.set_source_preference(
        user_id=int(user["id"]),
        domain=domain,
        preference=preference,
        trust_tier=tier,
        db_path=runtime_db_path(config),
    )
    await send_text(update, f"Source preference updated: {domain} -> {preference}")


async def subtopics_handler(update: Any, context: Any) -> None:
    chat = update.effective_chat
    if chat is None:
        return
    config = app_config.load_app_config()
    user = db_users.ensure_user(chat_id=int(chat.id), db_path=runtime_db_path(config))
    if not context.args:
        rows = db.list_topic_weights(user_id=int(user["id"]), db_path=runtime_db_path(config))
        if not rows:
            await send_text(update, "No subtopics yet. Run /run once to seed defaults.")
            return
        grouped: dict[str, list[str]] = {}
        for row in rows:
            topic = str(row.get("topic") or "topic")
            grouped.setdefault(topic, []).append(
                f"{row.get('subtopic')} (w={row.get('weight')}, enabled={bool(row.get('enabled', True))})"
            )
        lines = ["Subtopics:"]
        for topic, entries in grouped.items():
            lines.append(f"- {topic}:")
            lines.extend(f"  - {entry}" for entry in entries[:8])
        await send_text(update, "\n".join(lines))
        return
    if len(context.args) < 3:
        await send_text(update, "Use /subtopics promote|demote|enable|disable <topic> <subtopic>")
        return
    action = str(context.args[0]).strip().lower()
    topic = str(context.args[1]).strip().lower()
    subtopic = " ".join(context.args[2:]).strip().lower().replace(" ", "-")
    rows = db.list_topic_weights(user_id=int(user["id"]), topic=topic, db_path=runtime_db_path(config))
    existing = next((row for row in rows if str(row.get("subtopic") or "") == subtopic), None)
    weight = float(existing.get("weight", 0.6)) if existing else 0.6
    enabled = bool(existing.get("enabled", True)) if existing else True
    if action == "promote":
        weight = min(1.5, weight + 0.15)
    elif action == "demote":
        weight = max(0.0, weight - 0.15)
    elif action == "enable":
        enabled = True
    elif action == "disable":
        enabled = False
    else:
        await send_text(update, "Use /subtopics promote|demote|enable|disable <topic> <subtopic>")
        return
    db.set_topic_weight(
        user_id=int(user["id"]),
        topic=topic,
        subtopic=subtopic,
        weight=weight,
        enabled=enabled,
        source="manual",
        db_path=runtime_db_path(config),
    )
    await send_text(update, f"Subtopic updated: {topic}/{subtopic} weight={weight:.2f} enabled={enabled}")


async def memory_handler(update: Any, context: Any) -> None:
    chat = update.effective_chat
    if chat is None:
        return
    config = app_config.load_app_config()
    user = db_users.ensure_user(chat_id=int(chat.id), db_path=runtime_db_path(config))
    active = db.list_active_temporary_contexts(user_id=int(user["id"]), db_path=runtime_db_path(config))
    logs = db.list_execution_logs(user_id=int(user["id"]), limit=5, db_path=runtime_db_path(config))
    if not active:
        lines = ["No active temporary memory."]
    else:
        lines = ["Active temporary memory:"]
        for row in active:
            payload = row.get("payload") or {}
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except json.JSONDecodeError:
                    payload = {}
            lines.append(f"- {row.get('context_type')}: {payload} until {row.get('expires_at')}")
    if logs:
        lines.append("")
        lines.append("Recent execution logs:")
        for row in logs[:5]:
            lines.append(f"- [{row.get('stage')}] {row.get('status')} {row.get('message') or ''}".strip())
    await send_text(update, "\n".join(lines))


async def memory_clear_handler(update: Any, context: Any) -> None:
    chat = update.effective_chat
    if chat is None:
        return
    config = app_config.load_app_config()
    user = db_users.ensure_user(chat_id=int(chat.id), db_path=runtime_db_path(config))
    context_type = str(context.args[0]).strip().lower() if context.args else None
    cleared = db.clear_temporary_contexts(
        user_id=int(user["id"]),
        context_type=context_type,
        db_path=runtime_db_path(config),
    )
    await send_text(update, f"Cleared temporary contexts: {cleared}")


async def onboard_handler(update: Any, context: Any) -> None:
    chat = update.effective_chat
    if chat is None:
        return
    if str(os.getenv("PRA_FLAG_ONBOARDING", "true")).strip().lower() not in {"1", "true", "yes", "on"}:
        await send_text(update, "Onboarding flow is disabled by feature flag.")
        return
    config = app_config.load_app_config()
    user = db_users.ensure_user(chat_id=int(chat.id), db_path=runtime_db_path(config))
    profile = db.get_profile(user_id=int(user["id"]), db_path=runtime_db_path(config)) or {}
    start_step = "language" if not profile.get("language") else "location"
    db.upsert_onboarding_session(
        user_id=int(user["id"]),
        step=start_step,
        answers={},
        pending_question=_next_onboarding_question(start_step, str(user.get("language") or "en")),
        db_path=runtime_db_path(config),
    )
    await send_text(update, _next_onboarding_question(start_step, str(user.get("language") or "en")))


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
    runtime_db_path = config.runtime_db_path
    user = db_users.ensure_user(chat_id=int(chat.id), db_path=runtime_db_path)
    latest_run = db.latest_run_for_user(int(user["id"]), db_path=runtime_db_path)
    if latest_run is None:
        await send_text(update, "Run /run before sending feedback.")
        return

    notes = " ".join(context.args[1:]).strip()
    feedback_id = db.create_run_feedback(
        user_id=int(user["id"]),
        run_id=int(latest_run["id"]),
        rating=rating,
        notes=notes,
        db_path=runtime_db_path,
    )
    await send_text(update, f"Thanks. Feedback saved with id {feedback_id}.")


async def item_feedback_callback_handler(update: Any, context: Any) -> None:
    query = getattr(update, "callback_query", None)
    if query is None:
        return
    data = str(query.data or "").strip()
    parts = data.split(":")
    if len(parts) != 3 or parts[0] != "fb":
        await query.answer("Invalid feedback action.", show_alert=False)
        return
    _, item_id, vote = parts
    rating = VOTE_TO_RATING.get(vote)
    if rating is None:
        await query.answer("Invalid vote.", show_alert=False)
        return

    config = app_config.load_app_config()
    chat = update.effective_chat if update.effective_chat is not None else getattr(query.message, "chat", None)
    if chat is None:
        await query.answer("Chat not found.", show_alert=False)
        return
    runtime_db_path = config.runtime_db_path
    user = db_users.ensure_user(chat_id=int(chat.id), db_path=runtime_db_path)
    notes = f"telegram_item_vote:{vote}"
    try:
        feedback_id = db.create_feedback(
            user_id=int(user["id"]),
            article_id=item_id,
            rating=rating,
            notes=notes,
            db_path=runtime_db_path,
        )
    except Exception:
        LOGGER.exception("Failed to persist item feedback item_id=%s chat_id=%s", item_id, chat.id)
        await query.answer("Could not save feedback.", show_alert=False)
        return
    language = db_users.get_user_language(chat_id=int(chat.id), db_path=runtime_db_path) or config.default_language
    _, ack = _feedback_labels(str(language).strip().lower())
    try:
        await query.answer(ack, show_alert=False)
        if query.message is not None:
            await query.message.reply_text(f"{ack} (id={feedback_id})")
            await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        LOGGER.exception("Could not ack callback feedback for chat_id=%s", chat.id)


async def fallback_handler(update: Any, context: Any) -> None:
    text = update.message.text.strip() if update.message and update.message.text else ""
    chat = update.effective_chat
    if chat is None or not text:
        return
    config = app_config.load_app_config()
    user = db_users.ensure_user(chat_id=int(chat.id), db_path=runtime_db_path(config))
    session = db.get_onboarding_session(user_id=int(user["id"]), db_path=runtime_db_path(config))
    if session:
        step = str(session.get("step") or "language")
        answers = session.get("answers") or {}
        if isinstance(answers, str):
            try:
                answers = json.loads(answers)
            except json.JSONDecodeError:
                answers = {}
        if step == "language":
            candidate = text.strip().lower()
            if candidate not in SUPPORTED_LANGUAGES:
                await send_text(update, "Language must be one of: en, it, nl")
                return
            answers["language"] = candidate
            db_users.update_user_language(chat_id=int(chat.id), language=candidate, db_path=runtime_db_path(config))
            db.upsert_profile(user_id=int(user["id"]), language=candidate, db_path=runtime_db_path(config))
        elif step == "location":
            answers["home_location"] = text.strip()
            db.upsert_profile(user_id=int(user["id"]), home_location=text.strip(), db_path=runtime_db_path(config))
        elif step == "topics":
            topics = _topics_from_text(text)
            if not topics:
                await send_text(update, "Please provide at least one topic.")
                return
            answers["topics"] = topics
            db_users.update_user_topics(chat_id=int(chat.id), topics=topics, db_path=runtime_db_path(config))
            profile = db.get_profile(user_id=int(user["id"]), db_path=runtime_db_path(config)) or {}
            explicit = dict(profile.get("explicit_preferences") or {})
            explicit["topics"] = topics
            db.upsert_profile(user_id=int(user["id"]), explicit_preferences=explicit, db_path=runtime_db_path(config))
        elif step == "depth":
            depth = text.strip().lower()
            if depth not in {"brief", "standard", "deep"}:
                await send_text(update, "Depth must be: brief / standard / deep")
                return
            answers["desired_depth"] = depth
            db.upsert_profile(user_id=int(user["id"]), desired_depth=depth, db_path=runtime_db_path(config))

        next_step = _advance_onboarding_step(step)
        if next_step is None:
            db.clear_onboarding_session(user_id=int(user["id"]), db_path=runtime_db_path(config))
            db.append_profile_event(
                user_id=int(user["id"]),
                event_type="onboarding_completed",
                payload=answers,
                db_path=runtime_db_path(config),
            )
            await send_text(update, "Onboarding completed. You can now use /run.")
            return
        next_question = _next_onboarding_question(next_step, str(answers.get("language") or user.get("language") or "en"))
        db.upsert_onboarding_session(
            user_id=int(user["id"]),
            step=next_step,
            answers=answers,
            pending_question=next_question,
            db_path=runtime_db_path(config),
        )
        await send_text(update, next_question)
        return

    digest = _latest_digest_for_chat(context, int(chat.id))
    if digest and isinstance(digest.get("items"), list) and digest["items"]:
        index = _resolve_item_reference(text, digest["items"])
        if index is None and _looks_like_drilldown_request(text) and len(_normalize_text(text)) >= 8:
            index = _resolve_item_reference_llm(text, digest["items"])
        if index is not None:
            selected = next((item for item in digest["items"] if int(item.get("index") or 0) == index), None)
            if selected is not None:
                language = str(digest.get("language") or user.get("language") or config.default_language).strip().lower()
                await send_text(update, _format_detail_response(selected, language))
                return

    await send_text(
        update,
        "Send /run to generate a digest. Controls: /detail /profile /location /travel /sources /subtopics /memory /memory_clear /feedback.",
    )


def build_application(token: str) -> Any:
    from telegram.ext import Application, CallbackQueryHandler, CommandHandler, MessageHandler, filters

    application = Application.builder().token(token).build()
    application.add_handler(CommandHandler("start", start_handler))
    application.add_handler(CommandHandler("ping", ping_handler))
    application.add_handler(CommandHandler(["run", "news"], run_handler))
    application.add_handler(CommandHandler("detail", detail_handler))
    application.add_handler(CommandHandler(["topics", "settopics"], topics_handler))
    application.add_handler(CommandHandler("language", language_handler))
    application.add_handler(CommandHandler("profile", profile_handler))
    application.add_handler(CommandHandler("location", location_handler))
    application.add_handler(CommandHandler("travel", travel_handler))
    application.add_handler(CommandHandler("sources", sources_handler))
    application.add_handler(CommandHandler("subtopics", subtopics_handler))
    application.add_handler(CommandHandler("memory", memory_handler))
    application.add_handler(CommandHandler("memory_clear", memory_clear_handler))
    application.add_handler(CommandHandler("onboard", onboard_handler))
    application.add_handler(CommandHandler("feedback", feedback_handler))
    application.add_handler(CallbackQueryHandler(item_feedback_callback_handler, pattern=r"^fb:[^:]+:(like|dislike|star)$"))
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
    db.initialize_database(config.runtime_db_path)
    db_users.seed_users_from_config(db_path=config.runtime_db_path)

    if args.dry_run:
        db_label = config.database_url if config.db_backend == "postgres" else config.db_path
        print(f"telegram_dry_run=pass db={db_label} token_configured={config.telegram_token_configured}")
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
        config.database_url if config.db_backend == "postgres" else config.db_path,
    )
    application.run_polling()


if __name__ == "__main__":
    main()
