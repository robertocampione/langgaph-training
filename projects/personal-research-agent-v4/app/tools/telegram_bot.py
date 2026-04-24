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
from app import pipeline as research_pipeline  # noqa: E402


LOGGER = logging.getLogger(__name__)
SUPPORTED_LANGUAGES = {"en", "it", "nl"}
TELEGRAM_MESSAGE_LIMIT = 4096
MAX_TELEGRAM_ITEMS = 5
DEFAULT_TRAVEL_DAYS = 7
MAX_DETAIL_BODY_CHARS = 1400
CLARIFY_MAX_ATTEMPTS = 4
INTAKE_MAX_ATTEMPTS = 4

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

GENERIC_TOPIC_TERMS = {
    "news",
    "notizie",
    "nieuws",
    "events",
    "eventi",
    "general",
    "generale",
    "update",
    "updates",
    "local",
    "world",
    "mondo",
}

CLARIFY_CONTINUE_TERMS = {
    "continue",
    "continua",
    "skip",
    "forza",
    "force",
    "run",
    "vai",
}
NON_GEOGRAPHIC_AREA_VALUES = {
    "area",
    "location",
    "locatie",
    "geo",
    "global",
    "local",
    "auto",
    "news",
    "notizie",
    "nieuws",
    "events",
    "eventi",
    "evenementen",
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
        "Commands: /run, /detail, /profile, /topic_scope, /location, /travel, /sources, /subtopics, /memory, /topics, /language, /onboard, /reset_intake, /feedback"
    )


def parse_topics_args(args: list[str]) -> list[str]:
    if not args:
        return []
    cleaned_args = [item.strip().lower() for item in args if item and item.strip()]
    if not cleaned_args:
        return []
    if any("," in item or ";" in item or "\n" in item for item in cleaned_args):
        parts: list[str] = []
        for item in cleaned_args:
            normalized = item.replace(";", ",").replace("\n", ",")
            parts.extend(part.strip() for part in normalized.split(","))
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


def _truncate_log_text(text: str, limit: int = 1200) -> str:
    cleaned = str(text or "").strip()
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 3].rstrip() + "..."


def _detect_message_command(text: str) -> str:
    stripped = str(text or "").strip()
    if not stripped.startswith("/"):
        return ""
    token = stripped.split(maxsplit=1)[0]
    token = token.lstrip("/")
    if "@" in token:
        token = token.split("@", 1)[0]
    return token.strip().lower()


def _log_telegram_conversation(
    update: Any,
    direction: str,
    text: str,
    event: str,
    extra: dict[str, Any] | None = None,
) -> None:
    chat = getattr(update, "effective_chat", None)
    if chat is None:
        return
    payload_extra = dict(extra or {})
    try:
        config = app_config.load_app_config()
        user_name = None
        effective_user = getattr(update, "effective_user", None)
        if effective_user is not None:
            user_name = getattr(effective_user, "full_name", None)
        user = db_users.ensure_user(chat_id=int(chat.id), name=user_name, db_path=runtime_db_path(config))
        payload = {
            "direction": direction,
            "event": event,
            "chat_id": int(chat.id),
            "message_text": _truncate_log_text(text),
            **payload_extra,
        }
        db.append_execution_log(
            user_id=int(user["id"]),
            run_id=None,
            stage="telegram_conversation",
            status=direction,
            message=event,
            payload=payload,
            db_path=runtime_db_path(config),
        )
    except Exception:
        LOGGER.debug("telegram conversation log failed direction=%s event=%s", direction, event, exc_info=True)


def _audit_handler(handler: Any, event_name: str) -> Any:
    async def wrapped(update: Any, context: Any) -> None:
        text = ""
        args: list[str] = []
        if getattr(update, "message", None) is not None and getattr(update.message, "text", None):
            text = str(update.message.text)
        callback_query = getattr(update, "callback_query", None)
        if not text and callback_query is not None:
            text = str(getattr(callback_query, "data", "") or "")
        if context is not None:
            context_args = getattr(context, "args", None)
            if isinstance(context_args, list):
                args = [str(item) for item in context_args]
        command = _detect_message_command(text)
        _log_telegram_conversation(
            update=update,
            direction="user",
            text=text or event_name,
            event=event_name,
            extra={"command": command, "args": args},
        )
        await handler(update, context)

    return wrapped


async def send_text(update: Any, text: str, disable_preview: bool = True) -> None:
    if update.effective_chat is None:
        return
    _log_telegram_conversation(update=update, direction="bot", text=text, event="send_text")
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


def _is_generic_topic(topic: str) -> bool:
    value = _normalize_text(topic).lower()
    if not value:
        return True
    if value in GENERIC_TOPIC_TERMS:
        return True
    token_count = len(value.split())
    if token_count == 1 and len(value) <= 5:
        return True
    if token_count == 1:
        return True
    return False


def _generic_topics(topics: list[str]) -> list[str]:
    return [topic for topic in topics if _is_generic_topic(topic)]


def _should_request_topic_clarification(topics: list[str]) -> tuple[bool, list[str]]:
    if not topics:
        return True, []
    generic = _generic_topics(topics)
    if not generic:
        return False, []
    specific_count = sum(1 for topic in topics if not _is_generic_topic(topic))
    if specific_count == 0:
        return True, generic
    if len(generic) >= 2 and len(generic) >= max(2, len(topics) - 1):
        return True, generic
    return False, generic


def _clarification_prompt(language: str, topics: list[str], generic_topics: list[str]) -> str:
    visible_topics = ", ".join(topics) if topics else "(none)"
    visible_generic = ", ".join(generic_topics) if generic_topics else visible_topics
    if language == "it":
        return (
            "Per darti notizie più concrete devo restringere i topic.\n"
            f"Topic attuali: {visible_topics}\n"
            f"Topic troppo generici: {visible_generic}\n\n"
            "Scrivimi 2-4 topic più specifici separati da virgola "
            "(es: italia politica economica, maastricht mobilità urbana, europa energia rinnovabile), "
            "oppure scrivi 'continua' per eseguire subito senza modifiche."
        )
    if language == "nl":
        return (
            "Om concretere resultaten te geven moet ik de topics verfijnen.\n"
            f"Huidige topics: {visible_topics}\n"
            f"Te algemene topics: {visible_generic}\n\n"
            "Stuur 2-4 specifiekere topics, komma-gescheiden "
            "(bijv. nederland energiebeleid, maastricht mobiliteit gezinnen, europa duurzame energie), "
            "of stuur 'continue' om nu zonder wijzigingen te draaien."
        )
    return (
        "To deliver more concrete results I should narrow your topics.\n"
        f"Current topics: {visible_topics}\n"
        f"Too generic: {visible_generic}\n\n"
        "Send 2-4 more specific topics separated by commas "
        "(e.g. italy economic policy, maastricht family mobility, europe renewable energy), "
        "or send 'continue' to run now without changes."
    )


def _clarification_followup_prompt(language: str, attempt: int, generic_topics: list[str]) -> str:
    generic = ", ".join(generic_topics) if generic_topics else "current topics"
    if language == "it":
        return (
            f"Non è ancora abbastanza specifico (tentativo {attempt}/{CLARIFY_MAX_ATTEMPTS}).\n"
            f"Topic ancora generici: {generic}\n\n"
            "Riformuliamo così: per ogni topic indica almeno 2 dettagli tra "
            "sotto-tema, area geografica e orizzonte temporale.\n"
            "Esempio: 'trasporti maastricht lavori stradali prossimi 7 giorni, eventi famiglie weekend'.\n"
            "Se vuoi saltare, scrivi 'continua'."
        )
    if language == "nl":
        return (
            f"Nog niet specifiek genoeg (poging {attempt}/{CLARIFY_MAX_ATTEMPTS}).\n"
            f"Nog te algemeen: {generic}\n\n"
            "Geef per topic minstens 2 details: subonderwerp, geografie en/of tijdshorizon.\n"
            "Bijv.: 'maastricht mobiliteit wegwerkzaamheden komende 7 dagen, familie-evenementen weekend'.\n"
            "Stuur 'continue' om over te slaan."
        )
    return (
        f"Still not specific enough (attempt {attempt}/{CLARIFY_MAX_ATTEMPTS}).\n"
        f"Still generic: {generic}\n\n"
        "For each topic include at least 2 details among subtopic, geography, and time horizon.\n"
        "Example: 'maastricht mobility roadworks next 7 days, family events this weekend'.\n"
        "Send 'continue' to skip."
    )


def _is_continue_reply(text: str) -> bool:
    normalized = _normalize_text(text).lower()
    return normalized in CLARIFY_CONTINUE_TERMS


def _force_continue_requested(args: list[str]) -> bool:
    return any(str(arg).strip().lower() in {"continue", "continua"} for arg in args)


def _context_location_for_user(user_id: int, profile: dict[str, Any] | None, db_path: str | None) -> str:
    temporary_contexts = db.list_active_temporary_contexts(user_id=user_id, db_path=db_path)
    return research_pipeline.active_location_context(profile or {}, temporary_contexts)


def _profile_topic_settings(profile: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    return research_pipeline.topic_settings_from_profile(profile or {})


def _save_topic_settings_in_profile(
    user_id: int,
    profile: dict[str, Any] | None,
    topic_settings: dict[str, dict[str, Any]],
    db_path: str | None,
) -> dict[str, Any]:
    current = profile or {}
    explicit = dict(current.get("explicit_preferences") or {})
    explicit["topic_settings"] = topic_settings
    explicit["topics"] = list((explicit.get("topics") or []))
    return db.upsert_profile(user_id=user_id, explicit_preferences=explicit, db_path=db_path)


def _extract_labeled_value(text: str, labels: list[str]) -> str:
    for label in labels:
        pattern = rf"{label}\s*:\s*([^\n|]+)"
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return _normalize_text(match.group(1))
    return ""


def _normalize_area_values(raw_area: str) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()
    for part in re.split(r"[,;/|]+", str(raw_area or "")):
        cleaned = _normalize_text(part)
        lowered = cleaned.lower()
        if not cleaned:
            continue
        if lowered in NON_GEOGRAPHIC_AREA_VALUES:
            continue
        if len(cleaned) < 3:
            continue
        if lowered in seen:
            continue
        seen.add(lowered)
        values.append(cleaned)
    return values[:3]


def _extract_time_window_days(text: str) -> int | None:
    lower = _normalize_text(text).lower()
    if any(token in lower for token in {"oggi", "today", "vandaag"}):
        return 1
    if any(token in lower for token in {"weekend", "fine settimana"}):
        return 3
    day_match = re.search(r"\b(\d{1,2})\s*(giorni|giorno|days|day|dagen|dag)\b", lower)
    if day_match:
        return max(1, min(45, int(day_match.group(1))))
    week_match = re.search(r"\b(\d{1,2})\s*(settimane|settimana|weeks|week|weken|week)\b", lower)
    if week_match:
        return max(1, min(45, int(week_match.group(1)) * 7))
    month_match = re.search(r"\b(\d{1,2})\s*(mesi|mese|months|month|maanden|maand)\b", lower)
    if month_match:
        return max(1, min(90, int(month_match.group(1)) * 30))
    return None


def _extract_subtopics(text: str) -> list[str]:
    labeled = _extract_labeled_value(
        text,
        labels=["sottotemi", "sottotema", "subtopics", "subtopic", "subonderwerpen", "onderwerp"],
    )
    raw_source = labeled or text
    raw_parts = re.split(r"[,;|/]+", raw_source)
    parsed: list[str] = []
    for part in raw_parts:
        cleaned = _normalize_text(part).lower()
        cleaned = re.sub(r"\b(obiettivo|objective|doel|area|orizzonte|horizon|scope|geo_scope)\b.*", "", cleaned).strip()
        if not cleaned:
            continue
        if len(cleaned.split()) > 5:
            continue
        token = cleaned.replace(" ", "-")
        if token not in parsed and len(token) >= 3:
            parsed.append(token)
        if len(parsed) >= 6:
            break
    return parsed


def _parse_topic_intake_reply(
    text: str,
    topic: str,
    track_family: str,
    user_language: str,
    context_location: str,
) -> dict[str, Any]:
    normalized = _normalize_text(text)
    objective = _extract_labeled_value(normalized, labels=["obiettivo", "objective", "doel"])
    area = _extract_labeled_value(normalized, labels=["area", "geografia", "geo", "location", "locatie"])
    area_values = _normalize_area_values(area)
    depth = _extract_labeled_value(normalized, labels=["profondità", "depth", "diepte"]).lower()
    geo_scope_raw = _extract_labeled_value(normalized, labels=["scope", "geo_scope", "portata"]).lower()
    if not geo_scope_raw:
        if re.search(r"\b(global|globale)\b", normalized.lower()):
            geo_scope_raw = "global"
        elif re.search(r"\b(local|locale|lokale)\b", normalized.lower()):
            geo_scope_raw = "local"
        else:
            geo_scope_raw = "auto"
    time_window_days = _extract_time_window_days(normalized)
    subtopics = _extract_subtopics(normalized)
    if not objective:
        objective = normalized[:160]

    parsed = research_pipeline.normalize_topic_setting(
        topic=topic,
        raw_setting={
            "objective": objective,
            "geo_scope": geo_scope_raw,
            "locales": area_values,
            "time_window_days": time_window_days,
            "subtopics": subtopics,
            "priority": 1.0,
        },
        track_family=track_family,
        context_location=context_location,
        user_language=user_language,
    )
    if depth in {"brief", "standard", "deep"}:
        parsed["desired_depth"] = depth
    return parsed


def _topic_missing_fields(setting: dict[str, Any]) -> list[str]:
    def _list(value: Any) -> list[str]:
        if not value:
            return []
        if isinstance(value, str):
            return [value]
        if not isinstance(value, list):
            return []
        return [str(item).strip() for item in value if str(item).strip()]

    missing: list[str] = []
    if not _list(setting.get("subtopics")):
        missing.append("subtopics")
    locale_validation = str(setting.get("locales_validation") or "").strip().lower()
    if not _list(setting.get("locales")) or "invalid_input" in locale_validation:
        missing.append("area")
    if int(setting.get("time_window_days") or 0) <= 0:
        missing.append("time_window")
    return missing


def _intake_prompt_for_topic(language: str, topic: str, attempt: int = 0, missing_fields: list[str] | None = None) -> str:
    missing_fields = missing_fields or []
    missing_hint = ""
    if missing_fields:
        missing_hint = f"\nMissing: {', '.join(missing_fields)}"
    if language == "it":
        return (
            f"Topic `{topic}`: per procedere mi servono dettagli azionabili.\n"
            "Rispondi in questo formato: "
            "Obiettivo: ... | Area: ... | Orizzonte: ... | Sottotemi: ...\n"
            "Esempio: Obiettivo: impatto mobilita eventi | Area: Maastricht | "
            "Orizzonte: prossimi 7 giorni | Sottotemi: traffico, concerti, family.\n"
            + (f"Tentativo {attempt}/{INTAKE_MAX_ATTEMPTS}." if attempt else "")
            + missing_hint
        )
    if language == "nl":
        return (
            f"Topic `{topic}`: ik heb actiegerichte details nodig.\n"
            "Antwoord met: Doel: ... | Area: ... | Horizon: ... | Subtopics: ...\n"
            "Voorbeeld: Doel: impact mobiliteit events | Area: Maastricht | "
            "Horizon: komende 7 dagen | Subtopics: verkeer, concerten, familie.\n"
            + (f"Poging {attempt}/{INTAKE_MAX_ATTEMPTS}." if attempt else "")
            + missing_hint
        )
    return (
        f"Topic `{topic}`: I need actionable details before running.\n"
        "Reply with: Objective: ... | Area: ... | Horizon: ... | Subtopics: ...\n"
        "Example: Objective: mobility impact of events | Area: Maastricht | "
        "Horizon: next 7 days | Subtopics: traffic, concerts, family.\n"
        + (f"Attempt {attempt}/{INTAKE_MAX_ATTEMPTS}." if attempt else "")
        + missing_hint
    )


def _ensure_topic_settings_and_gate(user: dict[str, Any], config: app_config.AppConfig) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    profile = db.get_profile(user_id=int(user["id"]), db_path=runtime_db_path(config)) or {}
    context_location = _context_location_for_user(int(user["id"]), profile, runtime_db_path(config))
    topics = [research_pipeline.normalize_topic_text(topic) for topic in research_pipeline.normalize_topics_for_run(user.get("topics"))]
    topic_settings, _ = research_pipeline.ensure_topic_settings(
        user_id=int(user["id"]),
        topics=topics,
        profile=profile,
        context_location=context_location,
        user_language=str(user.get("language") or config.default_language),
        db_path=runtime_db_path(config),
    )
    profile = db.get_profile(user_id=int(user["id"]), db_path=runtime_db_path(config)) or {}
    gate = research_pipeline.intake_hard_gate_status(user=user, profile=profile, topic_settings=topic_settings)
    return profile, topic_settings, gate


def _start_topic_intake_session(
    user: dict[str, Any],
    config: app_config.AppConfig,
    gate: dict[str, Any],
    mode: str,
    max_results_per_query: int,
    fallback_to_stub: bool,
) -> str:
    topics_queue = list(gate.get("insufficient_topics") or gate.get("topics") or [])
    current_topic = topics_queue[0] if topics_queue else ""
    language = str(user.get("language") or config.default_language).strip().lower()
    prompt = _intake_prompt_for_topic(language, current_topic) if current_topic else _clarification_prompt(language, [], [])
    db.upsert_onboarding_session(
        user_id=int(user["id"]),
        step="intake_topic_details",
        answers={
            "mode": mode,
            "max_results_per_query": max_results_per_query,
            "fallback_to_stub": fallback_to_stub,
            "topics_queue": topics_queue,
            "current_topic": current_topic,
            "attempts": {},
        },
        pending_question=prompt,
        db_path=runtime_db_path(config),
    )
    db.append_profile_event(
        user_id=int(user["id"]),
        event_type="hard_gate_intake_started",
        payload={"topics_queue": topics_queue, "gate": gate},
        db_path=runtime_db_path(config),
    )
    return prompt


def _next_onboarding_question(step: str, language: str) -> str:
    lang = language if language in SUPPORTED_LANGUAGES else "en"
    prompts = {
        "en": {
            "language": "Choose language: en / it / nl",
            "location": "Where are you based now? (example: Maastricht)",
            "topics": "Main topics? (comma-separated, e.g. local mobility, european policy, juventus)",
            "depth": "Depth preference? brief / standard / deep",
        },
        "it": {
            "language": "Scegli lingua: en / it / nl",
            "location": "Dove sei basato ora? (esempio: Maastricht)",
            "topics": "Topic principali? (separati da virgola, es: mobilita locale, politica europea, juventus)",
            "depth": "Profondità preferita? brief / standard / deep",
        },
        "nl": {
            "language": "Kies taal: en / it / nl",
            "location": "Waar ben je nu gevestigd? (bijv. Maastricht)",
            "topics": "Belangrijkste topics? (komma-gescheiden, bijv. lokale mobiliteit, europees beleid, juventus)",
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
        _log_telegram_conversation(
            update=update,
            direction="bot",
            text=f"{caption}: {file_path.name}",
            event="send_document",
            extra={"document_path": str(file_path)},
        )
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
    profile, _topic_settings, gate = _ensure_topic_settings_and_gate(user, config)
    if _profile_incomplete(profile):
        await onboard_handler(update, context)
        return
    if gate.get("required"):
        existing_session = db.get_onboarding_session(user_id=int(user["id"]), db_path=runtime_db_path(config))
        if existing_session and str(existing_session.get("step") or "").strip().lower() == "intake_topic_details":
            prompt = str(existing_session.get("pending_question") or "").strip()
            if prompt:
                await send_text(update, prompt)
            return
        prompt = _start_topic_intake_session(
            user=user,
            config=config,
            gate=gate,
            mode=str(context.application.bot_data.get("run_mode", "auto")),
            max_results_per_query=int(context.application.bot_data.get("max_results_per_query", 2)),
            fallback_to_stub=bool(context.application.bot_data.get("fallback_to_stub", True)),
        )
        await send_text(update, prompt)


async def ping_handler(update: Any, context: Any) -> None:
    config = app_config.load_app_config()
    db_label = config.database_url if config.db_backend == "postgres" else config.db_path
    await send_text(
        update,
        f"pong db={db_label} token_configured={config.telegram_token_configured}",
    )


async def _execute_digest_run(
    update: Any,
    context: Any,
    chat_id: int,
    mode: str,
    max_results_per_query: int,
    fallback_to_stub: bool,
    announce: str = "Running your research digest now.",
) -> dict[str, Any] | None:
    chat = update.effective_chat
    if chat is None:
        return None
    await send_text(update, announce)
    try:
        result = await asyncio.to_thread(
            agent_main.run_for_chat_detailed,
            int(chat_id),
            mode,
            max_results_per_query,
            fallback_to_stub,
        )
    except Exception:
        LOGGER.exception("Pipeline run failed for chat_id=%s", chat_id)
        await send_text(update, "Sorry, I could not process that request.")
        return None

    if result.get("summary"):
        await send_text(update, result["summary"])

    language = str(result.get("language") or "en").strip().lower()
    compact = str(result.get("telegram_compact") or "").strip()
    if compact:
        await send_text(update, compact)

    enriched_items = result.get("enriched_items", [])
    _save_last_digest_for_chat(context, int(chat_id), result)
    digest = _latest_digest_for_chat(context, int(chat_id))
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
        _log_telegram_conversation(
            update=update,
            direction="bot",
            text=text,
            event="send_item_feedback_prompt",
            extra={"item_id": item_id},
        )
        await chat.send_message(text=text, reply_markup=item_feedback_keyboard(item_id), disable_web_page_preview=True)

    newsletter_sent = await send_markdown_file(update, str(result.get("newsletter_path") or ""), "Newsletter")
    report_sent = await send_markdown_file(update, str(result.get("report_path") or ""), "Report")
    if not newsletter_sent and result.get("newsletter"):
        await send_text(update, result["newsletter"])
    if not report_sent and result.get("report"):
        await send_text(update, result["report"])

    quality_status = str(result.get("quality_status") or "").strip().lower()
    selected_counts = result.get("selected_counts") or {}
    if quality_status == "warn" and isinstance(selected_counts, dict):
        missing = [str(topic) for topic, count in selected_counts.items() if int(count or 0) == 0]
        if missing:
            if language == "it":
                await send_text(
                    update,
                    "Nota qualità: mancano risultati solidi per "
                    + ", ".join(missing)
                    + ". Se vuoi, aggiorna i topic con /topics e poi rilancia /run.",
                )
            elif language == "nl":
                await send_text(
                    update,
                    "Kwaliteitsnotitie: er ontbreken sterke resultaten voor "
                    + ", ".join(missing)
                    + ". Werk eventueel topics bij met /topics en start daarna opnieuw met /run.",
                )
            else:
                await send_text(
                    update,
                    "Quality note: strong results are missing for "
                    + ", ".join(missing)
                    + ". You can refine topics with /topics and run /run again.",
                )
    return result


async def run_handler(update: Any, context: Any) -> None:
    chat = update.effective_chat
    if chat is None:
        return
    config = app_config.load_app_config()
    user = db_users.ensure_user(chat_id=int(chat.id), db_path=runtime_db_path(config))
    mode = str(context.application.bot_data.get("run_mode", "auto"))
    max_results_per_query = int(context.application.bot_data.get("max_results_per_query", 2))
    fallback_to_stub = bool(context.application.bot_data.get("fallback_to_stub", True))
    force_run = _force_continue_requested(context.args)

    session = db.get_onboarding_session(user_id=int(user["id"]), db_path=runtime_db_path(config))
    if session and str(session.get("step") or "").strip().lower() in {"clarify_topics", "intake_topic_details"} and not force_run:
        prompt = str(session.get("pending_question") or "").strip()
        if prompt:
            await send_text(update, prompt)
        else:
            await send_text(update, "Please reply to the intake question or run /run continue.")
        return

    profile_current, _topic_settings, gate = _ensure_topic_settings_and_gate(user, config)
    if _profile_incomplete(profile_current) and not force_run:
        await send_text(update, "Let's complete your base profile first.")
        await onboard_handler(update, context)
        return
    if gate.get("required") and not force_run:
        prompt = _start_topic_intake_session(
            user=user,
            config=config,
            gate=gate,
            mode=mode,
            max_results_per_query=max_results_per_query,
            fallback_to_stub=fallback_to_stub,
        )
        await send_text(update, prompt)
        return
    if gate.get("required") and force_run:
        db.append_profile_event(
            user_id=int(user["id"]),
            event_type="hard_gate_bypassed",
            payload={"source": "run_continue", "gate": gate},
            db_path=runtime_db_path(config),
        )

    await _execute_digest_run(
        update=update,
        context=context,
        chat_id=int(chat.id),
        mode=mode,
        max_results_per_query=max_results_per_query,
        fallback_to_stub=fallback_to_stub,
    )


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
    _profile, _topic_settings, _gate = _ensure_topic_settings_and_gate(user, config)
    await send_text(update, f"Updated topics for {user['name']}: " + ", ".join(updated["topics"]))


async def topic_scope_handler(update: Any, context: Any) -> None:
    chat = update.effective_chat
    if chat is None:
        return
    config = app_config.load_app_config()
    user = db_users.ensure_user(chat_id=int(chat.id), db_path=runtime_db_path(config))
    profile = db.get_profile(user_id=int(user["id"]), db_path=runtime_db_path(config)) or {}
    context_location = _context_location_for_user(int(user["id"]), profile, runtime_db_path(config))
    topic_settings = _profile_topic_settings(profile)
    topics = [research_pipeline.normalize_topic_text(topic) for topic in research_pipeline.normalize_topics_for_run(user.get("topics"))]

    if len(context.args) < 2:
        lines = ["Topic scopes:"]
        for topic in topics:
            setting = research_pipeline.normalize_topic_setting(
                topic=topic,
                raw_setting=topic_settings.get(topic) or {},
                track_family=research_pipeline.infer_track_family(topic),
                context_location=context_location,
                user_language=str(user.get("language") or config.default_language),
            )
            lines.append(
                f"- {topic}: scope={setting.get('geo_scope')} locales={','.join(setting.get('locales') or []) or '-'} "
                f"time_window={setting.get('time_window_days')}d"
            )
        lines.append("Use: /topic_scope <topic> auto|local|global")
        await send_text(update, "\n".join(lines))
        return

    requested_scope = str(context.args[-1]).strip().lower()
    if requested_scope not in research_pipeline.TOPIC_SCOPE_VALUES:
        await send_text(update, "Scope must be one of: auto, local, global")
        return
    topic = research_pipeline.normalize_topic_text(" ".join(context.args[:-1]))
    if not topic:
        await send_text(update, "Use: /topic_scope <topic> auto|local|global")
        return
    if topic not in topics:
        await send_text(update, f"Topic `{topic}` is not active. Add it first with /topics.")
        return

    current = topic_settings.get(topic) or {}
    updated_setting = research_pipeline.normalize_topic_setting(
        topic=topic,
        raw_setting={**current, "geo_scope": requested_scope},
        track_family=research_pipeline.infer_track_family(topic),
        context_location=context_location,
        user_language=str(user.get("language") or config.default_language),
    )
    topic_settings[topic] = updated_setting
    _save_topic_settings_in_profile(int(user["id"]), profile, topic_settings, runtime_db_path(config))
    _profile, _settings, _gate = _ensure_topic_settings_and_gate(user, config)
    await send_text(
        update,
        f"Updated scope: {topic} -> {requested_scope} (locales={','.join(updated_setting.get('locales') or []) or '-'})",
    )


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
    topic_settings = _profile_topic_settings(profile)
    context_location = _context_location_for_user(int(user["id"]), profile, runtime_db_path(config))
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
    for topic in [research_pipeline.normalize_topic_text(item) for item in research_pipeline.normalize_topics_for_run(user.get("topics"))]:
        setting = research_pipeline.normalize_topic_setting(
            topic=topic,
            raw_setting=topic_settings.get(topic) or {},
            track_family=research_pipeline.infer_track_family(topic),
            context_location=context_location,
            user_language=str(profile.get("language") or user.get("language") or config.default_language),
        )
        lines.append(
            f"- topic[{topic}]: subtopics={','.join(setting.get('subtopics') or []) or '-'} "
            f"scope={setting.get('geo_scope')} locales={','.join(setting.get('locales') or []) or '-'} "
            f"time_window={setting.get('time_window_days')}d priority={setting.get('priority')} confirmed={bool(setting.get('confirmed', False))}"
        )
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
    requested = str(context.args[0]).strip().lower() if context.args else ""
    force_full = requested in {"full", "reset", "restart", "from_scratch", "scratch"}
    start_step = "language" if force_full or not profile.get("language") else "location"
    if force_full:
        db.clear_onboarding_session(user_id=int(user["id"]), db_path=runtime_db_path(config))
        db.append_profile_event(
            user_id=int(user["id"]),
            event_type="onboarding_restart_requested",
            payload={"source": "telegram_command"},
            db_path=runtime_db_path(config),
        )
    db.upsert_onboarding_session(
        user_id=int(user["id"]),
        step=start_step,
        answers={},
        pending_question=_next_onboarding_question(start_step, str(user.get("language") or "en")),
        db_path=runtime_db_path(config),
    )
    await send_text(update, _next_onboarding_question(start_step, str(user.get("language") or "en")))


async def reset_intake_handler(update: Any, context: Any) -> None:
    chat = update.effective_chat
    if chat is not None:
        config = app_config.load_app_config()
        user = db_users.ensure_user(chat_id=int(chat.id), db_path=runtime_db_path(config))
        profile = db.get_profile(user_id=int(user["id"]), db_path=runtime_db_path(config)) or {}
        explicit = dict(profile.get("explicit_preferences") or {})
        explicit["topic_settings"] = {}
        db.upsert_profile(user_id=int(user["id"]), explicit_preferences=explicit, db_path=runtime_db_path(config))
        db.clear_onboarding_session(user_id=int(user["id"]), db_path=runtime_db_path(config))
    context.args = ["full"]
    await onboard_handler(update, context)


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
            _log_telegram_conversation(
                update=update,
                direction="bot",
                text=f"{ack} (id={feedback_id})",
                event="callback_feedback_ack",
                extra={"item_id": item_id, "vote": vote, "feedback_id": feedback_id},
            )
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
        if step == "intake_topic_details":
            if _is_continue_reply(text):
                db.clear_onboarding_session(user_id=int(user["id"]), db_path=runtime_db_path(config))
                db.append_profile_event(
                    user_id=int(user["id"]),
                    event_type="hard_gate_bypassed",
                    payload={"source": "intake_continue_message"},
                    db_path=runtime_db_path(config),
                )
                await _execute_digest_run(
                    update=update,
                    context=context,
                    chat_id=int(chat.id),
                    mode=str(answers.get("mode") or context.application.bot_data.get("run_mode", "auto")),
                    max_results_per_query=int(
                        answers.get("max_results_per_query") or context.application.bot_data.get("max_results_per_query", 2)
                    ),
                    fallback_to_stub=bool(
                        answers.get("fallback_to_stub")
                        if "fallback_to_stub" in answers
                        else context.application.bot_data.get("fallback_to_stub", True)
                    ),
                    announce="Running with explicit continue override.",
                )
                return
            profile = db.get_profile(user_id=int(user["id"]), db_path=runtime_db_path(config)) or {}
            context_location = _context_location_for_user(int(user["id"]), profile, runtime_db_path(config))
            topic_settings = _profile_topic_settings(profile)
            topics_queue = [research_pipeline.normalize_topic_text(topic) for topic in list(answers.get("topics_queue") or [])]
            current_topic = research_pipeline.normalize_topic_text(str(answers.get("current_topic") or ""))
            if not current_topic and topics_queue:
                current_topic = topics_queue[0]
            if not current_topic:
                db.clear_onboarding_session(user_id=int(user["id"]), db_path=runtime_db_path(config))
                await send_text(update, "Intake session completed. Use /run.")
                return

            family = research_pipeline.infer_track_family(current_topic)
            parsed_setting = _parse_topic_intake_reply(
                text=text,
                topic=current_topic,
                track_family=family,
                user_language=str(user.get("language") or config.default_language),
                context_location=context_location,
            )
            merged_raw = dict(topic_settings.get(current_topic) or {})
            merged_raw.update(
                {
                    "objective": parsed_setting.get("objective"),
                    "geo_scope": parsed_setting.get("geo_scope"),
                    "locales": parsed_setting.get("locales"),
                    "time_window_days": parsed_setting.get("time_window_days"),
                    "subtopics": parsed_setting.get("subtopics"),
                    "priority": parsed_setting.get("priority"),
                    "confirmed": True,
                }
            )
            normalized_setting = research_pipeline.normalize_topic_setting(
                topic=current_topic,
                raw_setting=merged_raw,
                track_family=family,
                context_location=context_location,
                user_language=str(user.get("language") or config.default_language),
            )
            topic_settings[current_topic] = normalized_setting
            _save_topic_settings_in_profile(int(user["id"]), profile, topic_settings, runtime_db_path(config))
            if parsed_setting.get("desired_depth") in {"brief", "standard", "deep"}:
                db.upsert_profile(
                    user_id=int(user["id"]),
                    desired_depth=str(parsed_setting.get("desired_depth")),
                    db_path=runtime_db_path(config),
                )

            _profile_after, _settings_after, gate = _ensure_topic_settings_and_gate(user, config)
            insufficient_topics = [research_pipeline.normalize_topic_text(topic) for topic in list(gate.get("insufficient_topics") or [])]
            attempts = dict(answers.get("attempts") or {})
            current_attempt = int(attempts.get(current_topic, 0) or 0)
            if current_topic in insufficient_topics:
                current_attempt += 1
                attempts[current_topic] = current_attempt
                missing = _topic_missing_fields(normalized_setting)
                prompt = _intake_prompt_for_topic(
                    str(user.get("language") or config.default_language),
                    current_topic,
                    attempt=current_attempt,
                    missing_fields=missing,
                )
                if current_attempt >= INTAKE_MAX_ATTEMPTS:
                    if str(user.get("language") or "en").strip().lower() == "it":
                        prompt += "\n\nPuoi anche forzare con /run continue."
                    elif str(user.get("language") or "en").strip().lower() == "nl":
                        prompt += "\n\nJe kunt ook forceren met /run continue."
                    else:
                        prompt += "\n\nYou can also override with /run continue."
                db.upsert_onboarding_session(
                    user_id=int(user["id"]),
                    step="intake_topic_details",
                    answers={**answers, "attempts": attempts, "current_topic": current_topic, "topics_queue": topics_queue},
                    pending_question=prompt,
                    db_path=runtime_db_path(config),
                )
                await send_text(update, prompt)
                return

            remaining = [topic for topic in topics_queue if topic != current_topic and topic in insufficient_topics]
            for topic in insufficient_topics:
                if topic not in remaining:
                    remaining.append(topic)
            if remaining:
                next_topic = remaining[0]
                prompt = _intake_prompt_for_topic(str(user.get("language") or config.default_language), next_topic)
                db.upsert_onboarding_session(
                    user_id=int(user["id"]),
                    step="intake_topic_details",
                    answers={**answers, "attempts": attempts, "current_topic": next_topic, "topics_queue": remaining},
                    pending_question=prompt,
                    db_path=runtime_db_path(config),
                )
                if str(user.get("language") or "en").strip().lower() == "it":
                    await send_text(update, f"Perfetto, `{current_topic}` è configurato. Passiamo al prossimo topic.")
                elif str(user.get("language") or "en").strip().lower() == "nl":
                    await send_text(update, f"Top, `{current_topic}` is ingesteld. Volgende topic.")
                else:
                    await send_text(update, f"Great, `{current_topic}` is set. Let's continue with the next topic.")
                await send_text(update, prompt)
                return

            db.clear_onboarding_session(user_id=int(user["id"]), db_path=runtime_db_path(config))
            db.append_profile_event(
                user_id=int(user["id"]),
                event_type="hard_gate_intake_completed",
                payload={"topic_settings_updated": True},
                db_path=runtime_db_path(config),
            )
            if str(user.get("language") or "en").strip().lower() == "it":
                await send_text(update, "Perfetto, intake completato. Avvio la run.")
            elif str(user.get("language") or "en").strip().lower() == "nl":
                await send_text(update, "Top, intake afgerond. Ik start nu de run.")
            else:
                await send_text(update, "Great, intake complete. Starting the run now.")
            await _execute_digest_run(
                update=update,
                context=context,
                chat_id=int(chat.id),
                mode=str(answers.get("mode") or context.application.bot_data.get("run_mode", "auto")),
                max_results_per_query=int(
                    answers.get("max_results_per_query") or context.application.bot_data.get("max_results_per_query", 2)
                ),
                fallback_to_stub=bool(
                    answers.get("fallback_to_stub")
                    if "fallback_to_stub" in answers
                    else context.application.bot_data.get("fallback_to_stub", True)
                ),
                announce="Running with your intake profile.",
            )
            return
        if step == "clarify_topics":
            if _is_continue_reply(text):
                db.clear_onboarding_session(user_id=int(user["id"]), db_path=runtime_db_path(config))
                await _execute_digest_run(
                    update=update,
                    context=context,
                    chat_id=int(chat.id),
                    mode=str(answers.get("mode") or context.application.bot_data.get("run_mode", "auto")),
                    max_results_per_query=int(
                        answers.get("max_results_per_query") or context.application.bot_data.get("max_results_per_query", 2)
                    ),
                    fallback_to_stub=bool(
                        answers.get("fallback_to_stub") if "fallback_to_stub" in answers else context.application.bot_data.get("fallback_to_stub", True)
                    ),
                    announce="Ok, running with current topics.",
                )
                return
            topics = _topics_from_text(text)
            if not topics:
                language = str(user.get("language") or config.default_language).strip().lower()
                attempts = int(answers.get("clarify_attempts", 0) or 0) + 1
                generic_existing = _generic_topics(list(user.get("topics") or []))
                prompt = _clarification_followup_prompt(language, attempts, generic_existing)
                db.upsert_onboarding_session(
                    user_id=int(user["id"]),
                    step="clarify_topics",
                    answers={**answers, "clarify_attempts": attempts},
                    pending_question=prompt,
                    db_path=runtime_db_path(config),
                )
                await send_text(update, prompt)
                return
            should_clarify, generic = _should_request_topic_clarification(topics)
            if should_clarify:
                attempts = int(answers.get("clarify_attempts", 0) or 0) + 1
                language = str(user.get("language") or config.default_language).strip().lower()
                if attempts >= CLARIFY_MAX_ATTEMPTS:
                    if language == "it":
                        tail = "\n\nUsa /run continue se vuoi procedere comunque."
                    elif language == "nl":
                        tail = "\n\nGebruik /run continue om toch door te gaan."
                    else:
                        tail = "\n\nUse /run continue to proceed anyway."
                    prompt = (
                        _clarification_followup_prompt(language, attempts, generic)
                        + tail
                    )
                else:
                    prompt = _clarification_followup_prompt(language, attempts, generic)
                db.upsert_onboarding_session(
                    user_id=int(user["id"]),
                    step="clarify_topics",
                    answers={**answers, "clarify_attempts": attempts},
                    pending_question=prompt,
                    db_path=runtime_db_path(config),
                )
                await send_text(update, prompt)
                return
            user_updated = db_users.update_user_topics(chat_id=int(chat.id), topics=topics, db_path=runtime_db_path(config))
            profile = db.get_profile(user_id=int(user["id"]), db_path=runtime_db_path(config)) or {}
            explicit = dict(profile.get("explicit_preferences") or {})
            explicit["topics"] = topics
            db.upsert_profile(user_id=int(user["id"]), explicit_preferences=explicit, db_path=runtime_db_path(config))
            db.append_profile_event(
                user_id=int(user["id"]),
                event_type="clarification_completed",
                payload={"topics": topics},
                db_path=runtime_db_path(config),
            )
            db.clear_onboarding_session(user_id=int(user["id"]), db_path=runtime_db_path(config))
            language = str(user.get("language") or config.default_language).strip().lower()
            if language == "it":
                await send_text(update, "Perfetto, topic aggiornati: " + ", ".join(user_updated.get("topics") or topics))
            elif language == "nl":
                await send_text(update, "Top, topics bijgewerkt: " + ", ".join(user_updated.get("topics") or topics))
            else:
                await send_text(update, "Great, updated topics: " + ", ".join(user_updated.get("topics") or topics))
            await _execute_digest_run(
                update=update,
                context=context,
                chat_id=int(chat.id),
                mode=str(answers.get("mode") or context.application.bot_data.get("run_mode", "auto")),
                max_results_per_query=int(
                    answers.get("max_results_per_query") or context.application.bot_data.get("max_results_per_query", 2)
                ),
                fallback_to_stub=bool(
                    answers.get("fallback_to_stub") if "fallback_to_stub" in answers else context.application.bot_data.get("fallback_to_stub", True)
                ),
                announce="Running with refined topics.",
            )
            return
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
            _profile_after, _topic_settings_after, _gate_after = _ensure_topic_settings_and_gate(user, config)
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
        "Send /run to generate a digest. Controls: /detail /profile /topic_scope /location /travel /sources /subtopics /memory /memory_clear /onboard /reset_intake /feedback.",
    )


async def telegram_error_handler(update: Any, context: Any) -> None:
    error = context.error
    error_name = type(error).__name__ if error is not None else "UnknownError"
    if error_name in {"ReadError", "NetworkError", "TimedOut"}:
        LOGGER.warning("Transient Telegram network error: %s", error_name)
        return
    LOGGER.exception("Unhandled Telegram update error (%s)", error_name, exc_info=error)


def build_application(token: str) -> Any:
    from telegram.ext import Application, CallbackQueryHandler, CommandHandler, MessageHandler, filters

    application = Application.builder().token(token).build()
    application.add_handler(CommandHandler("start", _audit_handler(start_handler, "cmd_start")))
    application.add_handler(CommandHandler("ping", _audit_handler(ping_handler, "cmd_ping")))
    application.add_handler(CommandHandler("run", _audit_handler(run_handler, "cmd_run")))
    application.add_handler(CommandHandler("detail", _audit_handler(detail_handler, "cmd_detail")))
    application.add_handler(CommandHandler(["topics", "settopics"], _audit_handler(topics_handler, "cmd_topics")))
    application.add_handler(CommandHandler("topic_scope", _audit_handler(topic_scope_handler, "cmd_topic_scope")))
    application.add_handler(CommandHandler("language", _audit_handler(language_handler, "cmd_language")))
    application.add_handler(CommandHandler("profile", _audit_handler(profile_handler, "cmd_profile")))
    application.add_handler(CommandHandler("location", _audit_handler(location_handler, "cmd_location")))
    application.add_handler(CommandHandler("travel", _audit_handler(travel_handler, "cmd_travel")))
    application.add_handler(CommandHandler("sources", _audit_handler(sources_handler, "cmd_sources")))
    application.add_handler(CommandHandler("subtopics", _audit_handler(subtopics_handler, "cmd_subtopics")))
    application.add_handler(CommandHandler("memory", _audit_handler(memory_handler, "cmd_memory")))
    application.add_handler(CommandHandler("memory_clear", _audit_handler(memory_clear_handler, "cmd_memory_clear")))
    application.add_handler(CommandHandler("onboard", _audit_handler(onboard_handler, "cmd_onboard")))
    application.add_handler(CommandHandler("reset_intake", _audit_handler(reset_intake_handler, "cmd_reset_intake")))
    application.add_handler(CommandHandler("feedback", _audit_handler(feedback_handler, "cmd_feedback")))
    application.add_handler(
        CallbackQueryHandler(
            _audit_handler(item_feedback_callback_handler, "callback_feedback"),
            pattern=r"^fb:[^:]+:(like|dislike|star)$",
        )
    )
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, _audit_handler(fallback_handler, "message_text")))
    application.add_error_handler(telegram_error_handler)
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
