"""Console research pipeline for Personal Research Agent v4."""

from __future__ import annotations

import json
import os
import logging
import re
import base64
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote_plus, urlencode, urlparse

from app import config as app_config
from app import db
from app import db_users
from app import llm
from app.nodes import interpretation as interpretation_node


LOGGER = logging.getLogger(__name__)


TRACKS = ("news", "events", "bitcoin")
DEFAULT_MAX_RESULTS_PER_QUERY = 2
MAX_ITEMS_TO_PROCESS = 10
MAX_ITEMS_TO_OUTPUT = 5
MAX_TOKENS_PER_RUN = 20000
MAX_LLM_ITEMS_PER_RUN = 3
DEFAULT_CONTEXT_LOCATION = "Maastricht"
TAVILY_ENDPOINT = "https://api.tavily.com/search"
GOOGLE_NEWS_RSS_ENDPOINT = "https://news.google.com/rss/search"
BING_NEWS_RSS_ENDPOINT = "https://www.bing.com/news/search"
LOW_VALUE_DOMAINS = {
    "ground.news",
    "linkees.com",
    "getyourguide.com",
    "coinmarketcap.com",
}
PREFERRED_DOMAINS = {
    "news": {"1limburg.nl", "dutchnews.nl", "nltimes.nl", "maastrichtuniversity.nl"},
    "events": {"visitmaastricht.com", "visitzuidlimburg.com", "maastrichtbereikbaar.nl", "maastrichtuniversity.nl"},
    "bitcoin": {"bitcoinops.org", "github.com", "bitcoinmagazine.com", "coindesk.com"},
}
SOURCE_TRUST_TIERS = {
    "news": {"1limburg.nl": 3, "dutchnews.nl": 3, "nltimes.nl": 2, "maastrichtuniversity.nl": 2},
    "events": {"visitmaastricht.com": 3, "visitzuidlimburg.com": 3, "maastrichtbereikbaar.nl": 2},
    "bitcoin": {"github.com": 3, "bitcoinops.org": 3, "coindesk.com": 2, "bitcoinmagazine.com": 2},
}
DEFAULT_SUBTOPIC_PACKS = {
    "bitcoin": {"market": 0.9, "policy": 0.7, "regulation": 0.75, "onchain": 0.8, "mining": 0.65, "core-dev": 0.85},
    "events": {"local-calendar": 0.9, "culture": 0.7, "family": 0.75, "transit-impact": 0.6},
    "news": {"local-governance": 0.7, "safety": 0.85, "economy": 0.8, "infrastructure": 0.75},
}
MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}
SUPPORTED_LANGUAGES = {"en", "it", "nl"}
RSS_LOCALE = {
    "en": {"hl": "en-US", "gl": "US", "ceid": "US:en"},
    "it": {"hl": "it", "gl": "IT", "ceid": "IT:it"},
    "nl": {"hl": "nl", "gl": "NL", "ceid": "NL:nl"},
}
BING_NEWS_SETLANG = {
    "en": "en-US",
    "it": "it-IT",
    "nl": "nl-NL",
}
GOOGLE_NEWS_RESOLVE_TIMEOUT_SECONDS = 7
URL_RESOLVE_CACHE: dict[str, str] = {}
GOOGLE_NEWS_TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]+$")
MAX_NEWS_AGE_DAYS = 7
MAX_BITCOIN_AGE_DAYS = 7
MAX_FINANCE_AGE_DAYS = 7
MAX_EVENT_PAST_DAYS = 7
MAX_EVENT_FUTURE_DAYS = 120


def _age_cap_days_for_track(
    track_family: str,
    topic_settings: dict[str, dict[str, Any]] | None = None,
    track_type: str = "",
) -> int:
    """Return freshness cap in days, honouring time_window_days from topic_settings when available.

    Priority: topic_settings[track_type].time_window_days
              > topic_settings[track_family].time_window_days
              > global constant fallback
    """
    if topic_settings:
        for key in (track_type, track_family):
            if not key:
                continue
            setting = topic_settings.get(key) or {}
            window = int(setting.get("time_window_days") or 0)
            if window > 0:
                return window
    _caps: dict[str, int] = {
        "news": MAX_NEWS_AGE_DAYS,
        "finance": MAX_FINANCE_AGE_DAYS,
        "bitcoin": MAX_BITCOIN_AGE_DAYS,
    }
    return _caps.get(track_family, MAX_NEWS_AGE_DAYS)
GENERIC_TOPIC_TERMS = {
    "news",
    "notizie",
    "nieuws",
    "events",
    "eventi",
    "evenementen",
    "general",
    "generale",
    "update",
    "updates",
    "local",
    "world",
    "mondo",
}
TOPIC_SCOPE_VALUES = {"auto", "local", "global"}
TOPIC_TOKEN_STOPWORDS = {
    "news",
    "notizie",
    "nieuws",
    "event",
    "events",
    "eventi",
    "evenementen",
    "latest",
    "ultime",
    "oggi",
    "today",
    "week",
    "weekend",
    "update",
    "updates",
}
DUTCH_LOCAL_SIGNALS = {
    "maastricht",
    "limburg",
    "netherlands",
    "nederland",
    "holland",
    "amsterdam",
    "rotterdam",
    "utrecht",
    "the hague",
    "den haag",
    "eindhoven",
}
LANGUAGE_REGION_TOKEN_RE = re.compile(r"^[a-z]{2}(?:[-_][a-z]{2})?$", flags=re.IGNORECASE)
NON_GEOGRAPHIC_LOCALE_TOKENS = {
    "auto",
    "global",
    "local",
    "world",
    "mondo",
    "news",
    "notizie",
    "nieuws",
    "events",
    "eventi",
    "evenementen",
}
GEO_HINTS = {
    "maastricht": {"locale": "Maastricht", "languages": ["nl", "en"]},
    "limburg": {"locale": "Limburg", "languages": ["nl", "en"]},
    "netherlands": {"locale": "Netherlands", "languages": ["nl", "en"]},
    "nederland": {"locale": "Netherlands", "languages": ["nl", "en"]},
    "holland": {"locale": "Netherlands", "languages": ["nl", "en"]},
    "amsterdam": {"locale": "Amsterdam", "languages": ["nl", "en"]},
    "rotterdam": {"locale": "Rotterdam", "languages": ["nl", "en"]},
    "utrecht": {"locale": "Utrecht", "languages": ["nl", "en"]},
    "italia": {"locale": "Italy", "languages": ["it", "en"]},
    "italy": {"locale": "Italy", "languages": ["it", "en"]},
    "roma": {"locale": "Rome", "languages": ["it", "en"]},
    "rome": {"locale": "Rome", "languages": ["it", "en"]},
    "milano": {"locale": "Milan", "languages": ["it", "en"]},
    "milan": {"locale": "Milan", "languages": ["it", "en"]},
    "spain": {"locale": "Spain", "languages": ["en"]},
    "spagna": {"locale": "Spain", "languages": ["en"]},
    "espana": {"locale": "Spain", "languages": ["en"]},
    "madrid": {"locale": "Madrid", "languages": ["en"]},
    "barcelona": {"locale": "Barcelona", "languages": ["en"]},
    "germany": {"locale": "Germany", "languages": ["en"]},
    "germania": {"locale": "Germany", "languages": ["en"]},
    "deutschland": {"locale": "Germany", "languages": ["en"]},
    "berlin": {"locale": "Berlin", "languages": ["en"]},
}


def normalize_language(language: str | None) -> str:
    value = (language or "").strip().lower()
    if value in SUPPORTED_LANGUAGES:
        return value
    return "en"


def _dedupe_strings(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        token = str(value or "").strip()
        key = token.lower()
        if not token or key in seen:
            continue
        seen.add(key)
        deduped.append(token)
    return deduped


def detect_geo_hints(values: list[str]) -> dict[str, list[str]]:
    joined = " ".join(str(value or "").strip().lower() for value in values if str(value or "").strip())
    if not joined:
        return {"locales": [], "languages": []}
    locales: list[str] = []
    languages: list[str] = []
    for token, payload in GEO_HINTS.items():
        if token not in joined:
            continue
        locales.append(str(payload.get("locale") or token))
        for language in list(payload.get("languages") or []):
            normalized = normalize_language(str(language))
            if normalized not in languages:
                languages.append(normalized)
    return {"locales": _dedupe_strings(locales), "languages": _dedupe_strings(languages)}


def _locale_languages_hint(locales: list[str]) -> list[str]:
    hints = detect_geo_hints(locales)
    languages = list(hints.get("languages") or [])
    if languages:
        return [normalize_language(language) for language in languages]
    return ["en"]


def infer_retrieval_languages(
    topic: str,
    track_family: str,
    context_location: str,
    user_language: str,
    geo_scope: str = "auto",
    topic_locales: list[str] | None = None,
) -> list[str]:
    topic_value = str(topic or "").strip().lower()
    location_value = str(context_location or "").strip().lower()
    locale_values = [str(value).strip() for value in (topic_locales or []) if str(value).strip()]
    combined = " ".join([topic_value, location_value, *[value.lower() for value in locale_values]]).strip()
    has_dutch_local_signal = any(signal in combined for signal in DUTCH_LOCAL_SIGNALS)
    topic_geo_hints = detect_geo_hints([topic_value, *locale_values])
    location_geo_hints = detect_geo_hints([location_value])
    normalized_scope = str(geo_scope or "auto").strip().lower()
    if normalized_scope not in TOPIC_SCOPE_VALUES:
        normalized_scope = "auto"

    ordered: list[str] = []

    if track_family in {"bitcoin", "finance"} and normalized_scope != "local":
        ordered.extend(topic_geo_hints.get("languages") or [])
        ordered.extend(["en"])
    elif normalized_scope == "global":
        ordered.extend(topic_geo_hints.get("languages") or [])
        ordered.extend(["en", normalize_language(user_language)])
    elif normalized_scope == "local":
        local_hint = _locale_languages_hint(locale_values or [context_location])
        if not local_hint:
            local_hint = list(location_geo_hints.get("languages") or [])
        ordered.extend(local_hint or ["en"])
    elif track_family in {"events", "news"} and has_dutch_local_signal:
        ordered.extend(["nl", "en"])
    else:
        ordered.extend([normalize_language(user_language)])

    ordered.append(normalize_language(user_language))
    deduped: list[str] = []
    for language in ordered:
        normalized = normalize_language(language)
        if normalized not in deduped:
            deduped.append(normalized)
    return deduped or ["en"]


def infer_track_family(track_type: str) -> str:
    value = str(track_type or "").strip().lower()
    if value in TRACKS:
        return value
    if any(token in value for token in {"event", "festival", "concert", "meetup", "calendar", "conference"}):
        return "events"
    if any(token in value for token in {"bitcoin", "btc", "crypto", "onchain", "blockchain"}):
        return "bitcoin"
    if any(token in value for token in {"finance", "market", "macro", "econom", "policy", "report"}):
        return "finance"
    return "news"


def preferred_domains_for_track(track_type: str) -> set[str]:
    direct = PREFERRED_DOMAINS.get(track_type, set())
    if direct:
        return direct
    family = infer_track_family(track_type)
    return PREFERRED_DOMAINS.get(family, set())


def source_trust_tier_for_track(track_type: str, source: str) -> int:
    direct = SOURCE_TRUST_TIERS.get(track_type, {})
    if source in direct:
        return int(direct[source])
    family = infer_track_family(track_type)
    return int(SOURCE_TRUST_TIERS.get(family, {}).get(source, 1))


def parse_rss_pubdate(value: str | None) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = parsedate_to_datetime(raw)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(microsecond=0).isoformat()


@dataclass(frozen=True)
class PipelineResult:
    run_id: int
    report: str
    newsletter: str
    report_path: str
    newsletter_path: str
    debug_dir: str
    quality_status: str
    selected_counts: dict[str, int]
    mode: str
    language: str
    quality_flags: list[str]
    enriched_items: list[dict[str, Any]]
    telegram_compact: str
    cost_trace: dict[str, Any]


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def slug_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def project_path(path: str | Path) -> Path:
    value = Path(path)
    if value.is_absolute():
        return value
    return app_config.PROJECT_ROOT / value


def write_json(path: Path, stage: str, mode: str, context: dict[str, Any], payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "stage": stage,
                "mode": mode,
                "created_at": utc_now(),
                "context": context,
                "payload": payload,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def env_flag(name: str, default: bool) -> bool:
    value = os.getenv(name, "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on"}


def write_kb_log(run_id: int, payload: dict[str, Any]) -> Path:
    kb_dir = project_path("kb_logs")
    kb_dir.mkdir(parents=True, exist_ok=True)
    path = kb_dir / f"{slug_timestamp()}__v4-{run_id}.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def write_debug_brief(
    debug_dir: Path,
    retrieval_trace: dict[str, Any],
    cost_trace: dict[str, Any],
    personalization: dict[str, Any],
) -> None:
    lines = [
        "# Debug Brief",
        "",
        "## Retrieval",
        json.dumps(retrieval_trace, indent=2, sort_keys=True),
        "",
        "## Cost",
        json.dumps(cost_trace, indent=2, sort_keys=True),
        "",
        "## Personalization",
        json.dumps(personalization, indent=2, sort_keys=True),
    ]
    (debug_dir / "debug_brief.md").write_text("\n".join(lines), encoding="utf-8")


def normalize_topics_for_run(topics: list[str] | tuple[str, ...] | str | None) -> list[str]:
    if not topics:
        return list(app_config.DEFAULT_TOPICS)
    # Auto-parse if topics arrived as a JSON-encoded string (e.g. from SQLite row)
    if isinstance(topics, str):
        try:
            parsed = json.loads(topics)
            topics = parsed if isinstance(parsed, list) else [topics]
        except (json.JSONDecodeError, ValueError):
            topics = [topics]
    normalized: list[str] = []
    seen: set[str] = set()
    for topic in topics:
        value = str(topic).strip().lower()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized or list(app_config.DEFAULT_TOPICS)



def normalize_topic_text(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def is_generic_topic(topic: str) -> bool:
    value = normalize_topic_text(topic)
    if not value:
        return True
    if value in GENERIC_TOPIC_TERMS:
        return True
    if len(value.split()) == 1 and len(value) <= 4:
        return True
    return False


def _to_list_of_strings(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, str):
        return [value]
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _looks_like_language_region_token(value: str) -> bool:
    cleaned = normalize_topic_text(value)
    if not cleaned:
        return False
    if cleaned in SUPPORTED_LANGUAGES:
        return True
    if LANGUAGE_REGION_TOKEN_RE.match(cleaned):
        return True
    return False


def _has_geographic_signal(value: str) -> bool:
    cleaned = re.sub(r"\s+", " ", str(value or "").strip())
    if not cleaned:
        return False
    lowered = cleaned.lower()
    if lowered in NON_GEOGRAPHIC_LOCALE_TOKENS:
        return False
    if _looks_like_language_region_token(cleaned):
        return False
    if not any(ch.isalpha() for ch in cleaned):
        return False
    if len(cleaned) < 3:
        return False
    return True


def _normalize_locales(locales: list[str]) -> tuple[list[str], bool]:
    valid: list[str] = []
    seen: set[str] = set()
    had_invalid = False
    for locale in locales:
        cleaned = re.sub(r"\s+", " ", str(locale or "").strip())
        if not cleaned:
            continue
        if not _has_geographic_signal(cleaned):
            had_invalid = True
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        valid.append(cleaned)
    return valid[:3], had_invalid


def _extract_subtopic_tokens(topic: str) -> list[str]:
    cleaned = re.sub(r"[^a-z0-9\s-]", " ", normalize_topic_text(topic))
    tokens = [token.strip("-") for token in cleaned.split() if token.strip("-")]
    return [token for token in tokens if token not in TOPIC_TOKEN_STOPWORDS and len(token) >= 3]


def deterministic_subtopics_for_topic(topic: str, track_family: str) -> list[str]:
    normalized_topic = normalize_topic_text(topic)
    if normalized_topic in DEFAULT_SUBTOPIC_PACKS:
        return list(DEFAULT_SUBTOPIC_PACKS[normalized_topic].keys())[:4]
    family_defaults = {
        "events": ["agenda", "venues", "tickets", "mobility"],
        "bitcoin": ["price-action", "etf-flow", "regulation", "onchain"],
        "finance": ["macro", "rates", "equities", "earnings"],
        "news": ["policy", "economy", "public-safety", "infrastructure"],
    }
    selected: list[str] = []
    for token in _extract_subtopic_tokens(topic):
        if token not in selected:
            selected.append(token)
        if len(selected) >= 4:
            break
    for fallback in family_defaults.get(track_family, family_defaults["news"]):
        if fallback not in selected:
            selected.append(fallback)
        if len(selected) >= 4:
            break
    return selected[:4]


def _parse_llm_json_payload(raw: str) -> dict[str, Any] | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else None
    except json.JSONDecodeError:
        pass
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not match:
        return None
    try:
        parsed = json.loads(match.group(0))
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def llm_topic_setting(
    topic: str,
    track_family: str,
    user_language: str,
    context_location: str,
) -> dict[str, Any] | None:
    if not llm.llm_enabled():
        return None
    response = llm.call_llm(
        role="utility",
        system_prompt=(
            "You extract actionable topic settings for a news research assistant. "
            "Return strict JSON with keys: subtopics (array of short slugs), objective, "
            "geo_scope (auto|local|global), locales (array), time_window_days (int), priority (float), "
            "search_query_language (2-letter ISO code of the Primary Native Language of the target 'locales', e.g., 'nl' for Maastricht, 'en' for London), "
            "translated_topic_phrase (the 'topic' explicitly translated to 'search_query_language'), "
            "optimized_search_queries (array of 3 highly optimized literal search engine queries in 'search_query_language' to find relevant articles. Combine the translated topic, locales, and critical keywords natively without arbitrary concat-noise)."
        ),
        user_prompt=(
            f"topic={topic}\n"
            f"track_family={track_family}\n"
            f"user_language={user_language}\n"
            f"context_location={context_location}\n"
            "Keep subtopics generic and reusable. Max 4 subtopics. Ensure optimized_search_queries are clean, highly targeted, and strictly in the target native language of the locale to maximize SEO match."
        ),
        temperature=0.1,
        timeout_seconds=12,
    )
    if not response.get("ok"):
        return None
    return _parse_llm_json_payload(str(response.get("content") or ""))


def normalize_topic_setting(
    topic: str,
    raw_setting: dict[str, Any] | None,
    track_family: str,
    context_location: str,
    user_language: str,
) -> dict[str, Any]:
    setting = dict(raw_setting or {})
    setting["search_query_language"] = str(setting.get("search_query_language") or "").strip().lower()
    setting["translated_topic_phrase"] = str(setting.get("translated_topic_phrase") or "").strip()
    setting["optimized_search_queries"] = _to_list_of_strings(setting.get("optimized_search_queries"))
    subtopics = [token.replace(" ", "-").lower() for token in _to_list_of_strings(setting.get("subtopics"))]
    subtopics = [token for token in subtopics if token]
    if not subtopics:
        subtopics = deterministic_subtopics_for_topic(topic, track_family)

    requested_scope = normalize_topic_text(str(setting.get("geo_scope") or ""))
    if requested_scope not in TOPIC_SCOPE_VALUES:
        if track_family == "events":
            requested_scope = "local"
        elif is_generic_topic(topic) and track_family == "news":
            requested_scope = "local"
        else:
            requested_scope = "auto"

    raw_locales = _to_list_of_strings(setting.get("locales"))
    locales, had_invalid_locales = _normalize_locales(raw_locales)
    locale_validation = "provided"
    if requested_scope == "local" and not locales:
        fallback_locale = re.sub(r"\s+", " ", str(context_location or "").strip())
        if fallback_locale and _has_geographic_signal(fallback_locale):
            locales = [fallback_locale]
            locale_validation = "derived_context_invalid_input" if had_invalid_locales else "derived_context"
        else:
            locale_validation = "invalid_input" if had_invalid_locales else "missing"
    elif not locales:
        locale_validation = "invalid_input" if had_invalid_locales else "missing"
    elif had_invalid_locales:
        locale_validation = "partial_invalid_input"

    default_window = 14 if track_family == "events" else 7
    time_window_days = _safe_int(setting.get("time_window_days"), default_window)
    if time_window_days <= 0:
        time_window_days = default_window
    time_window_days = max(1, min(90, time_window_days))

    priority = _safe_float(setting.get("priority"), 1.0)
    priority = max(0.25, min(2.0, round(priority, 2)))

    raw_objective = re.sub(r"\s+", " ", str(setting.get("objective") or "").strip())
    # Clean objective from intake artifacts (e.g. "my goal |area: city |horizon: 7d")
    clean_objective = raw_objective.split("|")[0].strip()
    
    if not clean_objective or clean_objective.lower() == "research":
        if user_language == "it":
            clean_objective = f"Capire gli sviluppi recenti e rilevanti su {topic}."
        elif user_language == "nl":
            clean_objective = f"Recente en relevante ontwikkelingen rond {topic} volgen."
        else:
            clean_objective = f"Track recent and relevant developments on {topic}."

    confirmed = bool(setting.get("confirmed", False))

    return {
        "topic_name": normalize_topic_text(topic),
        "search_query_language": setting.get("search_query_language", ""),
        "translated_topic_phrase": setting.get("translated_topic_phrase", ""),
        "optimized_search_queries": setting.get("optimized_search_queries", []),
        "subtopics": subtopics[:6],
        "geo_scope": requested_scope,
        "locales": locales[:3],
        "locales_validation": locale_validation,
        "time_window_days": time_window_days,
        "priority": priority,
        "objective": clean_objective,
        "confirmed": confirmed,
    }


def topic_setting_signal_count(setting: dict[str, Any] | None) -> int:
    if not setting:
        return 0
    signals = 0
    if _to_list_of_strings(setting.get("subtopics")):
        signals += 1
    if _to_list_of_strings(setting.get("locales")):
        signals += 1
    if _safe_int(setting.get("time_window_days"), 0) > 0:
        signals += 1
    return signals


def topic_setting_is_actionable(setting: dict[str, Any] | None) -> bool:
    return topic_setting_signal_count(setting) >= 2


def topic_settings_from_profile(profile: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    explicit = dict((profile or {}).get("explicit_preferences") or {})
    raw = explicit.get("topic_settings") or {}
    if not isinstance(raw, dict):
        return {}
    parsed: dict[str, dict[str, Any]] = {}
    for topic, value in raw.items():
        if isinstance(value, dict):
            parsed[normalize_topic_text(str(topic))] = value
    return parsed


def ensure_topic_settings(
    user_id: int,
    topics: list[str],
    profile: dict[str, Any] | None,
    context_location: str,
    user_language: str,
    db_path: str | None,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    profile_current = profile or {}
    explicit = dict(profile_current.get("explicit_preferences") or {})
    existing_settings = topic_settings_from_profile(profile_current)
    generated = 0
    updated = False
    synced_settings: dict[str, dict[str, Any]] = {}

    for topic in topics:
        normalized_topic = normalize_topic_text(topic)
        family = infer_track_family(normalized_topic)
        existing = existing_settings.get(normalized_topic)
        needs_generation = False
        if not existing:
            needs_generation = True
        elif not existing.get("search_query_language") or not existing.get("translated_topic_phrase") or not existing.get("optimized_search_queries"):
            needs_generation = True
            
        if needs_generation:
            llm_setting = llm_topic_setting(
                topic=normalized_topic,
                track_family=family,
                user_language=user_language,
                context_location=context_location,
            )
            if not existing:
                existing = llm_setting or {}
            else:
                # Merge new LLM insights (translation properties) without destroying manual edits
                if llm_setting:
                    existing["search_query_language"] = llm_setting.get("search_query_language", "")
                    existing["translated_topic_phrase"] = llm_setting.get("translated_topic_phrase", "")
                    if "optimized_search_queries" in llm_setting:
                        existing["optimized_search_queries"] = llm_setting.get("optimized_search_queries", [])
                    # Optionally merge subtopics if they were completely missing
                    if not existing.get("subtopics"):
                        existing["subtopics"] = llm_setting.get("subtopics", [])
                    if not existing.get("locales"):
                        existing["locales"] = llm_setting.get("locales", [])
            generated += 1
        setting = normalize_topic_setting(
            topic=normalized_topic,
            raw_setting=existing,
            track_family=family,
            context_location=context_location,
            user_language=user_language,
        )
        if existing_settings.get(normalized_topic) != setting:
            updated = True
        synced_settings[normalized_topic] = setting

        active_subtopics = set(setting["subtopics"])
        for index, subtopic in enumerate(setting["subtopics"][:6]):
            weight = max(0.2, round(setting["priority"] - index * 0.12, 2))
            db.set_topic_weight(
                user_id=user_id,
                topic=normalized_topic,
                subtopic=subtopic,
                weight=weight,
                enabled=True,
                source="topic_setting",
                db_path=db_path,
            )
        existing_rows = db.list_topic_weights(user_id=user_id, topic=normalized_topic, db_path=db_path)
        for row in existing_rows:
            row_subtopic = str(row.get("subtopic") or "")
            row_source = str(row.get("source") or "")
            if row_subtopic and row_subtopic not in active_subtopics and row_source == "topic_setting":
                db.set_topic_weight(
                    user_id=user_id,
                    topic=normalized_topic,
                    subtopic=row_subtopic,
                    weight=float(row.get("weight") or 0.1),
                    enabled=False,
                    source="topic_setting",
                    db_path=db_path,
                )

    merged_topic_settings = dict(existing_settings)
    merged_topic_settings.update(synced_settings)
    if explicit.get("topic_settings") != merged_topic_settings:
        explicit["topic_settings"] = merged_topic_settings
        explicit["topics"] = topics
        profile_current = db.upsert_profile(user_id=user_id, explicit_preferences=explicit, db_path=db_path)
        updated = True
    trace = {
        "topic_count": len(topics),
        "generated_topics": generated,
        "profile_updated": updated,
    }
    return merged_topic_settings, trace


def intake_hard_gate_status(
    user: dict[str, Any],
    profile: dict[str, Any] | None,
    topic_settings: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    topics = normalize_topics_for_run(user.get("topics"))
    profile_obj = profile or {}
    missing_profile_fields: list[str] = []
    if not str(profile_obj.get("language") or user.get("language") or "").strip():
        missing_profile_fields.append("language")
    if not str(profile_obj.get("home_location") or "").strip():
        missing_profile_fields.append("home_location")
    explicit_topics = []
    explicit = profile_obj.get("explicit_preferences") or {}
    if isinstance(explicit, dict):
        explicit_topics = list(explicit.get("topics") or [])
    if not explicit_topics:
        missing_profile_fields.append("topics")

    topic_status: dict[str, dict[str, Any]] = {}
    insufficient_topics: list[str] = []
    for topic in topics:
        normalized_topic = normalize_topic_text(topic)
        setting = topic_settings.get(normalized_topic) or {}
        track_family = infer_track_family(normalized_topic)
        requested_scope = str(setting.get("geo_scope") or "auto").strip().lower()
        locales = _to_list_of_strings(setting.get("locales"))
        locales_validation = str(setting.get("locales_validation") or "").strip().lower()
        has_local_signal = any(_has_geographic_signal(locale) for locale in locales)
        invalid_locales = "invalid_input" in locales_validation
        is_custom_topic = normalized_topic not in TRACKS
        strict_local_scope = requested_scope == "local" and (is_custom_topic or track_family in {"events", "news"})
        signals = topic_setting_signal_count(setting)
        actionable = topic_setting_is_actionable(setting)
        is_generic = is_generic_topic(normalized_topic)
        confirmed = bool(setting.get("confirmed", False))
        insufficient_reasons: list[str] = []
        if not actionable:
            insufficient_reasons.append("not_actionable")
        if is_generic and not confirmed:
            insufficient_reasons.append("generic_not_confirmed")
        if strict_local_scope and not has_local_signal:
            insufficient_reasons.append("missing_local_scope")
        if strict_local_scope and invalid_locales:
            insufficient_reasons.append("invalid_local_scope")
        insufficient = bool(insufficient_reasons)
        if insufficient:
            insufficient_topics.append(normalized_topic)
        topic_status[normalized_topic] = {
            "is_generic": is_generic,
            "signals": signals,
            "actionable": actionable,
            "confirmed": confirmed,
            "insufficient": insufficient,
            "insufficient_reasons": insufficient_reasons,
            "locales_validation": locales_validation,
            "has_local_signal": has_local_signal,
            "setting": setting,
        }

    required = bool(missing_profile_fields or insufficient_topics)
    return {
        "required": required,
        "missing_profile_fields": missing_profile_fields,
        "insufficient_topics": insufficient_topics,
        "topic_status": topic_status,
        "topics": topics,
    }


def ensure_default_topic_graph(user_id: int, topics: list[str], db_path: str | None) -> None:
    for topic in topics:
        topic_key = normalize_topic_text(topic)
        subtopics = DEFAULT_SUBTOPIC_PACKS.get(topic_key, {})
        for subtopic, weight in subtopics.items():
            db.set_topic_weight(
                user_id=user_id,
                topic=topic_key,
                subtopic=subtopic,
                weight=weight,
                enabled=True,
                source="default",
                db_path=db_path,
            )


def build_topic_plan(
    user_id: int,
    topics: list[str],
    db_path: str | None,
    topic_settings: dict[str, dict[str, Any]] | None = None,
) -> dict[str, list[str]]:
    ensure_default_topic_graph(user_id=user_id, topics=topics, db_path=db_path)
    plan: dict[str, list[str]] = {}
    topic_settings = topic_settings or {}
    for topic in topics:
        topic_key = normalize_topic_text(topic)
        rows = db.list_topic_weights(user_id=user_id, topic=topic_key, db_path=db_path)
        enabled_rows = [row for row in rows if bool(row.get("enabled", True))]
        enabled_rows.sort(key=lambda row: float(row.get("weight", 0.0)), reverse=True)
        subtopics = [str(row.get("subtopic") or "").strip() for row in enabled_rows[:3] if str(row.get("subtopic") or "").strip()]
        if not subtopics:
            fallback = _to_list_of_strings((topic_settings.get(topic_key) or {}).get("subtopics"))
            subtopics = [token.replace(" ", "-").lower() for token in fallback[:3]]
        plan[topic_key] = subtopics
    return plan


def active_location_context(profile: dict[str, Any] | None, temporary_contexts: list[dict[str, Any]]) -> str:
    for context in temporary_contexts:
        if str(context.get("context_type") or "").strip().lower() != "travel":
            continue
        payload = context.get("payload")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                payload = {}
        if isinstance(payload, dict):
            location = str(payload.get("location") or "").strip()
            if location:
                return location
    if profile:
        location = str(profile.get("home_location") or "").strip()
        if location:
            return location
    return DEFAULT_CONTEXT_LOCATION


def build_queries(
    user: dict[str, Any],
    topic_plan: dict[str, list[str]],
    context_location: str,
    topic_settings: dict[str, dict[str, Any]] | None = None,
    max_topics: int | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    topics = [normalize_topic_text(topic) for topic in normalize_topics_for_run(user.get("topics"))]
    language = normalize_language(str(user.get("language") or app_config.DEFAULT_LANGUAGE))
    topic_settings = topic_settings or {}
    if max_topics is not None:
        topics = topics[:max_topics]
    generic_templates = {
        "it": {
            "default": [
                "{topic_phrase} ultime notizie oggi",
                "{topic_phrase} aggiornamenti ultime 24 ore",
                "{topic_phrase} sviluppi principali settimana corrente",
            ],
            "events": [
                "{topic_phrase} eventi prossimi date location",
                "{topic_phrase} calendario weekend",
                "{topic_phrase} programma ufficiale eventi",
            ],
        },
        "nl": {
            "default": [
                "{topic_phrase} laatste nieuws vandaag",
                "{topic_phrase} updates laatste 24 uur",
                "{topic_phrase} belangrijkste ontwikkelingen deze week",
            ],
            "events": [
                "{topic_phrase} komende evenementen data locatie",
                "{topic_phrase} weekend agenda",
                "{topic_phrase} officieel evenementenprogramma",
            ],
        },
        "en": {
            "default": [
                "{topic_phrase} latest news today",
                "{topic_phrase} updates in the last 24 hours",
                "{topic_phrase} key developments this week",
            ],
            "events": [
                "{topic_phrase} upcoming events dates location",
                "{topic_phrase} weekend calendar",
                "{topic_phrase} official event schedule",
            ],
        },
    }

    def _dedupe_keep_order(values: list[str]) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []
        for value in values:
            normalized = " ".join(value.strip().split()).lower()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            ordered.append(" ".join(value.strip().split()))
        return ordered

    def _topic_label(topic: str) -> str:
        return str(topic).replace("-", " ").replace("_", " ").strip()

    def _is_location_sensitive(topic: str, topic_setting: dict[str, Any]) -> bool:
        value = str(topic or "").strip().lower()
        if str(topic_setting.get("geo_scope") or "").strip().lower() == "local":
            return True
        if value in {"news", "events", "local", "city", "travel"}:
            return True
        token_count = len(value.split())
        if token_count <= 2 and ("event" in value or "news" in value or "local" in value):
            return True
        return False

    def _topic_phrase(topic: str, selected_subtopics: list[str], location: str, query_language: str, topic_setting: dict[str, Any]) -> str:
        topic_value = str(topic or "").strip().lower()
        family = infer_track_family(topic)
        if topic_value in {"news", "notizie", "nieuws"}:
            if query_language == "it":
                return f"notizie locali {location} olanda".strip()
            if query_language == "nl":
                return f"lokaal nieuws {location} nederland".strip()
            return f"local news {location} netherlands".strip()
        if topic_value in {"events", "eventi", "evenementen"}:
            if query_language == "it":
                return f"eventi {location} weekend famiglie".strip()
            if query_language == "nl":
                return f"evenementen {location} weekend gezinnen".strip()
            return f"events {location} weekend families".strip()
        if topic_value in {"bitcoin", "btc", "crypto"} and family == "bitcoin":
            if query_language == "it":
                return "bitcoin prezzo etf regolamentazione mercati"
            if query_language == "nl":
                return "bitcoin prijs etf regelgeving markten"
            return "bitcoin price etf regulation markets"

        parts: list[str] = [str(topic_setting.get("translated_topic_phrase") or _topic_label(topic))]
        if location and _is_location_sensitive(topic, topic_setting):
            normalized_base = " ".join(parts).lower()
            normalized_location = location.strip().lower()
            if normalized_location and normalized_location not in normalized_base:
                parts.append(location.strip())
        for subtopic in selected_subtopics[:2]:
            token = str(subtopic).replace("-", " ").replace("_", " ").strip()
            if token:
                parts.append(token)
        return " ".join(part for part in parts if part).strip()

    def _templates_for_topic(topic: str, query_language: str) -> list[str]:
        family = infer_track_family(topic)
        lang = normalize_language(query_language)
        if lang not in generic_templates:
            lang = "en"
        language_pack = generic_templates[lang]
        if family == "events":
            return language_pack["events"]
        return language_pack["default"]

    def _recency_hint_for_topic(topic: str, topic_setting: dict[str, Any]) -> str:
        family = infer_track_family(topic)
        requested_window = _safe_int(topic_setting.get("time_window_days"), 14 if family == "events" else 7)
        requested_window = max(1, min(45, requested_window))
        if family == "events":
            return f"when:{requested_window}d"
        return f"when:{min(requested_window, 14)}d"

    queries: list[dict[str, Any]] = []
    analyst_reasoning_topics: dict[str, Any] = {}
    current_year = datetime.now(timezone.utc).year
    built_in_topics = {"news", "notizie", "nieuws", "events", "eventi", "evenementen", "bitcoin", "btc", "crypto"}
    for topic in topics:
        raw_setting = topic_settings.get(topic, {})
        family = infer_track_family(topic)
        normalized_setting = normalize_topic_setting(
            topic=topic,
            raw_setting=raw_setting,
            track_family=family,
            context_location=context_location,
            user_language=language,
        )
        selected_subtopics = topic_plan.get(topic, []) or list(normalized_setting.get("subtopics") or [])[:3]
        topic_value = str(topic or "").strip().lower()
        is_custom_topic = topic_value not in built_in_topics
        requested_scope = str(normalized_setting.get("geo_scope") or "auto").strip().lower()
        if requested_scope not in TOPIC_SCOPE_VALUES:
            requested_scope = "auto"
        scope_source = "override"
        if requested_scope == "auto":
            local_signals = (
                family == "events"
                or _is_location_sensitive(topic, normalized_setting)
                or bool(_to_list_of_strings(normalized_setting.get("locales")))
            )
            topic_scope_decision = "local" if local_signals else "global"
            scope_source = "auto"
        else:
            topic_scope_decision = requested_scope
        topic_locales = _to_list_of_strings(normalized_setting.get("locales"))
        if topic_scope_decision == "local" and not topic_locales:
            topic_locales = [context_location]
        location_target = topic_locales[0] if topic_locales else context_location
        if normalized_setting.get("search_query_language"):
            retrieval_languages = [normalize_language(normalized_setting["search_query_language"])]
        else:
            retrieval_languages = infer_retrieval_languages(
                topic=topic,
                track_family=family,
                context_location=context_location,
                user_language=language,
                geo_scope=topic_scope_decision,
                topic_locales=topic_locales,
            )
        query_language = retrieval_languages[0]
        phrase = _topic_phrase(
            topic=topic,
            selected_subtopics=selected_subtopics,
            location=location_target,
            query_language=query_language,
            topic_setting=normalized_setting,
        )
        recency_hint = _recency_hint_for_topic(topic, normalized_setting)
        optimized_search_queries = _to_list_of_strings(normalized_setting.get("optimized_search_queries"))
        
        if optimized_search_queries:
            template_queries = [f"{q} {recency_hint}".strip() for q in optimized_search_queries]
        else:
            template_queries = [
                f"{template.format(topic_phrase=phrase)} {recency_hint}".strip()
                for template in _templates_for_topic(topic, query_language=query_language)
            ]
        if not optimized_search_queries:
            for subtopic in selected_subtopics[:2]:
                subtopic_text = str(subtopic).replace("-", " ").strip()
                if subtopic_text:
                    template_queries.append(f"{_topic_label(topic)} {subtopic_text} {recency_hint}".strip())
            if is_custom_topic:
                if family == "events":
                    template_queries.extend(
                        [
                            f"{phrase} komende evenementen data locatie {recency_hint}".strip()
                            if query_language == "nl"
                            else f"{phrase} upcoming events dates location {recency_hint}".strip(),
                            f"{phrase} upcoming events dates location".strip(),
                        ]
                    )
                else:
                    template_queries.extend(
                        [
                            f"{phrase} latest developments {recency_hint}".strip(),
                            f"{phrase} latest developments".strip(),
                        ]
                    )
        if family == "events" and not optimized_search_queries:
            template_queries.append(
                f"{phrase} {current_year} {current_year + 1} dates tickets official schedule {recency_hint}".strip()
            )
        preferred_domains = sorted(preferred_domains_for_track(topic))
        site_query_limit = 1 if family in {"news", "events"} else 0
        site_queries = []
        if optimized_search_queries and len(optimized_search_queries) > 0:
            site_queries = [f"site:{domain} {optimized_search_queries[0]} {recency_hint}".strip() for domain in preferred_domains[:site_query_limit]]
        else:
            site_queries = [f"site:{domain} {phrase} {recency_hint}".strip() for domain in preferred_domains[:site_query_limit]]
        topic_queries = _dedupe_keep_order(template_queries + site_queries)[:5]
        analyst_reasoning_topics[topic] = {
            "intent": normalized_setting.get("objective"),
            "track_family": family,
            "subtopics_selected": selected_subtopics[:3],
            "topic_scope_decision": topic_scope_decision,
            "topic_scope_source": scope_source,
            "requested_geo_scope": requested_scope,
            "query_languages": retrieval_languages,
            "locales": topic_locales,
            "time_window_days": int(normalized_setting.get("time_window_days") or 7),
            "queries": topic_queries,
        }
        for query in topic_queries:
            queries.append(
                {
                    "track_type": topic,
                    "query": query,
                    "subtopics": selected_subtopics,
                    "query_language": query_language,
                    "retrieval_languages": retrieval_languages,
                    "topic_scope_decision": topic_scope_decision,
                    "topic_scope_source": scope_source,
                    "topic_locales": topic_locales,
                }
            )
    return queries, {"topics": analyst_reasoning_topics, "query_count": len(queries)}


def build_relaxed_queries(queries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    relaxed: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in queries:
        query = str(item.get("query") or "").strip()
        if not query:
            continue
        query = re.sub(r"\swhen:\d+d\b", "", query, flags=re.IGNORECASE).strip()
        query = re.sub(r"^site:[^\s]+\s+", "", query, flags=re.IGNORECASE).strip()
        normalized = " ".join(query.split()).lower()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        relaxed.append(
            {
                "track_type": str(item.get("track_type") or "news"),
                "query": query,
                "subtopics": list(item.get("subtopics") or []),
                "query_language": normalize_language(str(item.get("query_language") or "en")),
                "retrieval_languages": list(item.get("retrieval_languages") or []),
                "topic_scope_decision": str(item.get("topic_scope_decision") or "auto"),
                "topic_scope_source": str(item.get("topic_scope_source") or "auto"),
                "topic_locales": list(item.get("topic_locales") or []),
            }
        )
    return relaxed


def merge_retrieval_trace(
    base_trace: dict[str, Any],
    extra_trace: dict[str, Any],
    fallback_label: str,
    query_count_initial: int | None = None,
    query_count_relaxed: int | None = None,
) -> dict[str, Any]:
    merged_retriever = ",".join(
        sorted(
            {
                provider
                for provider in (str(base_trace.get("retriever", "")) + "," + str(extra_trace.get("retriever", ""))).split(",")
                if provider
            }
        )
    ) or str(base_trace.get("retriever", "fixture"))
    merged_provider = ",".join(
        sorted(
            {
                provider
                for provider in (
                    str(base_trace.get("retrieval_provider", "")) + "," + str(extra_trace.get("retrieval_provider", ""))
                ).split(",")
                if provider
            }
        )
    ) or str(base_trace.get("retrieval_provider", "local_fixture"))
    chain = list(base_trace.get("fallback_chain", []))
    if fallback_label not in chain:
        chain.append(fallback_label)
    merged = {
        **base_trace,
        "fallback_chain": chain,
        "providers_used": list(base_trace.get("providers_used", [])) + list(extra_trace.get("providers_used", [])),
        "retriever": merged_retriever,
        "retrieval_provider": merged_provider,
        "live_errors": list(base_trace.get("live_errors", [])) + list(extra_trace.get("live_errors", [])),
        "web_errors": list(base_trace.get("web_errors", [])) + list(extra_trace.get("web_errors", [])),
    }
    if query_count_initial is not None:
        merged["query_count_initial"] = query_count_initial
    if query_count_relaxed is not None:
        merged["query_count_relaxed"] = query_count_relaxed
    return merged


def tavily_search(query: str, max_results: int) -> list[dict[str, Any]]:
    token = os.getenv("TAVILY_API_KEY", "").strip()
    if not token:
        raise RuntimeError("TAVILY_API_KEY is required for live retrieval")
    payload = json.dumps(
        {
            "api_key": token,
            "query": query,
            "topic": "news",
            "days": 7,
            "search_depth": "advanced",
            "max_results": max_results,
            "include_answer": False,
            "include_raw_content": False,
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        TAVILY_ENDPOINT,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            data = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:240]
        detail = f"HTTP {exc.code}"
        if body:
            detail = f"{detail}: {body}"
        raise RuntimeError(f"Tavily retrieval failed: {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Tavily retrieval failed: {exc}") from exc
    results = data.get("results", [])
    return results if isinstance(results, list) else []


def strip_html(value: str) -> str:
    return re.sub(r"<[^>]+>", " ", value).replace("&nbsp;", " ").strip()


def extract_first_href(value: str) -> str:
    decoded = unescape(value or "")
    match = re.search(r"""href=["']([^"']+)["']""", decoded, flags=re.IGNORECASE)
    if not match:
        return ""
    return str(match.group(1)).strip()


def google_news_rss_search_with_locale(query: str, max_results: int, locale: dict[str, str]) -> list[dict[str, Any]]:
    temporal_query = f"{query} when:7d" if "when:" not in query else query
    url = (
        f"{GOOGLE_NEWS_RSS_ENDPOINT}?q={quote_plus(temporal_query)}"
        f"&hl={quote_plus(locale['hl'])}"
        f"&gl={quote_plus(locale['gl'])}"
        f"&ceid={quote_plus(locale['ceid'])}"
    )
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) PRA-v4/1.0"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            xml_payload = response.read()
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Google News RSS retrieval failed: {exc}") from exc

    root = ET.fromstring(xml_payload)
    results: list[dict[str, Any]] = []
    for item in root.findall(".//item")[:max_results]:
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        description_raw = item.findtext("description") or ""
        description = strip_html(description_raw)
        published_at = parse_rss_pubdate(item.findtext("pubDate"))
        description_url = extract_first_href(description_raw)
        source_tag = item.find("source")
        source_text = (source_tag.text or "").strip() if source_tag is not None else ""
        source_url = source_tag.attrib.get("url", "") if source_tag is not None else ""
        best_source_url = source_url
        if description_url and domain_from_url(description_url) != "news.google.com":
            best_source_url = description_url
        if not link:
            continue
        results.append(
            {
                "title": title or link,
                "url": link,
                "content": description,
                "score": 0.55,
                "source_label": source_text,
                "source_url": best_source_url,
                "published_at": published_at,
                "published_at_confidence": 0.9 if published_at else 0.0,
            }
        )
    return results


def decode_bing_apiclick_url(raw_url: str) -> str:
    if not raw_url:
        return raw_url
    parsed = urlparse(raw_url)
    if parsed.netloc.lower() not in {"www.bing.com", "bing.com"}:
        return raw_url
    if "/news/apiclick" not in parsed.path.lower():
        return raw_url
    params = parse_qs(parsed.query)
    target = (params.get("url", [None])[0] or params.get("u", [None])[0] or "").strip()
    return target or raw_url


def bing_news_rss_search(query: str, max_results: int, language: str) -> list[dict[str, Any]]:
    setlang = BING_NEWS_SETLANG.get(normalize_language(language), BING_NEWS_SETLANG["en"])
    url = f"{BING_NEWS_RSS_ENDPOINT}?q={quote_plus(query)}&format=rss&setlang={quote_plus(setlang)}"
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) PRA-v4/1.0"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            xml_payload = response.read()
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Bing News RSS retrieval failed: {exc}") from exc

    root = ET.fromstring(xml_payload)
    results: list[dict[str, Any]] = []
    for item in root.findall(".//item")[:max_results]:
        title = (item.findtext("title") or "").strip()
        link = decode_bing_apiclick_url((item.findtext("link") or "").strip())
        description = strip_html(item.findtext("description") or "")
        published_at = parse_rss_pubdate(item.findtext("pubDate"))
        if not link:
            continue
        source_label = ""
        if " - " in title:
            source_label = title.rsplit(" - ", 1)[-1].strip()
        results.append(
            {
                "title": title or link,
                "url": link,
                "content": description,
                "score": 0.6,
                "source_label": source_label,
                "source_url": link,
                "published_at": published_at,
                "published_at_confidence": 0.85 if published_at else 0.0,
            }
        )
    return results


def google_news_rss_search(query: str, max_results: int, language: str) -> list[dict[str, Any]]:
    locale = RSS_LOCALE.get(normalize_language(language), RSS_LOCALE["en"])
    results = google_news_rss_search_with_locale(query, max_results, locale)
    if results or locale == RSS_LOCALE["en"]:
        return results
    return google_news_rss_search_with_locale(query, max_results, RSS_LOCALE["en"])


def decode_google_news_token_simple(token: str) -> str:
    if not token or not GOOGLE_NEWS_TOKEN_RE.match(token):
        return ""
    try:
        decoded_bytes = base64.urlsafe_b64decode(token + "==")
    except Exception:
        return ""
    try:
        decoded_text = decoded_bytes.decode("latin1")
    except Exception:
        return ""

    prefix = b"\x08\x13\x22".decode("latin1")
    if decoded_text.startswith(prefix):
        decoded_text = decoded_text[len(prefix) :]
    suffix = b"\xd2\x01\x00".decode("latin1")
    if decoded_text.endswith(suffix):
        decoded_text = decoded_text[: -len(suffix)]
    if not decoded_text:
        return ""
    length = bytearray(decoded_text, "latin1")[0]
    if length >= 0x80:
        decoded_text = decoded_text[2 : length + 1]
    else:
        decoded_text = decoded_text[1 : length + 1]
    if decoded_text.startswith("AU_yqL"):
        return ""
    if decoded_text.startswith("http://") or decoded_text.startswith("https://"):
        return decoded_text
    return ""


def decode_google_news_token_batchexecute(token: str, timeout_seconds: int = 8) -> str:
    if not token:
        return ""
    payload = (
        '[[["Fbv4je","[\\"garturlreq\\",[[\\"en-US\\",\\"US\\",[\\"FINANCE_TOP_INDICES\\",\\"WEB_TEST_1_0_0\\"],'
        'null,null,1,1,\\"US:en\\",null,180,null,null,null,null,null,0,null,null,[1608992183,723341000]],'
        '\\"en-US\\",\\"US\\",1,[2,3,4,8],1,0,\\"655000234\\",0,0,null,0],\\"'
        + token
        + '\\"]",null,"1"]]]'
    )
    request = urllib.request.Request(
        "https://news.google.com/_/DotsSplashUi/data/batchexecute?rpcids=Fbv4je",
        data=urlencode({"f.req": payload}).encode("utf-8"),
        headers={
            "Content-Type": "application/x-www-form-urlencoded;charset=utf-8",
            "Referer": "https://news.google.com/",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) PRA-v4/1.0",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            body = response.read().decode("utf-8", errors="replace")
    except Exception:
        return ""

    header = '[\\"garturlres\\",\\"'
    footer = '\\",'
    if header not in body:
        return ""
    raw_value = body.split(header, 1)[1]
    if footer not in raw_value:
        return ""
    raw_value = raw_value.split(footer, 1)[0].strip()
    if not raw_value:
        return ""
    raw_value = raw_value.replace("\\/", "/")
    try:
        raw_value = raw_value.encode("utf-8").decode("unicode_escape")
    except Exception:
        return ""
    if raw_value.startswith("http://") or raw_value.startswith("https://"):
        return raw_value
    return ""


def resolve_google_news_url(raw_url: str, source_url: str = "") -> str:
    if not raw_url:
        return raw_url
    domain = domain_from_url(raw_url)
    if domain != "news.google.com":
        return raw_url

    cached = URL_RESOLVE_CACHE.get(raw_url)
    if cached:
        return cached

    resolved = raw_url
    try:
        request = urllib.request.Request(
            raw_url,
            headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) PRA-v4/1.0"},
            method="GET",
        )
        with urllib.request.urlopen(request, timeout=GOOGLE_NEWS_RESOLVE_TIMEOUT_SECONDS) as response:
            response_url = str(response.geturl() or "").strip()
            if response_url:
                resolved = response_url
    except Exception:
        resolved = raw_url

    parsed = urlparse(resolved)
    query = parse_qs(parsed.query)
    direct_url = (query.get("url", [None])[0] or query.get("u", [None])[0] or "").strip()
    if direct_url:
        resolved = direct_url

    if domain_from_url(resolved) == "news.google.com":
        token = parsed.path.rstrip("/").split("/")[-1]
        decoded_simple = decode_google_news_token_simple(token)
        if decoded_simple:
            resolved = decoded_simple
        else:
            decoded_remote = decode_google_news_token_batchexecute(token)
            if decoded_remote:
                resolved = decoded_remote

    # If redirection collapses to a generic publisher homepage, preserve the
    # Google article URL instead of returning a weak root-domain link.
    resolved_domain = domain_from_url(resolved)
    resolved_path = urlparse(resolved).path.strip()
    if resolved_domain != "news.google.com" and resolved_path in {"", "/"}:
        resolved = raw_url

    if resolved == raw_url and source_url:
        source_path = urlparse(source_url).path.strip()
        if source_path and source_path != "/":
            resolved = source_url

    URL_RESOLVE_CACHE[raw_url] = resolved
    return resolved


def resolve_search_result_url(raw_url: str, source_url: str = "") -> str:
    if not raw_url:
        return raw_url
    domain = domain_from_url(raw_url)
    if domain in {"www.bing.com", "bing.com"}:
        return decode_bing_apiclick_url(raw_url)
    if domain == "news.google.com":
        return resolve_google_news_url(raw_url, source_url)
    return raw_url


def fixture_results(track_type: str) -> list[dict[str, Any]]:
    now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    fixtures = {
        "news": [
            {
                "title": "Limburg mobility plan gets fresh funding",
                "url": "https://example.com/2026/04/22/limburg-mobility-plan",
                "content": (
                    "Regional authorities approved new mobility funding around Maastricht, including "
                    "updated public transport timelines and road safety interventions for commuters."
                ),
                "score": 0.82,
                "published_at": now_iso,
                "published_at_confidence": 1.0,
            }
        ],
        "events": [
            {
                "title": "Family science weekend in Maastricht",
                "url": "https://example.com/events/2026-04-25-family-science-maastricht",
                "content": (
                    "A city-wide science weekend announces workshops for families in Maastricht with "
                    "confirmed dates, venue details, and ticket windows for the coming week."
                ),
                "score": 0.78,
                "published_at": now_iso,
                "published_at_confidence": 1.0,
            }
        ],
        "bitcoin": [
            {
                "title": "Bitcoin Core release candidate testing continues",
                "url": "https://github.com/bitcoin/bitcoin/issues/33368",
                "content": (
                    "Developers continue release-candidate validation for Bitcoin Core, reporting "
                    "test outcomes, pending fixes, and rollout notes with immediate technical impact."
                ),
                "score": 0.8,
                "published_at": now_iso,
                "published_at_confidence": 1.0,
            }
        ],
        "finance": [
            {
                "title": "Eurozone inflation slows as policy outlook stabilizes",
                "url": "https://example.com/finance/2026-04-23-eurozone-inflation-policy-outlook",
                "content": (
                    "New macro data shows eurozone inflation easing while policymakers signal cautious "
                    "rate decisions, with direct implications for household spending and investment planning."
                ),
                "score": 0.79,
                "published_at": now_iso,
                "published_at_confidence": 1.0,
            }
        ],
    }
    direct = fixtures.get(track_type)
    if direct is not None:
        return direct
    family = infer_track_family(track_type)
    family_rows = fixtures.get(family, [])
    if not family_rows:
        return []
    slug = re.sub(r"[^a-z0-9]+", "-", str(track_type).lower()).strip("-") or family
    scoped: list[dict[str, Any]] = []
    for row in family_rows:
        cloned = dict(row)
        base_url = str(cloned.get("url") or "").rstrip("/")
        if base_url:
            separator = "&" if "?" in base_url else "?"
            cloned["url"] = f"{base_url}{separator}topic={slug}"
        scoped.append(cloned)
    return scoped


def rss_results_for_languages(
    query: str,
    max_results_per_query: int,
    retrieval_languages: list[str],
    include_google: bool,
    include_bing: bool,
) -> tuple[list[dict[str, Any]], list[str]]:
    results: list[dict[str, Any]] = []
    providers: list[str] = []
    errors: list[str] = []

    for language_code in retrieval_languages:
        lang = normalize_language(language_code)
        if include_bing:
            try:
                bing_rows = bing_news_rss_search(query, max_results_per_query, lang)
                if bing_rows:
                    providers.append(f"bing_news_rss:{lang}")
                    results.extend(bing_rows)
            except RuntimeError as exc:
                errors.append(str(exc))
        if include_google:
            try:
                google_rows = google_news_rss_search(query, max_results_per_query, lang)
                if google_rows:
                    providers.append(f"google_news_rss:{lang}")
                    results.extend(google_rows)
            except RuntimeError as exc:
                errors.append(str(exc))

    if not results and errors:
        raise RuntimeError("; ".join(errors[:3]))

    provider_multiplier = int(include_google) + int(include_bing)
    provider_multiplier = provider_multiplier if provider_multiplier > 0 else 1
    cap = max_results_per_query * max(1, len(retrieval_languages)) * provider_multiplier
    return results[:cap], providers


def retrieve_candidates(
    queries: list[dict[str, Any]],
    mode: str,
    max_results_per_query: int,
    language: str,
    db_path: str | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    LOGGER.info("Retrieval started mode=%s topics=%s", mode, [q["track_type"] for q in queries])
    candidates: list[dict[str, Any]] = []
    cache_hits = 0
    live_errors: list[str] = []
    web_errors: list[str] = []
    fallback_chain: list[str] = []
    providers_used: list[str] = []
    effective_mode = mode
    if mode == "auto":
        effective_mode = "live" if os.getenv("TAVILY_API_KEY", "").strip() else "web_fallback"
        fallback_chain.append(f"auto->{effective_mode}")

    for query_item in queries:
        track_type = query_item["track_type"]
        LOGGER.debug("Processing query for track_type=%s", track_type)
        track_family = infer_track_family(track_type)
        query_text = str(query_item.get("query") or "")
        is_site_query = query_text.strip().lower().startswith("site:")
        retrieval_languages_raw = query_item.get("retrieval_languages") or [language]
        retrieval_languages: list[str] = []
        for entry in retrieval_languages_raw:
            normalized = normalize_language(str(entry))
            if normalized not in retrieval_languages:
                retrieval_languages.append(normalized)
        if not retrieval_languages:
            retrieval_languages = [normalize_language(language)]
        provider_used = "fixture"
        if effective_mode == "live":
            try:
                results = tavily_search(query_item["query"], max_results_per_query)
                provider_used = "tavily"
            except RuntimeError as exc:
                live_errors.append(str(exc))
                try:
                    if track_family in {"finance", "bitcoin"}:
                        results, providers_for_query = rss_results_for_languages(
                            query_item["query"],
                            max_results_per_query,
                            retrieval_languages,
                            include_google=False,
                            include_bing=True,
                        )
                        provider_used = "+".join(providers_for_query) if providers_for_query else "bing_news_rss"
                    elif track_family in {"news", "events"} or is_site_query:
                        results, providers_for_query = rss_results_for_languages(
                            query_item["query"],
                            max_results_per_query,
                            retrieval_languages,
                            include_google=True,
                            include_bing=True,
                        )
                        provider_used = "+".join(providers_for_query) if providers_for_query else "bing_news_rss+google_news_rss"
                    else:
                        results, providers_for_query = rss_results_for_languages(
                            query_item["query"],
                            max_results_per_query,
                            retrieval_languages,
                            include_google=True,
                            include_bing=True,
                        )
                        provider_used = "+".join(providers_for_query) if providers_for_query else "bing_news_rss+google_news_rss"
                    fallback_chain.append(f"live->web_fallback:{provider_used}")
                    effective_mode = "web_fallback" if results else effective_mode
                except RuntimeError as web_exc:
                    web_errors.append(str(web_exc))
                    results = fixture_results(track_type)
                    fallback_chain.append("live->fixture_fallback")
                    effective_mode = "fixture_fallback"
                    provider_used = "fixture"
        elif effective_mode == "web_fallback":
            try:
                if track_family in {"finance", "bitcoin"}:
                    results, providers_for_query = rss_results_for_languages(
                        query_item["query"],
                        max_results_per_query,
                        retrieval_languages,
                        include_google=False,
                        include_bing=True,
                    )
                    provider_used = "+".join(providers_for_query) if providers_for_query else "bing_news_rss"
                elif track_family in {"news", "events"} or is_site_query:
                    results, providers_for_query = rss_results_for_languages(
                        query_item["query"],
                        max_results_per_query,
                        retrieval_languages,
                        include_google=True,
                        include_bing=True,
                    )
                    provider_used = "+".join(providers_for_query) if providers_for_query else "bing_news_rss+google_news_rss"
                else:
                    results, providers_for_query = rss_results_for_languages(
                        query_item["query"],
                        max_results_per_query,
                        retrieval_languages,
                        include_google=True,
                        include_bing=True,
                    )
                    provider_used = "+".join(providers_for_query) if providers_for_query else "bing_news_rss+google_news_rss"
            except RuntimeError as web_exc:
                web_errors.append(str(web_exc))
                results = fixture_results(track_type)
                fallback_chain.append("web_fallback->fixture_fallback")
                effective_mode = "fixture_fallback"
                provider_used = "fixture"
        else:
            results = fixture_results(track_type)
            provider_used = "fixture"

        providers_used.append(provider_used)

        for result in results[:max_results_per_query]:
            raw_url = str(result.get("url", "")).strip()
            source_url = str(result.get("source_url", "")).strip()
            if not raw_url:
                continue
            url = resolve_search_result_url(raw_url, source_url)
            cached = db.get_article_by_url(url, db_path=db_path)
            if cached is not None:
                cache_hits += 1
            article_enrichment = enrich_article_from_url(url, track_family=track_family)
            summary_text = clean_summary(str(result.get("content") or result.get("summary") or "").strip())
            if "<script" in summary_text.lower() or "nocollect" in summary_text.lower():
                summary_text = ""
            source = domain_from_url(source_url) if source_url else ""
            if not source or source == "news.google.com":
                source = domain_from_url(str(url or source_url))
            trust_tier = source_trust_tier_for_track(track_type, source)
            published_at = str(result.get("published_at") or (cached or {}).get("published_at") or "").strip()
            published_at_confidence = float(
                result.get("published_at_confidence")
                or (cached or {}).get("published_at_confidence")
                or (0.85 if published_at else 0.0)
            )
            candidate = {
                "item_id": db.article_id_for_url(url),
                "track_type": track_type,
                "track_family": track_family,
                "topic_name": query_item.get("topic_name", track_type),
                "query": query_item["query"],
                "title": str(result.get("title", "")).strip() or url,
                "url": url,
                "raw_url": raw_url,
                "summary": summary_text,
                "score": float(result.get("score") or 0.0),
                "source": source,
                "source_label": str(result.get("source_label") or ""),
                "source_url": source_url,
                "retrieval_provider": provider_used,
                "query_language": str(query_item.get("query_language") or retrieval_languages[0]),
                "retrieval_languages": retrieval_languages,
                "topic_scope_decision": str(query_item.get("topic_scope_decision") or "auto"),
                "topic_scope_source": str(query_item.get("topic_scope_source") or "auto"),
                "topic_locales": list(query_item.get("topic_locales") or []),
                "article_text_excerpt": str(article_enrichment.get("article_text_excerpt") or ""),
                "article_body_markdown": str(article_enrichment.get("article_body_markdown") or ""),
                "published_at": published_at,
                "published_at_confidence": round(published_at_confidence, 3),
                "source_trust_tier": trust_tier,
            }
            if not candidate["article_text_excerpt"]:
                candidate["article_text_excerpt"] = clean_summary(candidate["summary"], max_length=240)
            if candidate["article_text_excerpt"] and candidate["summary"] and len(candidate["summary"]) < 100:
                candidate["summary"] = candidate["article_text_excerpt"]
            candidate["source_type"] = classify_source_type(
                url=candidate["url"],
                title=candidate["title"],
                summary=candidate["summary"],
            )
            candidate["quality_score"] = score_candidate(candidate)
            candidate["selection_reason"] = selection_reason(candidate)
            db.cache_article(
                {
                    "id": candidate["item_id"],
                    "title": candidate["title"],
                    "url": candidate["url"],
                    "category": track_type,
                    "domain": candidate["source"],
                    "published_at": candidate.get("published_at"),
                    "summary": candidate["summary"],
                    "article_text_excerpt": candidate["article_text_excerpt"],
                    "article_body_markdown": candidate["article_body_markdown"],
                    "published_at_confidence": candidate["published_at_confidence"],
                    "source_trust_tier": candidate["source_trust_tier"],
                },
                db_path=db_path,
            )
            candidates.append(candidate)

    trace = {
        "retriever": ",".join(sorted({provider for provider in providers_used if provider})) or "fixture",
        "retrieval_provider": ",".join(sorted({provider for provider in providers_used if provider})) or "local_fixture",
        "providers_used": providers_used,
        "reasoning_active": False,
        "reasoning_model_used": None,
        "fallback_chain": fallback_chain,
        "mode_requested": mode,
        "mode_used": effective_mode,
        "query_count": len(queries),
        "max_results_per_query": max_results_per_query,
        "result_cap_total": len(queries) * max_results_per_query,
        "cache_hits": cache_hits,
        "live_errors": live_errors,
        "web_errors": web_errors,
    }
    return dedupe_candidates(candidates), trace


def dedupe_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for candidate in candidates:
        key = candidate["url"].rstrip("/")
        if key in seen:
            continue
        seen.add(key)
        unique.append(candidate)
    return unique


def domain_from_url(url: str) -> str:
    return urlparse(url).netloc.lower().removeprefix("www.")


def url_path(url: str) -> str:
    return urlparse(url).path.lower()


def url_query(url: str) -> str:
    return urlparse(url).query.lower()


def classify_source_type(url: str, title: str, summary: str) -> str:
    path = url_path(url)
    query = url_query(url)
    combined = f"{title} {summary}".lower()
    if any(token in path for token in ["/tag/", "/category/", "/topics/", "/search", "/archive"]) or any(
        token in query for token in ["s=", "search=", "q=", "page="]
    ):
        return "listing"
    if any(token in combined for token in ["guide", "how to", "travel", "itinerary", "walking tour", "things to do"]):
        return "guide"
    if any(token in combined for token in ["breaking", "report", "investigation", "analysis", "announced", "approved"]):
        return "report"
    if any(token in combined for token in ["live updates", "breaking news", "developing"]):
        return "breaking"
    return "article"


def extract_date(text: str) -> datetime | None:
    match = re.search(r"(20\d{2})[-/](\d{2})[-/](\d{2})", text)
    if match:
        year, month, day = (int(part) for part in match.groups())
        try:
            return datetime(year, month, day, tzinfo=timezone.utc)
        except ValueError:
            return None
    month_match = re.search(
        r"\b(\d{1,2})\s+("
        + "|".join(MONTHS)
        + r")\s+(20\d{2})\b",
        text.lower(),
    )
    if not month_match:
        return None
    day = int(month_match.group(1))
    month = MONTHS[month_match.group(2)]
    year = int(month_match.group(3))
    try:
        return datetime(year, month, day, tzinfo=timezone.utc)
    except ValueError:
        return None


def parse_iso_datetime(value: str | None) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def clean_summary(summary: str, max_length: int = 420) -> str:
    text = re.sub(r"\s+", " ", summary).strip()
    boilerplate_markers = [
        "###### Thank you for donating",
        "## Help us to keep",
        "View Live Map",
        "Check Weather",
    ]
    for marker in boilerplate_markers:
        if marker in text:
            text = text.split(marker, 1)[0].strip()
    leading_noise_markers = [
        "Notizie Video Prezzi Ricerca Consensus",
        "Notizie Notizie Opinioni Eventi Confronto",
        "Investing.com - Il Principale Portale Finanziario",
        "Mercati Indici Italia - indici",
    ]
    trailing_noise_markers = [
        "Informazioni Chi siamo",
        "Privacy Condizioni d'uso",
        "Calendario economico",
        "Accedi Iscriviti gratis",
    ]
    lowered = text.lower()
    for marker in leading_noise_markers:
        idx = lowered.find(marker.lower())
        if idx >= 0 and idx < 120:
            text = text[idx + len(marker) :].strip(" -|:")
            lowered = text.lower()
    for marker in trailing_noise_markers:
        idx = lowered.find(marker.lower())
        if idx >= 0:
            text = text[:idx].rstrip(" -|:")
            lowered = text.lower()
    text = text.removeprefix("# DutchNews.nl - ")
    if len(text) > max_length:
        text = text[: max_length - 3].rstrip() + "..."
    return text


def _strip_html_tags(value: str) -> str:
    without_scripts = re.sub(r"(?is)<script.*?>.*?</script>", " ", value)
    without_styles = re.sub(r"(?is)<style.*?>.*?</style>", " ", without_scripts)
    without_tags = re.sub(r"(?is)<[^>]+>", " ", without_styles)
    return unescape(re.sub(r"\s+", " ", without_tags)).strip()


def looks_like_javascript_payload(text: str) -> bool:
    sample = (text or "").strip().lower()
    if len(sample) < 120:
        return False
    markers = [
        "use strict",
        "closure library",
        "spdx-license",
        "function(",
        "addEventListener".lower(),
        "var window=this",
        "copyright google llc",
        "<script",
        "nocollect",
        "gstatic.com/_/mss/boq-dots",
        "@font-face",
        "fonts.gstatic.com",
        "format('truetype')",
    ]
    hit_count = sum(1 for marker in markers if marker in sample)
    return hit_count >= 2


def looks_like_navigation_text(text: str) -> bool:
    sample = (text or "").strip().lower()
    if len(sample) < 80:
        return False
    markers = [
        "notizie video prezzi ricerca",
        "informazioni chi siamo",
        "privacy condizioni d'uso",
        "accedi iscriviti",
        "accetta i cookie",
        "accept cookies",
        "all rights reserved",
        "tutti i diritti riservati",
        "skip to content",
        "skip to main",
        "subscribe to our newsletter",
    ]
    if any(marker in sample for marker in markers):
        return True
    
    # Generic homepage menu/index detection: high density of specific navigational words
    nav_words = {"home", "about", "contact", "login", "register", "search", "menu", "privacy", "terms", "subscribe", "newsletter"}
    words = sample.split()
    nav_word_count = sum(1 for w in words if w in nav_words)
    if len(words) > 0 and nav_word_count / len(words) > 0.15:
        return True

    # Breadcrumbs or pipe separators common in footers/headers
    if sample.count(" | ") >= 3 or sample.count(" > ") >= 2 or sample.count(" - ") >= 4:
        return True
    
    return False


def _normalize_compare_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()


def is_low_information_excerpt(excerpt: str, title: str, summary: str) -> bool:
    excerpt_norm = _normalize_compare_text(excerpt)
    title_norm = _normalize_compare_text(title)
    summary_norm = _normalize_compare_text(summary)
    if len(excerpt_norm) < 90:
        return True
    if title_norm and excerpt_norm == title_norm:
        return True
    if title_norm and excerpt_norm.startswith(title_norm) and len(excerpt_norm) < 180:
        return True
    if summary_norm and excerpt_norm == summary_norm and len(excerpt_norm) < 120:
        return True
    return False


def enrich_article_from_url(url: str, track_family: str = "news", timeout_seconds: int = 12) -> dict[str, Any]:
    try:
        request = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) PRA-v4/1.0"},
            method="GET",
        )
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            content_type = str(response.headers.get("Content-Type", "")).lower()
            if "text/html" not in content_type and "application/xhtml" not in content_type:
                return {"article_text_excerpt": "", "article_body_markdown": "", "article_fetch_ok": False}
            html = response.read(350_000).decode("utf-8", errors="replace")
    except Exception:
        return {"article_text_excerpt": "", "article_body_markdown": "", "article_fetch_ok": False}

    min_paragraph_len = 28 if track_family == "events" else 40
    fallback_body_len = 90 if track_family == "events" else 120
    min_excerpt_len = 45 if track_family == "events" else 60

    paragraphs = re.findall(r"(?is)<p[^>]*>(.*?)</p>", html)
    clean_paragraphs = [_strip_html_tags(paragraph) for paragraph in paragraphs]
    clean_paragraphs = [
        paragraph
        for paragraph in clean_paragraphs
        if len(paragraph) > min_paragraph_len and not looks_like_navigation_text(paragraph)
    ]
    if not clean_paragraphs:
        body_text = _strip_html_tags(html)
        body_text = clean_summary(body_text, max_length=5000)
        clean_paragraphs = [body_text] if len(body_text) > fallback_body_len and not looks_like_navigation_text(body_text) else []

    body_text = "\n\n".join(clean_paragraphs[:10]).strip()
    excerpt = clean_summary(" ".join(clean_paragraphs[:2]), max_length=420)
    if (
        looks_like_javascript_payload(excerpt)
        or looks_like_javascript_payload(body_text)
        or looks_like_navigation_text(excerpt)
        or looks_like_navigation_text(body_text)
    ):
        return {"article_text_excerpt": "", "article_body_markdown": "", "article_fetch_ok": False}
    if len(excerpt) < min_excerpt_len:
        excerpt = ""
    return {
        "article_text_excerpt": excerpt,
        "article_body_markdown": body_text[:6000],
        "article_fetch_ok": bool(excerpt),
    }


def reject_reason(
    candidate: dict[str, Any],
    now: datetime,
    topic_settings: dict[str, dict[str, Any]] | None = None,
) -> tuple[str | None, str]:
    url = candidate["url"].lower()
    path = url_path(candidate["url"])
    query = url_query(candidate["url"])
    title = candidate["title"].lower()
    summary = candidate.get("summary", "").lower()
    track_type = candidate["track_type"]
    track_family = str(candidate.get("track_family") or infer_track_family(track_type))
    source = candidate.get("source", "")
    source_type = str(candidate.get("source_type") or classify_source_type(candidate.get("url", ""), candidate.get("title", ""), candidate.get("summary", "")))
    def _has_event_signal(text: str) -> bool:
        lowered = text.lower()
        return any(
            token in lowered
            for token in ["event", "events", "festival", "concert", "weekend", "conference", "carnival", "expo", "tickets", "agenda"]
        )

    def _has_future_event_hint(text: str) -> bool:
        lowered = text.lower()
        if str(now.year + 1) in lowered or str(now.year) in lowered:
            return True
        return any(month_name in lowered for month_name in MONTHS.keys())

    if domain_from_url(candidate["url"]) != "news.google.com" and path in {"", "/"}:
        return "not_article_page", "root_homepage_url"
    if source in LOW_VALUE_DOMAINS:
        return "low_value_source", f"domain={source}"
    if any(part in url for part in ["/tag/", "/category/", "/topics/", "/search", "/archive", "/interest/"]) or any(
        key in query for key in ["s=", "search=", "q=", "page="]
    ):
        return "generic_listing", "listing_or_archive_url"
    if track_family in {"news", "finance", "bitcoin"} and source_type == "listing":
        return "generic_listing", "listing_page_for_news_slot"
    if track_family == "news" and source_type == "guide":
        return "low_relevance_news", "guide_or_evergreen_page"
    if track_family == "news" and (
        any(title == word for word in ["news", "latest news", "112"])
        or any(word in title for word in ["headlines today", "local news, events", "breaking news headlines"])
    ):
        return "not_article_page", "static_news_title"
    if track_family == "news" and any(
        word in title + " " + summary
        for word in [
            "tour a piedi",
            "viaggio panoramico",
            "walking tour",
            "driving tour",
            "(4k)",
            "street view",
            "travel vlog",
        ]
    ):
        return "low_relevance_news", "travel_video_or_tour"
    if track_family == "events" and (
        "calendar" in title
        or "event calendar" in title
        or "activities" in title
        or path.rstrip("/").endswith("/events")
        or "/events/?" in url
    ):
        return "generic_listing", "event_listing_page"
    if track_family == "bitcoin" and (
        url.rstrip("/").endswith(("github.com/bitcoin/bitcoin", "bitcoin.org"))
        or path.rstrip("/").endswith("/newsletters")
        or "/zh/" in path
        or "latest bitcoin" in title
        or "latest updates" in title
        or source in {"x.com", "twitter.com"} and "pull request" not in title
    ):
        return "low_value_bitcoin", "root_or_overview_page"
    excerpt = str(candidate.get("article_text_excerpt") or "").strip()
    summary_text = str(candidate.get("summary") or "").strip()
    title_text = str(candidate.get("title") or "").strip()
    event_text = f"{title_text} {summary_text}".strip()
    if looks_like_javascript_payload(excerpt) or looks_like_javascript_payload(str(candidate.get("summary") or "")):
        return "not_article_page", "noisy_extracted_content"
    if looks_like_navigation_text(excerpt) or looks_like_navigation_text(str(candidate.get("summary") or "")):
        return "not_article_page", "navigation_text_extract"
    if is_low_information_excerpt(excerpt, title_text, summary_text):
        if track_family == "events":
            has_event_signal = _has_event_signal(event_text)
            has_timestamp = parse_iso_datetime(str(candidate.get("published_at") or "")) is not None
            if not (has_event_signal and has_timestamp):
                return "not_article_page", "low_information_excerpt"
        else:
            return "not_article_page", "low_information_excerpt"
    if len(excerpt) < 80:
        if track_family == "events":
            has_event_signal = _has_event_signal(event_text)
            has_timestamp = parse_iso_datetime(str(candidate.get("published_at") or "")) is not None
            if not (has_event_signal and has_timestamp and len(title_text) >= 30):
                return "not_article_page", "missing_article_body"
        else:
            return "not_article_page", "missing_article_body"

    date = parse_iso_datetime(str(candidate.get("published_at") or "")) or extract_date(url + " " + title + " " + summary)
    if date is not None:
        age_days = (now - date).days
        if track_family == "news" and age_days > _age_cap_days_for_track("news", topic_settings, track_type):
            return "not_recent", f"age_days={age_days}"
        if track_family == "finance" and age_days > _age_cap_days_for_track("finance", topic_settings, track_type):
            return "not_recent", f"finance_age_days={age_days}"
        if track_family == "bitcoin" and age_days > _age_cap_days_for_track("bitcoin", topic_settings, track_type):
            return "not_recent", f"bitcoin_age_days={age_days}"
        if track_family == "events" and age_days > MAX_EVENT_PAST_DAYS:
            if not (_has_event_signal(event_text) and _has_future_event_hint(event_text)):
                return "not_recent", f"event_age_days={age_days}"
        if track_family == "events" and age_days < -MAX_EVENT_FUTURE_DAYS:
            return "too_far_future", f"event_age_days={age_days}"
        if track_family not in TRACKS and age_days > MAX_NEWS_AGE_DAYS:
            return "not_recent", f"custom_topic_age_days={age_days}"
    elif track_family in {"news", "finance", "bitcoin"}:
        # Softer rejection for Tavily results: try to infer date from URL/title before hard-rejecting.
        # Tavily search_depth=basic often omits published_date even for fresh articles.
        inferred_date = extract_date(url + " " + title + " " + summary)
        provider = str(candidate.get("retrieval_provider") or "")
        if inferred_date is not None:
            # We found a date in the URL/title — re-evaluate freshness with it
            age_days = (now - inferred_date).days
            cap = _age_cap_days_for_track(track_family, topic_settings, track_type)
            if age_days > cap:
                return "not_recent", f"inferred_age_days={age_days}"
            # Date found and fresh enough — allow through
        elif "tavily" in provider and candidate.get("article_text_excerpt"):
            # Tavily result with a good excerpt but no date: treat as potentially fresh.
            # The article passed all content quality checks — don't kill it for missing date.
            pass  # allow through, quality_guard will catch stale patterns in LLM step
        else:
            return "missing_publish_date", "missing_publish_date_signal"
    elif track_family == "events":
        event_signal = any(
            word in url + " " + title + " " + summary
            for word in ["event", "festival", "concert", "weekend", "meetup", "conference", "agenda", "calendar", "2026", "2027"]
        )
        if not event_signal:
            return "missing_specific_date", "event_without_date_signal"
    return None, ""


def validate_candidates(
    candidates: list[dict[str, Any]],
    topic_settings: dict[str, dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    now = datetime.now(timezone.utc)
    valid: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    reason_counts: dict[str, int] = {}
    for candidate in candidates:
        reason, detail = reject_reason(candidate, now, topic_settings=topic_settings)
        if reason:
            rejected_item = {**candidate, "reason": reason, "reason_detail": detail, "stage": "validator"}
            rejected.append(rejected_item)
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
            continue
        valid.append(candidate)
    return valid, rejected, reason_counts


def _subtopic_match_score(item: dict[str, Any], subtopics: list[str]) -> float:
    if not subtopics:
        return 0.0
    haystack = normalize_topic_text(
        " ".join(
            [
                str(item.get("title") or ""),
                str(item.get("summary") or ""),
                str(item.get("query") or ""),
            ]
        )
    )
    if not haystack:
        return 0.0
    hits = 0
    for subtopic in subtopics:
        token = normalize_topic_text(str(subtopic).replace("-", " "))
        if token and token in haystack:
            hits += 1
    return round(min(1.0, hits / max(1, len(subtopics))), 3)


def select_items(
    validated: list[dict[str, Any]],
    topics: list[str],
    per_track: int = 2,
    topic_settings: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    topic_settings = topic_settings or {}
    for track in topics:
        track_key = normalize_topic_text(track)
        track_items = [
            item for item in validated 
            if normalize_topic_text(str(item.get("topic_name") or item.get("track_type") or "")) == track_key
        ]
        subtopics = _to_list_of_strings((topic_settings.get(track_key) or {}).get("subtopics"))
        ranked_items: list[dict[str, Any]] = []
        for item in track_items:
            subtopic_score = _subtopic_match_score(item, subtopics)
            base_score = float(item.get("final_score", item.get("quality_score", item.get("score", 0.0))))
            composite = round(base_score + subtopic_score * 0.2, 4)
            ranked_items.append(
                {
                    **item,
                    "subtopic_match_score": subtopic_score,
                    "final_score": composite,
                    "quality_score": composite,
                }
            )
        selected.extend(sorted(ranked_items, key=lambda item: item.get("final_score", 0), reverse=True)[:per_track])
    return selected


def score_candidate(candidate: dict[str, Any]) -> float:
    score = float(candidate.get("score") or 0.0)
    track_type = candidate["track_type"]
    track_family = str(candidate.get("track_family") or infer_track_family(track_type))
    source = candidate.get("source", "")
    title = candidate["title"].lower()
    url = candidate["url"].lower()
    summary = candidate.get("summary", "").lower()
    source_type = str(candidate.get("source_type") or "article").lower()

    if source in preferred_domains_for_track(track_type):
        score += 0.35
    if source in LOW_VALUE_DOMAINS:
        score -= 0.75
    if any(word in url for word in ["/tag/", "/category/", "/search", "/archive", "/interest/"]):
        score -= 0.45
    if source_type == "listing":
        score -= 0.65
    elif source_type == "guide":
        score -= 0.45
    elif source_type in {"report", "breaking"}:
        score += 0.2
    if track_family == "events" and any(
        word in title + summary for word in ["maastricht", "limburg", "weekend", "april", "2026", "family", "festival", "concert"]
    ):
        score += 0.25
    if track_family == "bitcoin" and any(word in title + summary + url for word in ["issue", "pull request", "optech", "newsletter", "core"]):
        score += 0.3
    if track_family in {"news", "finance"} and any(word in title + summary for word in ["maastricht", "limburg", "netherlands", "dutch"]):
        score += 0.2
    score += min(0.25, max(0.0, float(candidate.get("source_trust_tier", 0)) * 0.05))
    if candidate.get("article_text_excerpt"):
        score += 0.15
    return round(score, 4)


def selection_reason(candidate: dict[str, Any]) -> str:
    source = candidate.get("source", "unknown source")
    track_type = candidate["track_type"]
    if source in preferred_domains_for_track(track_type):
        return f"preferred {track_type} source: {source}"
    if infer_track_family(track_type) == "bitcoin" and "github.com/bitcoin/bitcoin" in candidate["url"]:
        return "Bitcoin Core technical signal"
    return f"matched {track_type} query from {source}"


def selected_counts(items: list[dict[str, Any]], topics: list[str]) -> dict[str, int]:
    return {
        track: sum(1 for item in items if normalize_topic_text(str(item.get("track_type") or "")) == normalize_topic_text(track))
        for track in topics
    }


def _feedback_delta_from_stats(stats: dict[str, int]) -> float:
    likes = int(stats.get("like", 0))
    dislikes = int(stats.get("dislike", 0))
    delta = 0.0
    if likes >= 2 and likes > dislikes:
        delta += min(0.2, 0.05 * (likes - dislikes))
    if dislikes >= 3 and dislikes > likes:
        delta -= min(0.3, 0.08 * (dislikes - likes))
    return delta


def apply_feedback_adjustments(
    validated_items: list[dict[str, Any]],
    feedback_profile: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    topic_stats = feedback_profile.get("topics", {}) if isinstance(feedback_profile, dict) else {}
    domain_stats = feedback_profile.get("domains", {}) if isinstance(feedback_profile, dict) else {}
    adjusted: list[dict[str, Any]] = []
    adjustments_preview: list[dict[str, Any]] = []

    for item in validated_items:
        base_score = float(item.get("quality_score", item.get("score", 0.0)))
        track = str(item.get("track_type") or "").strip().lower()
        source = str(item.get("source") or "").strip().lower()
        feedback_delta = 0.0
        if track and track in topic_stats:
            feedback_delta += _feedback_delta_from_stats(topic_stats[track])
        if source and source in domain_stats:
            feedback_delta += _feedback_delta_from_stats(domain_stats[source])
        feedback_delta = max(-0.45, min(0.35, feedback_delta))
        final_score = round(base_score + feedback_delta, 4)
        adjusted_item = {
            **item,
            "base_score": round(base_score, 4),
            "feedback_delta": round(feedback_delta, 4),
            "final_score": final_score,
            "quality_score": final_score,
        }
        adjusted.append(adjusted_item)
        adjustments_preview.append(
            {
                "item_id": adjusted_item.get("item_id"),
                "track_type": track,
                "source": source,
                "base_score": adjusted_item["base_score"],
                "feedback_delta": adjusted_item["feedback_delta"],
                "final_score": adjusted_item["final_score"],
            }
        )

    trace = {
        "feedback_profile_sample_size": int(feedback_profile.get("sample_size", 0)) if isinstance(feedback_profile, dict) else 0,
        "feedback_totals": feedback_profile.get("totals", {}) if isinstance(feedback_profile, dict) else {},
        "applied_items": len(adjusted),
        "adjustments_preview": adjustments_preview[:20],
    }
    return adjusted, trace


def apply_source_preferences(
    items: list[dict[str, Any]],
    source_preferences: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not source_preferences:
        return items, {"applied_items": 0, "adjustments_preview": [], "source_preferences": 0}
    pref_map = {
        str(row.get("domain") or "").strip().lower(): str(row.get("preference") or "neutral").strip().lower()
        for row in source_preferences
    }
    adjusted: list[dict[str, Any]] = []
    preview: list[dict[str, Any]] = []
    for item in items:
        source = str(item.get("source") or "").strip().lower()
        preference = pref_map.get(source, "neutral")
        delta = 0.0
        if preference == "allow":
            delta = 0.15
        elif preference == "deny":
            delta = -0.35
        base_score = float(item.get("quality_score", item.get("score", 0.0)))
        final_score = round(base_score + delta, 4)
        updated = {
            **item,
            "source_preference": preference,
            "source_pref_delta": round(delta, 4),
            "quality_score": final_score,
            "final_score": round(float(item.get("final_score", base_score)) + delta, 4),
        }
        adjusted.append(updated)
        preview.append(
            {
                "item_id": updated.get("item_id"),
                "source": source,
                "source_preference": preference,
                "source_pref_delta": round(delta, 4),
            }
        )
    return adjusted, {"applied_items": len(adjusted), "adjustments_preview": preview[:20], "source_preferences": len(source_preferences)}


def cap_items_for_processing(items: list[dict[str, Any]], max_items: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if len(items) <= max_items:
        return items, []
    ranked = sorted(items, key=lambda item: item.get("quality_score", item.get("score", 0.0)), reverse=True)
    return ranked[:max_items], ranked[max_items:]


def trim_selected_items(items: list[dict[str, Any]], max_items: int) -> list[dict[str, Any]]:
    if len(items) <= max_items:
        return items
    ranked = sorted(items, key=lambda item: item.get("final_score", item.get("quality_score", 0.0)), reverse=True)
    return ranked[:max_items]


def quality_flags(counts: dict[str, int], mode_used: str, reason_counts: dict[str, int], topics: list[str]) -> list[str]:
    flags: list[str] = []
    if mode_used in {"fixture_fallback", "web_fallback"}:
        flags.append("retrieval_fallback")
    for track in topics:
        if counts.get(track, 0) == 0:
            flags.append(f"missing_{track}")
    if "bitcoin" in topics and reason_counts.get("low_value_bitcoin", 0) > 0:
        flags.append("bitcoin_low_value_rejected")
    if reason_counts.get("not_recent", 0) > 0:
        flags.append("stale_items_rejected")
    return flags


def quality_status(flags: list[str]) -> str:
    return "ok" if not any(flag.startswith("missing_") or flag == "retrieval_fallback" for flag in flags) else "warn"


def md_link(item: dict[str, Any]) -> str:
    return f"[{item['title']}]({item['url']})"


def topic_display_name(topic: str, language: str) -> str:
    names = {
        "it": {"news": "Notizie", "events": "Eventi", "bitcoin": "Bitcoin"},
        "nl": {"news": "Nieuws", "events": "Evenementen", "bitcoin": "Bitcoin"},
        "en": {"news": "News", "events": "Events", "bitcoin": "Bitcoin"},
    }
    return names.get(language, names["en"]).get(topic, topic.title())


def output_labels_for_language(language: str) -> dict[str, str]:
    labels = {
        "it": {
            "report_title": "# Report Personal Research Agent v4",
            "newsletter_title": "# Newsletter Personal Research Agent v4",
            "quality": "Qualità",
            "selected_counts": "Conteggi selezionati",
            "selected": "Selezionati",
            "selected_items": "## Elementi selezionati",
            "none_selected": "Nessun elemento selezionato in questa run.",
            "source": "Fonte",
            "why_selected": "Perché selezionato",
            "summary": "Riepilogo",
            "why_it_matters": "Perché conta",
            "suggested_action": "Azione suggerita",
            "what_happened": "Cosa è successo",
            "context_now": "Contesto attuale",
            "key_facts": "Dati chiave",
            "source_link": "Fonte",
            "query": "Query",
            "no_strong_item": "nessun elemento forte selezionato",
        },
        "nl": {
            "report_title": "# Personal Research Agent v4 Rapport",
            "newsletter_title": "# Personal Research Agent v4 Nieuwsbrief",
            "quality": "Kwaliteit",
            "selected_counts": "Geselecteerde aantallen",
            "selected": "Geselecteerd",
            "selected_items": "## Geselecteerde items",
            "none_selected": "Geen item geselecteerd in deze run.",
            "source": "Bron",
            "why_selected": "Waarom geselecteerd",
            "summary": "Samenvatting",
            "why_it_matters": "Waarom belangrijk",
            "suggested_action": "Voorgestelde actie",
            "what_happened": "Wat is er gebeurd",
            "context_now": "Context nu",
            "key_facts": "Belangrijkste gegevens",
            "source_link": "Bron",
            "query": "Query",
            "no_strong_item": "geen sterk item geselecteerd",
        },
        "en": {
            "report_title": "# Personal Research Agent v4 Report",
            "newsletter_title": "# Personal Research Agent v4 Newsletter",
            "quality": "Quality",
            "selected_counts": "Selected counts",
            "selected": "Selected",
            "selected_items": "## Selected Items",
            "none_selected": "No item selected in this run.",
            "source": "Source",
            "why_selected": "Why selected",
            "summary": "Summary",
            "why_it_matters": "Why it matters",
            "suggested_action": "Suggested action",
            "what_happened": "What happened",
            "context_now": "Context now",
            "key_facts": "Key facts",
            "source_link": "Source",
            "query": "Query",
            "no_strong_item": "no strong item selected",
        },
    }
    return labels.get(language, labels["en"])


def infer_key_fact(item: dict[str, Any]) -> str:
    title = clean_summary(str(item.get("title") or "").strip(), max_length=140)
    summary = clean_summary(str(item.get("summary") or "").strip(), max_length=200)
    excerpt = clean_summary(str(item.get("article_text_excerpt") or "").strip(), max_length=200)
    source = str(item.get("source") or "unknown")
    primary = excerpt or summary
    if looks_like_javascript_payload(primary) or "<script" in primary.lower() or "nocollect" in primary.lower():
        primary = ""
    parts = []
    if title:
        parts.append(title)
    if primary:
        parts.append(primary)
    parts.append(f"source: {source}")
    return " | ".join(parts)


def _newsletter_summary(item: dict[str, Any]) -> str:
    summary = str(item.get("short_summary") or item.get("summary") or "").strip()
    excerpt = str(item.get("article_text_excerpt") or "").strip()
    if excerpt and excerpt.lower() not in summary.lower():
        summary = f"{summary} {excerpt}".strip() if summary else excerpt
    return clean_summary(summary, max_length=620)


def _newsletter_detail(item: dict[str, Any]) -> str:
    detail = str(item.get("article_text_excerpt") or item.get("summary") or "").strip()
    return clean_summary(detail, max_length=320)


def is_generic_context_line(text: str) -> bool:
    value = (text or "").strip().lower()
    if not value:
        return True
    generic_markers = [
        "notizia con potenziale impatto",
        "aggiornamento utile per pianificare",
        "segnale tecnico o di mercato rilevante",
        "possible impact on market",
        "news with potential local or macro impact",
        "useful for local planning and scheduling",
    ]
    return any(marker in value for marker in generic_markers)


def build_outputs(
    user: dict[str, Any],
    enriched_items: list[dict[str, Any]],
    counts: dict[str, int],
    quality: str,
    topics: list[str],
    language: str,
) -> tuple[str, str]:
    labels = output_labels_for_language(language)
    report_lines = [
        labels["report_title"],
        "",
        f"User: {user['name']} | Language: {user['language']} | Quality: {quality}",
        f"{labels['selected_counts']}: " + ", ".join(f"{topic}={counts.get(topic, 0)}" for topic in topics),
        "",
        labels["selected_items"],
    ]
    for track in topics:
        report_lines.extend(["", f"### {topic_display_name(track, language)}"])
        track_items = [
            item
            for item in enriched_items
            if normalize_topic_text(str(item.get("track_type") or "")) == normalize_topic_text(track)
        ]
        if not track_items:
            report_lines.append(f"- {labels['none_selected']}")
        for item in track_items:
            score_value = item.get("final_score", item.get("quality_score", item.get("score", 0)))
            report_lines.append(f"- {md_link(item)}")
            report_lines.append(
                f"  - {labels['source']}: {item.get('source', 'unknown')} | Score: {score_value}"
            )
            report_lines.append(f"  - {labels['why_selected']}: {item.get('selection_reason', 'matched query')}")
            report_lines.append(f"  - {labels['what_happened']}: {item.get('short_summary') or item.get('summary') or 'No summary available.'}")
            report_lines.append(f"  - {labels['context_now']}: {item.get('why_it_matters') or '-'}")
            report_lines.append(f"  - {labels['key_facts']}: {infer_key_fact(item)}")
            action = str(item.get("suggested_action") or "").strip()
            if action:
                report_lines.append(f"  - {labels['suggested_action']}: {action}")
            report_lines.append(f"  - {labels['source_link']}: {item.get('url', '')}")
            report_lines.append(f"  - {labels['query']}: `{item.get('query', '')}`")

    newsletter_lines = [
        labels["newsletter_title"],
        "",
        f"{labels['quality']}: {quality}",
        f"{labels['selected']}: " + ", ".join(f"{topic}={counts.get(topic, 0)}" for topic in topics),
        "",
    ]
    if not enriched_items:
        newsletter_lines.append(f"- {labels['no_strong_item']}")
    else:
        for item in enriched_items[:MAX_ITEMS_TO_OUTPUT]:
            category = str(item.get("track_type") or "news")
            summary = _newsletter_summary(item)
            detail = _newsletter_detail(item)
            newsletter_lines.append(f"- {category}: {md_link(item)}")
            if summary:
                newsletter_lines.append(f"  - {labels['what_happened']}: {summary}")
            if detail and detail.lower() not in summary.lower():
                newsletter_lines.append(f"  - {labels['key_facts']}: {detail}")
            newsletter_lines.append(f"  - {labels['source_link']}: {item.get('url', '')}")
    return "\n".join(report_lines), "\n".join(newsletter_lines)


def retrieve_candidates_tavily_fallback(
    topics_missing: list[str],
    topic_plan: dict[str, list[str]],
    topic_settings: dict[str, dict[str, Any]],
    language: str,
    max_results: int,
    db_path: str | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Targeted Tavily retrieval for topics that produced zero validated candidates.

    Called only if PRA_FLAG_TAVILY_PERTOPIC_FALLBACK is enabled and TAVILY_API_KEY is set.
    Returns a list of candidate dicts (pre-enrichment) and a trace dict.
    """
    if not os.getenv("TAVILY_API_KEY", "").strip():
        return [], {"skipped": "no_tavily_key"}
    candidates: list[dict[str, Any]] = []
    traces: list[dict[str, Any]] = []
    for track_type in topics_missing:
        track_family = infer_track_family(track_type)
        setting = topic_settings.get(normalize_topic_text(track_type)) or {}
        subtopics = _to_list_of_strings(setting.get("subtopics"))
        locales = _to_list_of_strings(setting.get("locales"))
        objective = str(setting.get("objective") or "").split("|")[0].strip()
        # Build a focused query from objective + subtopics + locales
        query_parts: list[str] = []
        if objective and len(objective) >= 5 and objective.lower() != "research":
            query_parts.append(objective[:80])
        elif subtopics:
            query_parts.append(" ".join(subtopics[:3]))
        else:
            query_parts.append(str(track_type))
        if locales:
            query_parts.append(locales[0])
        query_parts.append("latest news")
        query = " ".join(query_parts)[:200]
        try:
            results = tavily_search(query, max_results)
            trace_entry = {"topic": track_type, "query": query, "results": len(results)}
        except RuntimeError as exc:
            traces.append({"topic": track_type, "query": query, "error": str(exc)})
            continue
        traces.append(trace_entry)
        for result in results:
            raw_url = str(result.get("url", "")).strip()
            if not raw_url:
                continue
            url = resolve_search_result_url(raw_url, "")
            cached = db.get_article_by_url(url, db_path=db_path)
            article_enrichment = enrich_article_from_url(url, track_family=track_family)
            summary_text = clean_summary(str(result.get("content") or "").strip())
            if "<script" in summary_text.lower() or "nocollect" in summary_text.lower():
                summary_text = ""
            source = domain_from_url(url)
            trust_tier = source_trust_tier_for_track(track_type, source)
            published_at = str(result.get("published_date") or (cached or {}).get("published_at") or "").strip()
            published_at_confidence = float(0.75 if published_at else 0.0)
            candidate = {
                "item_id": db.article_id_for_url(url),
                "track_type": track_type,
                "track_family": track_family,
                "query": query,
                "title": str(result.get("title", "")).strip() or url,
                "url": url,
                "raw_url": raw_url,
                "summary": summary_text,
                "score": float(result.get("score") or 0.65),
                "source": source,
                "source_label": source,
                "source_url": url,
                "retrieval_provider": "tavily_pertopic_fallback",
                "query_language": normalize_language(language),
                "retrieval_languages": [normalize_language(language)],
                "topic_scope_decision": str(setting.get("geo_scope") or "auto"),
                "topic_scope_source": "tavily_fallback",
                "topic_locales": locales,
                "article_text_excerpt": str(article_enrichment.get("article_text_excerpt") or ""),
                "article_body_markdown": str(article_enrichment.get("article_body_markdown") or ""),
                "published_at": published_at,
                "published_at_confidence": published_at_confidence,
                "source_trust_tier": trust_tier,
            }
            if not candidate["article_text_excerpt"]:
                candidate["article_text_excerpt"] = clean_summary(candidate["summary"], max_length=240)
            if candidate["article_text_excerpt"] and candidate["summary"] and len(candidate["summary"]) < 100:
                candidate["summary"] = candidate["article_text_excerpt"]
            candidate["source_type"] = classify_source_type(
                url=candidate["url"],
                title=candidate["title"],
                summary=candidate["summary"],
            )
            candidate["quality_score"] = score_candidate(candidate)
            candidate["selection_reason"] = selection_reason(candidate)
            db.cache_article(
                {
                    "id": candidate["item_id"],
                    "title": candidate["title"],
                    "url": candidate["url"],
                    "category": track_type,
                    "domain": candidate["source"],
                    "published_at": candidate.get("published_at"),
                    "summary": candidate["summary"],
                    "article_text_excerpt": candidate["article_text_excerpt"],
                    "article_body_markdown": candidate["article_body_markdown"],
                    "published_at_confidence": candidate["published_at_confidence"],
                    "source_trust_tier": candidate["source_trust_tier"],
                },
                db_path=db_path,
            )
            candidates.append(candidate)
    trace = {"tavily_pertopic_fallback": traces, "candidate_count": len(candidates)}
    return dedupe_candidates(candidates), trace


def run_research_digest(
    chat_id: int,
    mode: str = "auto",
    max_results_per_query: int = DEFAULT_MAX_RESULTS_PER_QUERY,
    override_topics: list[str] | None = None,
) -> PipelineResult:
    LOGGER.info("run_research_digest entry chat_id=%s mode=%s override_topics=%s", chat_id, mode, bool(override_topics))
    config = app_config.load_app_config()
    runtime_db_path = config.runtime_db_path
    user = db_users.ensure_user(chat_id=chat_id, db_path=runtime_db_path)
    run_language = normalize_language(str(user.get("language") or config.default_language))
    if override_topics is not None:
        topics_for_run = [normalize_topic_text(topic) for topic in override_topics]
    else:
        topics_for_run = [normalize_topic_text(topic) for topic in normalize_topics_for_run(user.get("topics"))]
    profile = db.get_profile(user_id=int(user["id"]), db_path=runtime_db_path)
    temporary_contexts = (
        db.list_active_temporary_contexts(user_id=int(user["id"]), db_path=runtime_db_path)
        if env_flag("PRA_FLAG_TEMP_CONTEXTS", default=True)
        else []
    )
    location_for_run = active_location_context(profile, temporary_contexts)
    topic_settings, topic_settings_trace = ensure_topic_settings(
        user_id=int(user["id"]),
        topics=topics_for_run,
        profile=profile,
        context_location=location_for_run,
        user_language=run_language,
        db_path=runtime_db_path,
    )
    if topic_settings_trace.get("profile_updated"):
        profile = db.get_profile(user_id=int(user["id"]), db_path=runtime_db_path)
    hard_gate = intake_hard_gate_status(user=user, profile=profile, topic_settings=topic_settings)
    topic_plan = (
        build_topic_plan(
            user_id=int(user["id"]),
            topics=topics_for_run,
            db_path=runtime_db_path,
            topic_settings=topic_settings,
        )
        if env_flag("PRA_FLAG_SUBTOPIC_GRAPH", default=True)
        else {topic: list((topic_settings.get(topic) or {}).get("subtopics") or [])[:3] for topic in topics_for_run}
    )
    run_id = db.log_run(
        user_id=int(user["id"]),
        quality_status="running",
        selected_counts={track: 0 for track in topics_for_run},
        db_path=runtime_db_path,
    )
    debug_dir = project_path("debug") / f"{slug_timestamp()}__v4-{run_id}"
    context = {
        "run_id": f"v4-{run_id}",
        "chat_id": chat_id,
        "thread_id": f"chat-{chat_id}",
        "timestamp": utc_now(),
    }
    db.append_execution_log(
        user_id=int(user["id"]),
        run_id=run_id,
        stage="pipeline_start",
        status="ok",
        message="run_started",
        payload={
            "mode": mode,
            "location": location_for_run,
            "topics": topics_for_run,
            "topic_plan": topic_plan,
            "topic_settings_trace": topic_settings_trace,
            "hard_gate": hard_gate,
        },
        db_path=runtime_db_path,
    )

    try:
        if env_flag("PRA_FLAG_HARD_INTAKE_GATE", default=True) and bool(hard_gate.get("required")):
            raise RuntimeError(
                "INTAKE_REQUIRED: "
                + json.dumps(
                    {
                        "missing_profile_fields": hard_gate.get("missing_profile_fields", []),
                        "insufficient_topics": hard_gate.get("insufficient_topics", []),
                    },
                    sort_keys=True,
                )
            )
        queries, topic_reasoning = build_queries(
            user=user,
            topic_plan=topic_plan,
            context_location=location_for_run,
            topic_settings=topic_settings,
        )
        write_json(
            debug_dir / "01_input.json",
            "input",
            mode,
            context,
            {
                "user": {"id": user["id"], "name": user["name"], "language": user["language"], "topics": user["topics"]},
                "profile": profile or {},
                "temporary_contexts": temporary_contexts,
                "topic_plan": topic_plan,
                "topic_settings": topic_settings,
                "topic_settings_trace": topic_settings_trace,
                "hard_gate": hard_gate,
                "context_location": location_for_run,
                "queries": queries,
                "analyst_reasoning_topics": topic_reasoning.get("topics", {}),
            },
        )
        candidates, retrieval_trace = retrieve_candidates(queries, mode, max_results_per_query, run_language, runtime_db_path)
        retrieval_trace["query_languages"] = sorted(
            {normalize_language(str(query.get("query_language") or run_language)) for query in queries}
        )
        retrieval_trace["topic_scope_decisions"] = {
            topic: str(payload.get("topic_scope_decision") or "auto")
            for topic, payload in dict(topic_reasoning.get("topics") or {}).items()
        }
        relaxed_queries = build_relaxed_queries(queries)
        if not candidates and mode in {"auto", "live", "web_fallback"}:
            if relaxed_queries:
                relaxed_candidates, relaxed_trace = retrieve_candidates(
                    relaxed_queries,
                    mode,
                    max_results_per_query,
                    run_language,
                    runtime_db_path,
                )
                if relaxed_candidates:
                    candidates = relaxed_candidates
                    retrieval_trace = merge_retrieval_trace(
                        retrieval_trace,
                        relaxed_trace,
                        fallback_label="query_relaxation",
                        query_count_initial=len(queries),
                        query_count_relaxed=len(relaxed_queries),
                    )
        validated, rejected, reason_counts = validate_candidates(candidates, topic_settings=topic_settings)
        if not validated and candidates and mode in {"auto", "live", "web_fallback"}:
            already_relaxed = "query_relaxation" in list(retrieval_trace.get("fallback_chain", []))
            if relaxed_queries and not already_relaxed:
                relaxed_candidates, relaxed_trace = retrieve_candidates(
                    relaxed_queries,
                    mode,
                    max_results_per_query,
                    run_language,
                    runtime_db_path,
                )
                if relaxed_candidates:
                    candidates = dedupe_candidates(candidates + relaxed_candidates)
                    retrieval_trace = merge_retrieval_trace(
                        retrieval_trace,
                        relaxed_trace,
                        fallback_label="query_relaxation_post_validation",
                        query_count_initial=len(queries),
                        query_count_relaxed=len(relaxed_queries),
                    )
                    validated, rejected, reason_counts = validate_candidates(candidates, topic_settings=topic_settings)
        # Fix B: Tavily per-topic fallback for topics still empty after RSS retrieval
        if env_flag("PRA_FLAG_TAVILY_PERTOPIC_FALLBACK", default=True) and mode in {"auto", "live", "web_fallback"}:
            counts_after_rss = selected_counts(
                [c for c in validated if c not in rejected],
                topics_for_run,
            )
            topics_with_zero = [t for t in topics_for_run if counts_after_rss.get(t, 0) == 0]
            if topics_with_zero and os.getenv("TAVILY_API_KEY", "").strip():
                tavily_extra, tavily_trace = retrieve_candidates_tavily_fallback(
                    topics_missing=topics_with_zero,
                    topic_plan=topic_plan,
                    topic_settings=topic_settings,
                    language=run_language,
                    max_results=max(2, max_results_per_query),
                    db_path=runtime_db_path,
                )
                if tavily_extra:
                    candidates = dedupe_candidates(candidates + tavily_extra)
                    retrieval_trace.setdefault("fallback_chain", []).append("tavily_pertopic_fallback")
                    retrieval_trace["tavily_pertopic_fallback"] = tavily_trace
                    validated, rejected, reason_counts = validate_candidates(candidates, topic_settings=topic_settings)
        write_json(
            debug_dir / "02_retrieval.json",
            "retrieval",
            retrieval_trace["mode_used"],
            context,
            {
                "candidate_count": len(candidates),
                "trace": retrieval_trace,
                "analyst_reasoning_topics": topic_reasoning.get("topics", {}),
                "candidate_preview": candidates[:10],
            },
        )
        processing_candidates, processing_skipped = cap_items_for_processing(validated, MAX_ITEMS_TO_PROCESS)
        feedback_profile = db.feedback_profile_for_user(user_id=int(user["id"]), db_path=runtime_db_path)
        adjusted_validated, feedback_trace = apply_feedback_adjustments(processing_candidates, feedback_profile)
        source_preferences = db.list_source_preferences(user_id=int(user["id"]), db_path=runtime_db_path)
        adjusted_validated, source_pref_trace = apply_source_preferences(adjusted_validated, source_preferences)
        db.append_workflow_log(
            user_id=int(user["id"]),
            run_id=run_id,
            workflow_name="selection",
            step="feedback_and_source_adjustment",
            status="ok",
            payload={"feedback_trace": feedback_trace, "source_pref_trace": source_pref_trace},
            db_path=runtime_db_path,
        )
        rejection_path = [
            {
                "item_id": item.get("item_id"),
                "track_type": item.get("track_type"),
                "url": item.get("url"),
                "source": item.get("source"),
                "source_type": item.get("source_type"),
                "reason": item.get("reason"),
                "reason_detail": item.get("reason_detail"),
                "topic_scope_decision": item.get("topic_scope_decision"),
            }
            for item in rejected
        ]
        write_json(
            debug_dir / "02_validator.json",
            "validator",
            retrieval_trace["mode_used"],
            context,
            {
                "candidate_count": len(candidates),
                "valid_count": len(validated),
                "valid_count_after_processing_cap": len(processing_candidates),
                "rejected_count": len(rejected),
                "processing_skipped_count": len(processing_skipped),
                "reason_counts": reason_counts,
                "tracks_seen": sorted({item["track_type"] for item in candidates}),
                "validated_preview": adjusted_validated[:10],
                "rejected_preview": rejected[:10],
                "feedback_adjustment": feedback_trace,
                "source_preference_adjustment": source_pref_trace,
                "rejection_path": rejection_path[:80],
            },
        )
        selected = select_items(adjusted_validated, topics_for_run, topic_settings=topic_settings)
        selected = trim_selected_items(selected, MAX_ITEMS_TO_OUTPUT)
        counts = selected_counts(selected, topics_for_run)
        flags = quality_flags(counts, retrieval_trace["mode_used"], reason_counts, topics_for_run)
        quality = quality_status(flags)
        interpretation_config = interpretation_node.InterpretationConfig(
            max_items_to_output=MAX_ITEMS_TO_OUTPUT,
            max_tokens_per_run=MAX_TOKENS_PER_RUN,
            max_llm_items_per_run=MAX_LLM_ITEMS_PER_RUN,
        )
        budget_ctx: dict[str, Any] = {
            "tokens_used_estimate": 0,
            "llm_calls": 0,
            "llm_fallbacks": 0,
            "budget_exceeded": False,
        }
        enriched_items, interpretation_trace = interpretation_node.enrich_items(
            selected_items=selected,
            user_context={"language": run_language},
            budget_ctx=budget_ctx,
            config=interpretation_config,
            db_path=runtime_db_path,
        )
        telegram_compact = interpretation_node.format_for_telegram(
            enriched_items,
            user_language=run_language,
            max_items=MAX_ITEMS_TO_OUTPUT,
        )
        report, newsletter = build_outputs(user, enriched_items, counts, quality, topics_for_run, run_language)
        cost_trace = {
            "max_items_to_process": MAX_ITEMS_TO_PROCESS,
            "max_items_to_output": MAX_ITEMS_TO_OUTPUT,
            "max_tokens_per_run": MAX_TOKENS_PER_RUN,
            "max_llm_items_per_run": MAX_LLM_ITEMS_PER_RUN,
            "processed_items": len(processing_candidates),
            "skipped_items_processing_limit": len(processing_skipped),
            "llm_calls": int(interpretation_trace.get("llm_calls", 0)),
            "tokens_used_estimate": int(interpretation_trace.get("tokens_used_estimate", 0)),
            "cached_hits": int(interpretation_trace.get("cache_hits", 0)),
            "skipped_items": int(interpretation_trace.get("skipped_items", 0)),
            "budget_exceeded": bool(interpretation_trace.get("budget_exceeded", False)),
            "llm_enabled": bool(interpretation_trace.get("llm_enabled", False)),
            "llm_providers_configured": interpretation_trace.get("llm_providers_configured", []),
            "llm_providers_used": interpretation_trace.get("llm_providers_used", []),
            "llm_models_used": interpretation_trace.get("llm_models_used", []),
            "feedback_adjustment": feedback_trace,
            "source_preference_adjustment": source_pref_trace,
        }
        scored_items = [
            {
                "item_id": item.get("item_id"),
                "track_type": item.get("track_type"),
                "source": item.get("source"),
                "source_type": item.get("source_type"),
                "base_score": item.get("base_score"),
                "feedback_delta": item.get("feedback_delta"),
                "final_score": item.get("final_score", item.get("quality_score")),
                "interpretation_mode": item.get("interpretation_mode", "deterministic"),
                "llm_provider": item.get("llm_provider", ""),
                "llm_model": item.get("llm_model", ""),
                "language": item.get("language", run_language),
                "topic_scope_decision": item.get("topic_scope_decision", "auto"),
                "query_language": item.get("query_language"),
            }
            for item in enriched_items
        ]
        selected_reasoning = [
            {
                "item_id": item.get("item_id"),
                "track_type": item.get("track_type"),
                "source": item.get("source"),
                "source_type": item.get("source_type"),
                "base_score": item.get("base_score", item.get("score")),
                "feedback_delta": item.get("feedback_delta", 0.0),
                "final_score": item.get("final_score", item.get("quality_score")),
                "topic_scope_decision": item.get("topic_scope_decision", "auto"),
                "query_language": item.get("query_language"),
                "query": item.get("query"),
                "selection_reason": item.get("selection_reason"),
            }
            for item in selected
        ]
        analyst_reasoning = {
            "topics": topic_reasoning.get("topics", {}),
            "selected_items": selected_reasoning,
            "rejection_path": rejection_path[:120],
        }
        write_json(
            debug_dir / "03_interpretation.json",
            "interpretation",
            retrieval_trace["mode_used"],
            context,
            {
                "enriched_count": len(enriched_items),
                "cost_trace": cost_trace,
                "items": scored_items,
                "analyst_reasoning": analyst_reasoning,
            },
        )
        report_path = debug_dir / "report.md"
        newsletter_path = debug_dir / "newsletter.md"
        report_path.write_text(report, encoding="utf-8")
        newsletter_path.write_text(newsletter, encoding="utf-8")
        write_json(
            debug_dir / "02_output.json",
            "output",
            retrieval_trace["mode_used"],
            context,
            {
                "report_len": len(report),
                "newsletter_len": len(newsletter),
                "selected_counts": counts,
                "quality_gate_status": {"status": quality, "selected": counts, "mode": retrieval_trace["mode_used"], "flags": flags},
                "cost_trace": cost_trace,
                "selected_items_scored": scored_items,
                "analyst_reasoning": analyst_reasoning,
            },
        )
        write_json(
            debug_dir / "final_output.json",
            "final_output",
            retrieval_trace["mode_used"],
            context,
            {
                "final_report": report,
                "final_newsletter": newsletter,
                "trace_payload": {
                    "run_id": f"v4-{run_id}",
                    "selected_counts": counts,
                    "reason_counts": reason_counts,
                    "quality_gate_status": {"status": quality, "selected": counts, "flags": flags},
                    "quality_flags_summary": flags,
                    "retrieval_trace": retrieval_trace,
                    "cost_trace": cost_trace,
                    "selected_items_scored": scored_items,
                    "analyst_reasoning": analyst_reasoning,
                    "personalization_source": {
                        "profile_version": int((profile or {}).get("profile_version", 0) or 0),
                        "temporary_contexts_applied": len(temporary_contexts),
                        "topic_plan": topic_plan,
                        "topic_settings": topic_settings,
                        "location": location_for_run,
                    },
                },
            },
        )
        personalization_payload = {
            "profile_version": int((profile or {}).get("profile_version", 0) or 0),
            "temporary_contexts": temporary_contexts,
            "topic_plan": topic_plan,
            "topic_settings": topic_settings,
            "location": location_for_run,
            "source_preferences": source_preferences,
        }
        if env_flag("DEBUG", default=False):
            write_debug_brief(
                debug_dir=debug_dir,
                retrieval_trace=retrieval_trace,
                cost_trace=cost_trace,
                personalization=personalization_payload,
            )
        write_kb_log(
            run_id=run_id,
            payload={
                "run_id": f"v4-{run_id}",
                "created_at": utc_now(),
                "quality_status": quality,
                "mode": retrieval_trace["mode_used"],
                "selected_counts": counts,
                "quality_flags": flags,
                "retrieval_trace": retrieval_trace,
                "cost_trace": cost_trace,
                "analyst_reasoning": analyst_reasoning,
                "personalization": personalization_payload,
            },
        )
        db.append_execution_log(
            user_id=int(user["id"]),
            run_id=run_id,
            stage="pipeline_complete",
            status="ok",
            message="run_completed",
            payload={"quality": quality, "selected_counts": counts},
            db_path=runtime_db_path,
        )
        db.append_profile_event(
            user_id=int(user["id"]),
            event_type="run_completed",
            payload={"run_id": run_id, "quality_status": quality, "selected_counts": counts},
            db_path=runtime_db_path,
        )
        db.update_run_summary(
            run_id=run_id,
            report_path=str(report_path.relative_to(app_config.PROJECT_ROOT)),
            newsletter_path=str(newsletter_path.relative_to(app_config.PROJECT_ROOT)),
            quality_status=quality,
            selected_counts=counts,
            db_path=runtime_db_path,
        )
        return PipelineResult(
            run_id=run_id,
            report=report,
            newsletter=newsletter,
            report_path=str(report_path),
            newsletter_path=str(newsletter_path),
            debug_dir=str(debug_dir),
            quality_status=quality,
            selected_counts=counts,
            mode=retrieval_trace["mode_used"],
            language=run_language,
            quality_flags=flags,
            enriched_items=enriched_items,
            telegram_compact=telegram_compact,
            cost_trace=cost_trace,
        )
    except Exception as exc:
        write_json(debug_dir / "error.json", "error", mode, context, {"error": str(exc)})
        db.append_execution_log(
            user_id=int(user["id"]),
            run_id=run_id,
            stage="pipeline_error",
            status="error",
            message=str(exc),
            payload={"mode": mode},
            db_path=runtime_db_path,
        )
        db.update_run_summary(
            run_id=run_id,
            quality_status="error",
            selected_counts={track: 0 for track in topics_for_run},
            db_path=runtime_db_path,
        )
        raise


def format_console_summary(result: PipelineResult) -> str:
    counts = ", ".join(f"{track}={count}" for track, count in result.selected_counts.items())
    if result.language == "it":
        return (
            "Digest Personal Research Agent v4 completato "
            f"(run_id={result.run_id}, quality={result.quality_status}, mode={result.mode}). "
            f"Selezionati: {counts}. "
            f"Report: {result.report_path}. Newsletter: {result.newsletter_path}. Debug: {result.debug_dir}"
        )
    if result.language == "nl":
        return (
            "Personal Research Agent v4 digest voltooid "
            f"(run_id={result.run_id}, quality={result.quality_status}, mode={result.mode}). "
            f"Geselecteerd: {counts}. "
            f"Rapport: {result.report_path}. Nieuwsbrief: {result.newsletter_path}. Debug: {result.debug_dir}"
        )
    return (
        "Personal Research Agent v4 digest complete "
        f"(run_id={result.run_id}, quality={result.quality_status}, mode={result.mode}). "
        f"Selected: {counts}. "
        f"Report: {result.report_path}. Newsletter: {result.newsletter_path}. Debug: {result.debug_dir}"
    )
