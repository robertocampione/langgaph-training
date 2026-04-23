"""Console research pipeline for Personal Research Agent v4."""

from __future__ import annotations

import json
import os
import re
import base64
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote_plus, urlencode, urlparse

from app import config as app_config
from app import db
from app import db_users
from app.nodes import interpretation as interpretation_node


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


def normalize_language(language: str | None) -> str:
    value = (language or "").strip().lower()
    if value in SUPPORTED_LANGUAGES:
        return value
    return "en"


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


def normalize_topics_for_run(topics: list[str] | tuple[str, ...] | None) -> list[str]:
    if not topics:
        return list(app_config.DEFAULT_TOPICS)
    normalized: list[str] = []
    seen: set[str] = set()
    for topic in topics:
        value = str(topic).strip().lower()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized or list(app_config.DEFAULT_TOPICS)


def ensure_default_topic_graph(user_id: int, topics: list[str], db_path: str | None) -> None:
    for topic in topics:
        subtopics = DEFAULT_SUBTOPIC_PACKS.get(topic, {})
        for subtopic, weight in subtopics.items():
            db.set_topic_weight(
                user_id=user_id,
                topic=topic,
                subtopic=subtopic,
                weight=weight,
                enabled=True,
                source="default",
                db_path=db_path,
            )


def build_topic_plan(user_id: int, topics: list[str], db_path: str | None) -> dict[str, list[str]]:
    ensure_default_topic_graph(user_id=user_id, topics=topics, db_path=db_path)
    plan: dict[str, list[str]] = {}
    for topic in topics:
        rows = db.list_topic_weights(user_id=user_id, topic=topic, db_path=db_path)
        enabled_rows = [row for row in rows if bool(row.get("enabled", True))]
        enabled_rows.sort(key=lambda row: float(row.get("weight", 0.0)), reverse=True)
        plan[topic] = [str(row.get("subtopic") or "").strip() for row in enabled_rows[:3] if str(row.get("subtopic") or "").strip()]
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
    max_topics: int | None = None,
) -> list[dict[str, str]]:
    topics = normalize_topics_for_run(user.get("topics"))
    language = normalize_language(str(user.get("language") or app_config.DEFAULT_LANGUAGE))
    if max_topics is not None:
        topics = topics[:max_topics]

    current_year = datetime.now(timezone.utc).year
    query_templates_by_language = {
        "it": {
            "news": [
                "site:nltimes.nl Maastricht Limburg latest news",
                "site:dutchnews.nl Maastricht Limburg latest news",
                "Netherlands Limburg Maastricht breaking news",
            ],
            "events": [
                "site:visitmaastricht.com Maastricht events 2026",
                "site:mecc.nl Maastricht events 2026",
                "Maastricht events this weekend date location",
            ],
            "bitcoin": [
                "Bitcoin notizie oggi prezzo ETF regolamentazione",
                "Bitcoin Core aggiornamenti tecnici BIP Lightning mining",
                "site:bitcoinops.org/en/newsletters/ Bitcoin Optech 2026",
            ],
        },
        "nl": {
            "news": [
                "site:nltimes.nl Maastricht Limburg latest news",
                "site:dutchnews.nl Maastricht Limburg latest news",
                "Nederland Limburg Maastricht laatste nieuws",
            ],
            "events": [
                "site:visitmaastricht.com Maastricht events 2026",
                "site:mecc.nl Maastricht events 2026",
                "Maastricht evenementen dit weekend datum locatie",
            ],
            "bitcoin": [
                "Bitcoin nieuws vandaag prijs ETF regelgeving",
                "Bitcoin Core technische update BIP Lightning mining",
                "site:bitcoinops.org/en/newsletters/ Bitcoin Optech 2026",
            ],
        },
        "en": {
            "news": [
                "site:nltimes.nl Maastricht Limburg latest news",
                "site:dutchnews.nl Netherlands Limburg Maastricht latest news",
                "Netherlands Limburg Maastricht latest news",
            ],
            "events": [
                "site:visitmaastricht.com Maastricht events this weekend",
                "site:mecc.nl Maastricht events 2026",
                "Maastricht events this weekend date location",
            ],
            "bitcoin": [
                "Bitcoin latest news today market ETF policy",
                "Bitcoin Core technical updates BIP Lightning mining",
                "site:bitcoinops.org/en/newsletters/ Bitcoin Optech 2026",
            ],
        },
    }
    custom_topic_templates = {
        "it": [
            "{topic} ultime notizie oggi",
            "{topic} aggiornamenti ultime 24 ore",
            "{topic} sviluppi principali settimana corrente",
        ],
        "nl": [
            "{topic} laatste nieuws vandaag",
            "{topic} updates laatste 24 uur",
            "{topic} belangrijkste ontwikkelingen deze week",
        ],
        "en": [
            "{topic} latest news today",
            "{topic} updates in the last 24 hours",
            "{topic} key developments this week",
        ],
    }
    query_templates = query_templates_by_language[language]
    custom_templates = custom_topic_templates[language]
    queries: list[dict[str, str]] = []
    for topic in topics:
        location_hint = f" {context_location}" if topic in {"news", "events"} and context_location else ""
        selected_subtopics = topic_plan.get(topic, [])
        topic_queries = query_templates.get(topic)
        if topic_queries is None:
            topic_label = topic.replace("-", " ").replace("_", " ")
            topic_queries = [template.format(topic=f"{topic_label}{location_hint}", year=current_year) for template in custom_templates]
        for query in topic_queries:
            enriched_query = f"{query}{location_hint}".strip()
            is_site_query = query.strip().lower().startswith("site:")
            if selected_subtopics and topic == "bitcoin":
                enriched_query = f"{enriched_query} {' '.join(selected_subtopics[:2])}".strip()
            elif selected_subtopics and topic in {"news", "events"} and not is_site_query:
                enriched_query = f"{enriched_query} {selected_subtopics[0]}".strip()
            queries.append({"track_type": topic, "query": enriched_query, "subtopics": selected_subtopics})
    return queries


def tavily_search(query: str, max_results: int) -> list[dict[str, Any]]:
    token = os.getenv("TAVILY_API_KEY", "").strip()
    if not token:
        raise RuntimeError("TAVILY_API_KEY is required for live retrieval")
    payload = json.dumps(
        {
            "api_key": token,
            "query": query,
            "search_depth": "basic",
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
    url = (
        f"{GOOGLE_NEWS_RSS_ENDPOINT}?q={quote_plus(query)}"
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
    fixtures = {
        "news": [
            {
                "title": "Limburg mobility plan gets fresh funding",
                "url": "https://example.com/2026/04/22/limburg-mobility-plan",
                "content": "A recent Limburg mobility update with local impact around Maastricht.",
                "score": 0.82,
            }
        ],
        "events": [
            {
                "title": "Family science weekend in Maastricht",
                "url": "https://example.com/events/2026-04-25-family-science-maastricht",
                "content": "A near-term family friendly event in Maastricht with explicit date and location.",
                "score": 0.78,
            }
        ],
        "bitcoin": [
            {
                "title": "Bitcoin Core release candidate testing continues",
                "url": "https://github.com/bitcoin/bitcoin/issues/33368",
                "content": "Recent Bitcoin Core testing discussion with practical technical relevance.",
                "score": 0.8,
            }
        ],
    }
    return fixtures.get(track_type, [])


def retrieve_candidates(
    queries: list[dict[str, str]],
    mode: str,
    max_results_per_query: int,
    language: str,
    db_path: str | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
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
        query_text = str(query_item.get("query") or "")
        is_site_query = query_text.strip().lower().startswith("site:")
        provider_used = "fixture"
        if effective_mode == "live":
            try:
                results = tavily_search(query_item["query"], max_results_per_query)
                provider_used = "tavily"
            except RuntimeError as exc:
                live_errors.append(str(exc))
                try:
                    if track_type in {"news", "events"} or is_site_query:
                        results = google_news_rss_search(query_item["query"], max_results_per_query, language)
                        provider_used = "google_news_rss"
                        if not results:
                            results = bing_news_rss_search(query_item["query"], max_results_per_query, language)
                            provider_used = "bing_news_rss"
                    else:
                        results = bing_news_rss_search(query_item["query"], max_results_per_query, language)
                        provider_used = "bing_news_rss"
                        if not results:
                            results = google_news_rss_search(query_item["query"], max_results_per_query, language)
                            provider_used = "google_news_rss"
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
                if track_type in {"news", "events"} or is_site_query:
                    results = google_news_rss_search(query_item["query"], max_results_per_query, language)
                    provider_used = "google_news_rss"
                    if not results:
                        results = bing_news_rss_search(query_item["query"], max_results_per_query, language)
                        provider_used = "bing_news_rss"
                else:
                    results = bing_news_rss_search(query_item["query"], max_results_per_query, language)
                    provider_used = "bing_news_rss"
                    if not results:
                        results = google_news_rss_search(query_item["query"], max_results_per_query, language)
                        provider_used = "google_news_rss"
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
            article_enrichment = enrich_article_from_url(url)
            summary_text = clean_summary(str(result.get("content") or result.get("summary") or "").strip())
            if "<script" in summary_text.lower() or "nocollect" in summary_text.lower():
                summary_text = ""
            source = domain_from_url(source_url) if source_url else ""
            if not source or source == "news.google.com":
                source = domain_from_url(str(url or source_url))
            trust_tier = int(SOURCE_TRUST_TIERS.get(track_type, {}).get(source, 1))
            candidate = {
                "item_id": db.article_id_for_url(url),
                "track_type": track_type,
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
                "article_text_excerpt": str(article_enrichment.get("article_text_excerpt") or ""),
                "article_body_markdown": str(article_enrichment.get("article_body_markdown") or ""),
                "published_at_confidence": 0.35,
                "source_trust_tier": trust_tier,
            }
            if not candidate["article_text_excerpt"]:
                candidate["article_text_excerpt"] = clean_summary(candidate["summary"], max_length=240)
            if candidate["article_text_excerpt"] and candidate["summary"] and len(candidate["summary"]) < 100:
                candidate["summary"] = candidate["article_text_excerpt"]
            candidate["quality_score"] = score_candidate(candidate)
            candidate["selection_reason"] = selection_reason(candidate)
            db.cache_article(
                {
                    "id": candidate["item_id"],
                    "title": candidate["title"],
                    "url": candidate["url"],
                    "category": track_type,
                    "domain": candidate["source"],
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
    ]
    hit_count = sum(1 for marker in markers if marker in sample)
    return hit_count >= 2


def looks_like_navigation_text(text: str) -> bool:
    sample = (text or "").strip().lower()
    if len(sample) < 80:
        return False
    markers = [
        "notizie video prezzi ricerca consensus",
        "informazioni chi siamo",
        "privacy condizioni d'uso",
        "migliori criptovalute",
        "calendario economico",
        "mercati indici",
        "accedi iscriviti gratis",
        "fondi mondiali",
    ]
    if any(marker in sample for marker in markers):
        return True
    token_count = len(sample.split())
    if token_count > 70 and sample.count("|") >= 4:
        return True
    return False


def enrich_article_from_url(url: str, timeout_seconds: int = 12) -> dict[str, Any]:
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

    paragraphs = re.findall(r"(?is)<p[^>]*>(.*?)</p>", html)
    clean_paragraphs = [_strip_html_tags(paragraph) for paragraph in paragraphs]
    clean_paragraphs = [
        paragraph
        for paragraph in clean_paragraphs
        if len(paragraph) > 40 and not looks_like_navigation_text(paragraph)
    ]
    if not clean_paragraphs:
        body_text = _strip_html_tags(html)
        body_text = clean_summary(body_text, max_length=5000)
        clean_paragraphs = [body_text] if len(body_text) > 120 and not looks_like_navigation_text(body_text) else []

    body_text = "\n\n".join(clean_paragraphs[:10]).strip()
    excerpt = clean_summary(" ".join(clean_paragraphs[:2]), max_length=420)
    if (
        looks_like_javascript_payload(excerpt)
        or looks_like_javascript_payload(body_text)
        or looks_like_navigation_text(excerpt)
        or looks_like_navigation_text(body_text)
    ):
        return {"article_text_excerpt": "", "article_body_markdown": "", "article_fetch_ok": False}
    if len(excerpt) < 60:
        excerpt = ""
    return {
        "article_text_excerpt": excerpt,
        "article_body_markdown": body_text[:6000],
        "article_fetch_ok": bool(excerpt),
    }


def reject_reason(candidate: dict[str, Any], now: datetime) -> tuple[str | None, str]:
    url = candidate["url"].lower()
    path = url_path(candidate["url"])
    query = url_query(candidate["url"])
    title = candidate["title"].lower()
    summary = candidate.get("summary", "").lower()
    track_type = candidate["track_type"]
    source = candidate.get("source", "")
    if domain_from_url(candidate["url"]) != "news.google.com" and path in {"", "/"}:
        return "not_article_page", "root_homepage_url"
    if source in LOW_VALUE_DOMAINS:
        return "low_value_source", f"domain={source}"
    if any(part in url for part in ["/tag/", "/category/", "/topics/", "/search", "/archive", "/interest/"]) or any(
        key in query for key in ["s=", "search=", "q=", "page="]
    ):
        return "generic_listing", "listing_or_archive_url"
    if track_type == "news" and (
        any(title == word for word in ["news", "latest news", "112"])
        or any(word in title for word in ["headlines today", "local news, events", "breaking news headlines"])
    ):
        return "not_article_page", "static_news_title"
    if track_type == "news" and any(
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
    if track_type == "events" and (
        "calendar" in title
        or "event calendar" in title
        or "activities" in title
        or path.rstrip("/").endswith("/events")
        or "/events/?" in url
    ):
        return "generic_listing", "event_listing_page"
    if track_type == "bitcoin" and (
        url.rstrip("/").endswith(("github.com/bitcoin/bitcoin", "bitcoin.org"))
        or path.rstrip("/").endswith("/newsletters")
        or "/zh/" in path
        or "latest bitcoin" in title
        or "latest updates" in title
        or source in {"x.com", "twitter.com"} and "pull request" not in title
    ):
        return "low_value_bitcoin", "root_or_overview_page"
    excerpt = str(candidate.get("article_text_excerpt") or "").strip()
    if looks_like_javascript_payload(excerpt) or looks_like_javascript_payload(str(candidate.get("summary") or "")):
        return "not_article_page", "noisy_extracted_content"
    if looks_like_navigation_text(excerpt) or looks_like_navigation_text(str(candidate.get("summary") or "")):
        return "not_article_page", "navigation_text_extract"
    if len(excerpt) < 80:
        return "not_article_page", "missing_article_body"

    date = extract_date(url + " " + title + " " + summary)
    if date is not None:
        age_days = (now - date).days
        if track_type == "news" and age_days > 14:
            return "not_recent", f"age_days={age_days}"
        if track_type == "events" and age_days > 1:
            return "not_recent", f"event_age_days={age_days}"
        if track_type == "events" and age_days < -45:
            return "too_far_future", f"event_age_days={age_days}"
        if track_type not in TRACKS and age_days > 21:
            return "not_recent", f"custom_topic_age_days={age_days}"
    elif track_type == "events" and not any(word in url + title for word in ["event", "weekend", "maastricht"]):
        return "missing_specific_date", "event_without_date_signal"
    return None, ""


def validate_candidates(candidates: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    now = datetime.now(timezone.utc)
    valid: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    reason_counts: dict[str, int] = {}
    for candidate in candidates:
        reason, detail = reject_reason(candidate, now)
        if reason:
            rejected_item = {**candidate, "reason": reason, "reason_detail": detail, "stage": "validator"}
            rejected.append(rejected_item)
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
            continue
        valid.append(candidate)
    return valid, rejected, reason_counts


def select_items(validated: list[dict[str, Any]], topics: list[str], per_track: int = 2) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for track in topics:
        track_items = [item for item in validated if item["track_type"] == track]
        selected.extend(sorted(track_items, key=lambda item: item.get("quality_score", 0), reverse=True)[:per_track])
    return selected


def score_candidate(candidate: dict[str, Any]) -> float:
    score = float(candidate.get("score") or 0.0)
    track_type = candidate["track_type"]
    source = candidate.get("source", "")
    title = candidate["title"].lower()
    url = candidate["url"].lower()
    summary = candidate.get("summary", "").lower()

    if source in PREFERRED_DOMAINS.get(track_type, set()):
        score += 0.35
    if source in LOW_VALUE_DOMAINS:
        score -= 0.75
    if any(word in url for word in ["/tag/", "/category/", "/search", "/archive", "/interest/"]):
        score -= 0.45
    if track_type == "events" and any(word in title + summary for word in ["maastricht", "limburg", "weekend", "april", "2026"]):
        score += 0.25
    if track_type == "bitcoin" and any(word in title + summary + url for word in ["issue", "pull request", "optech", "newsletter", "core"]):
        score += 0.3
    if track_type == "news" and any(word in title + summary for word in ["maastricht", "limburg", "netherlands", "dutch"]):
        score += 0.2
    score += min(0.25, max(0.0, float(candidate.get("source_trust_tier", 0)) * 0.05))
    if candidate.get("article_text_excerpt"):
        score += 0.15
    return round(score, 4)


def selection_reason(candidate: dict[str, Any]) -> str:
    source = candidate.get("source", "unknown source")
    track_type = candidate["track_type"]
    if source in PREFERRED_DOMAINS.get(track_type, set()):
        return f"preferred {track_type} source: {source}"
    if track_type == "bitcoin" and "github.com/bitcoin/bitcoin" in candidate["url"]:
        return "Bitcoin Core technical signal"
    return f"matched {track_type} query from {source}"


def selected_counts(items: list[dict[str, Any]], topics: list[str]) -> dict[str, int]:
    return {track: sum(1 for item in items if item["track_type"] == track) for track in topics}


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
        track_items = [item for item in enriched_items if item["track_type"] == track]
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
            summary = str(item.get("short_summary") or item.get("summary") or "").strip()
            if len(summary) > 420:
                summary = summary[:417].rstrip() + "..."
            why = str(item.get("why_it_matters") or "").strip()
            if len(why) > 260:
                why = why[:257].rstrip() + "..."
            newsletter_lines.append(f"- {category}: {md_link(item)}")
            if summary:
                newsletter_lines.append(f"  - {labels['what_happened']}: {summary}")
            if why and not is_generic_context_line(why):
                newsletter_lines.append(f"  - {labels['context_now']}: {why}")
            newsletter_lines.append(f"  - {labels['key_facts']}: {infer_key_fact(item)}")
            action = str(item.get("suggested_action") or "").strip()
            if action:
                newsletter_lines.append(f"  - {labels['suggested_action']}: {action}")
            newsletter_lines.append(f"  - {labels['source_link']}: {item.get('url', '')}")
    return "\n".join(report_lines), "\n".join(newsletter_lines)


def run_research_digest(
    chat_id: int,
    mode: str = "auto",
    max_results_per_query: int = DEFAULT_MAX_RESULTS_PER_QUERY,
) -> PipelineResult:
    config = app_config.load_app_config()
    runtime_db_path = config.runtime_db_path
    user = db_users.ensure_user(chat_id=chat_id, db_path=runtime_db_path)
    run_language = normalize_language(str(user.get("language") or config.default_language))
    topics_for_run = normalize_topics_for_run(user.get("topics"))
    profile = db.get_profile(user_id=int(user["id"]), db_path=runtime_db_path)
    temporary_contexts = (
        db.list_active_temporary_contexts(user_id=int(user["id"]), db_path=runtime_db_path)
        if env_flag("PRA_FLAG_TEMP_CONTEXTS", default=True)
        else []
    )
    topic_plan = (
        build_topic_plan(user_id=int(user["id"]), topics=topics_for_run, db_path=runtime_db_path)
        if env_flag("PRA_FLAG_SUBTOPIC_GRAPH", default=True)
        else {topic: [] for topic in topics_for_run}
    )
    location_for_run = active_location_context(profile, temporary_contexts)
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
        payload={"mode": mode, "location": location_for_run, "topics": topics_for_run, "topic_plan": topic_plan},
        db_path=runtime_db_path,
    )

    try:
        queries = build_queries(user=user, topic_plan=topic_plan, context_location=location_for_run)
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
                "context_location": location_for_run,
                "queries": queries,
            },
        )
        candidates, retrieval_trace = retrieve_candidates(queries, mode, max_results_per_query, run_language, runtime_db_path)
        write_json(
            debug_dir / "02_retrieval.json",
            "retrieval",
            retrieval_trace["mode_used"],
            context,
            {"candidate_count": len(candidates), "trace": retrieval_trace, "candidate_preview": candidates[:10]},
        )
        validated, rejected, reason_counts = validate_candidates(candidates)
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
            },
        )
        selected = select_items(adjusted_validated, topics_for_run)
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
                "base_score": item.get("base_score"),
                "feedback_delta": item.get("feedback_delta"),
                "final_score": item.get("final_score", item.get("quality_score")),
                "interpretation_mode": item.get("interpretation_mode", "deterministic"),
                "llm_provider": item.get("llm_provider", ""),
                "llm_model": item.get("llm_model", ""),
                "language": item.get("language", run_language),
            }
            for item in enriched_items
        ]
        write_json(
            debug_dir / "03_interpretation.json",
            "interpretation",
            retrieval_trace["mode_used"],
            context,
            {
                "enriched_count": len(enriched_items),
                "cost_trace": cost_trace,
                "items": scored_items,
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
                    "personalization_source": {
                        "profile_version": int((profile or {}).get("profile_version", 0) or 0),
                        "temporary_contexts_applied": len(temporary_contexts),
                        "topic_plan": topic_plan,
                        "location": location_for_run,
                    },
                },
            },
        )
        personalization_payload = {
            "profile_version": int((profile or {}).get("profile_version", 0) or 0),
            "temporary_contexts": temporary_contexts,
            "topic_plan": topic_plan,
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
