"""Console research pipeline for Personal Research Agent v3."""

from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus, urlparse

from app import config as app_config
from app import db
from app import db_users


TRACKS = ("news", "events", "bitcoin")
DEFAULT_MAX_RESULTS_PER_QUERY = 2
TAVILY_ENDPOINT = "https://api.tavily.com/search"
GOOGLE_NEWS_RSS_ENDPOINT = "https://news.google.com/rss/search"
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


def build_queries(user: dict[str, Any], max_topics: int | None = None) -> list[dict[str, str]]:
    topics = normalize_topics_for_run(user.get("topics"))
    language = normalize_language(str(user.get("language") or app_config.DEFAULT_LANGUAGE))
    if max_topics is not None:
        topics = topics[:max_topics]

    current_year = datetime.now(timezone.utc).year
    query_templates_by_language = {
        "it": {
            "news": [
                "site:1limburg.nl Maastricht Limburg notizie oggi",
                "site:dutchnews.nl Paesi Bassi Limburg Maastricht notizie aggiornate",
                "site:nltimes.nl Olanda Limburg Maastricht ultime notizie",
            ],
            "events": [
                "site:visitmaastricht.com eventi Maastricht questo weekend",
                "site:visitzuidlimburg.com evento Maastricht weekend famiglie",
                "site:maastrichtbereikbaar.nl eventi Maastricht weekend",
            ],
            "bitcoin": [
                "site:bitcoinops.org/en/newsletters Bitcoin Optech newsletter",
                "site:github.com/bitcoin/bitcoin/issues Bitcoin Core issue recenti",
                "site:coindesk.com Bitcoin aggiornamento mercato",
            ],
        },
        "nl": {
            "news": [
                "site:1limburg.nl Maastricht Limburg nieuws vandaag",
                "site:dutchnews.nl Nederland Limburg Maastricht laatste nieuws",
                "site:nltimes.nl Nederland Limburg Maastricht nieuws",
            ],
            "events": [
                "site:visitmaastricht.com Maastricht evenementen dit weekend",
                "site:visitzuidlimburg.com Maastricht evenement familie weekend",
                "site:maastrichtbereikbaar.nl Maastricht evenementen weekend",
            ],
            "bitcoin": [
                "site:bitcoinops.org/en/newsletters Bitcoin Optech nieuwsbrief",
                "site:github.com/bitcoin/bitcoin/issues Bitcoin Core recente issues",
                "site:coindesk.com Bitcoin markt update",
            ],
        },
        "en": {
            "news": [
                "site:1limburg.nl Maastricht Limburg today local news",
                "site:dutchnews.nl Netherlands Limburg Maastricht latest news",
                "site:nltimes.nl Netherlands Limburg Maastricht latest news",
            ],
            "events": [
                "site:visitmaastricht.com Maastricht events this weekend",
                "site:visitzuidlimburg.com Maastricht event family weekend",
                "site:maastrichtbereikbaar.nl Maastricht events weekend",
            ],
            "bitcoin": [
                "site:bitcoinops.org/en/newsletters Bitcoin Optech newsletter",
                "site:github.com/bitcoin/bitcoin/issues Bitcoin Core recent issues",
                "site:coindesk.com Bitcoin market technical update",
            ],
        },
    }
    custom_topic_templates = {
        "it": [
            "{topic} ultime notizie {year}",
            "{topic} aggiornamenti ufficiali {year}",
            "{topic} analisi e sviluppi principali {year}",
        ],
        "nl": [
            "{topic} laatste updates {year}",
            "{topic} officieel nieuws {year}",
            "{topic} analyse en belangrijkste ontwikkelingen {year}",
        ],
        "en": [
            "{topic} latest updates {year}",
            "{topic} official news {year}",
            "{topic} analysis and key developments {year}",
        ],
    }
    query_templates = query_templates_by_language[language]
    custom_templates = custom_topic_templates[language]
    queries: list[dict[str, str]] = []
    for topic in topics:
        topic_queries = query_templates.get(topic)
        if topic_queries is None:
            topic_label = topic.replace("-", " ").replace("_", " ")
            topic_queries = [template.format(topic=topic_label, year=current_year) for template in custom_templates]
        for query in topic_queries:
            queries.append({"track_type": topic, "query": query})
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


def google_news_rss_search_with_locale(query: str, max_results: int, locale: dict[str, str]) -> list[dict[str, Any]]:
    url = (
        f"{GOOGLE_NEWS_RSS_ENDPOINT}?q={quote_plus(query)}"
        f"&hl={quote_plus(locale['hl'])}"
        f"&gl={quote_plus(locale['gl'])}"
        f"&ceid={quote_plus(locale['ceid'])}"
    )
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) PRA-v3/1.0"},
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
        description = strip_html(item.findtext("description") or "")
        source_tag = item.find("source")
        source_text = (source_tag.text or "").strip() if source_tag is not None else ""
        source_url = source_tag.attrib.get("url", "") if source_tag is not None else ""
        if not link:
            continue
        results.append(
            {
                "title": title or link,
                "url": link,
                "content": description,
                "score": 0.55,
                "source_label": source_text,
                "source_url": source_url,
            }
        )
    return results


def google_news_rss_search(query: str, max_results: int, language: str) -> list[dict[str, Any]]:
    locale = RSS_LOCALE.get(normalize_language(language), RSS_LOCALE["en"])
    results = google_news_rss_search_with_locale(query, max_results, locale)
    if results or locale == RSS_LOCALE["en"]:
        return results
    return google_news_rss_search_with_locale(query, max_results, RSS_LOCALE["en"])


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
    db_path: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    cache_hits = 0
    live_errors: list[str] = []
    web_errors: list[str] = []
    fallback_chain: list[str] = []
    effective_mode = mode
    if mode == "auto":
        effective_mode = "live" if os.getenv("TAVILY_API_KEY", "").strip() else "web_fallback"
        fallback_chain.append(f"auto->{effective_mode}")

    for query_item in queries:
        track_type = query_item["track_type"]
        if effective_mode == "live":
            try:
                results = tavily_search(query_item["query"], max_results_per_query)
            except RuntimeError as exc:
                live_errors.append(str(exc))
                try:
                    results = google_news_rss_search(query_item["query"], max_results_per_query, language)
                    fallback_chain.append("live->web_fallback")
                    effective_mode = "web_fallback"
                except RuntimeError as web_exc:
                    web_errors.append(str(web_exc))
                    results = fixture_results(track_type)
                    fallback_chain.append("live->fixture_fallback")
                    effective_mode = "fixture_fallback"
        elif effective_mode == "web_fallback":
            try:
                results = google_news_rss_search(query_item["query"], max_results_per_query, language)
            except RuntimeError as web_exc:
                web_errors.append(str(web_exc))
                results = fixture_results(track_type)
                fallback_chain.append("web_fallback->fixture_fallback")
                effective_mode = "fixture_fallback"
        else:
            results = fixture_results(track_type)

        for result in results[:max_results_per_query]:
            url = str(result.get("url", "")).strip()
            if not url:
                continue
            cached = db.get_article_by_url(url, db_path)
            if cached is not None:
                cache_hits += 1
            candidate = {
                "item_id": db.article_id_for_url(url),
                "track_type": track_type,
                "query": query_item["query"],
                "title": str(result.get("title", "")).strip() or url,
                "url": url,
                "summary": clean_summary(str(result.get("content") or result.get("summary") or "").strip()),
                "score": float(result.get("score") or 0.0),
                "source": domain_from_url(str(result.get("source_url") or url)),
                "source_label": str(result.get("source_label") or ""),
            }
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
                },
                db_path,
            )
            candidates.append(candidate)

    trace = {
        "retriever": (
            "tavily"
            if effective_mode == "live"
            else "google_news_rss"
            if effective_mode == "web_fallback"
            else "fixture"
        ),
        "retrieval_provider": (
            "tavily"
            if effective_mode == "live"
            else "google_news_rss"
            if effective_mode == "web_fallback"
            else "local_fixture"
        ),
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
    text = text.removeprefix("# DutchNews.nl - ")
    if len(text) > max_length:
        text = text[: max_length - 3].rstrip() + "..."
    return text


def reject_reason(candidate: dict[str, Any], now: datetime) -> tuple[str | None, str]:
    url = candidate["url"].lower()
    path = url_path(candidate["url"])
    query = url_query(candidate["url"])
    title = candidate["title"].lower()
    summary = candidate.get("summary", "").lower()
    track_type = candidate["track_type"]
    source = candidate.get("source", "")
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

    date = extract_date(url + " " + title + " " + summary)
    if date is not None:
        age_days = (now - date).days
        if track_type == "news" and age_days > 14:
            return "not_recent", f"age_days={age_days}"
        if track_type == "events" and age_days > 1:
            return "not_recent", f"event_age_days={age_days}"
        if track_type == "events" and age_days < -45:
            return "too_far_future", f"event_age_days={age_days}"
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
            "report_title": "# Report Personal Research Agent v3",
            "newsletter_title": "# Newsletter Personal Research Agent v3",
            "quality": "Qualità",
            "selected_counts": "Conteggi selezionati",
            "selected": "Selezionati",
            "selected_items": "## Elementi selezionati",
            "none_selected": "Nessun elemento selezionato in questa run.",
            "source": "Fonte",
            "why_selected": "Perché selezionato",
            "summary": "Riepilogo",
            "query": "Query",
            "no_strong_item": "nessun elemento forte selezionato",
        },
        "nl": {
            "report_title": "# Personal Research Agent v3 Rapport",
            "newsletter_title": "# Personal Research Agent v3 Nieuwsbrief",
            "quality": "Kwaliteit",
            "selected_counts": "Geselecteerde aantallen",
            "selected": "Geselecteerd",
            "selected_items": "## Geselecteerde items",
            "none_selected": "Geen item geselecteerd in deze run.",
            "source": "Bron",
            "why_selected": "Waarom geselecteerd",
            "summary": "Samenvatting",
            "query": "Query",
            "no_strong_item": "geen sterk item geselecteerd",
        },
        "en": {
            "report_title": "# Personal Research Agent v3 Report",
            "newsletter_title": "# Personal Research Agent v3 Newsletter",
            "quality": "Quality",
            "selected_counts": "Selected counts",
            "selected": "Selected",
            "selected_items": "## Selected Items",
            "none_selected": "No item selected in this run.",
            "source": "Source",
            "why_selected": "Why selected",
            "summary": "Summary",
            "query": "Query",
            "no_strong_item": "no strong item selected",
        },
    }
    return labels.get(language, labels["en"])


def build_outputs(
    user: dict[str, Any],
    selected: list[dict[str, Any]],
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
        track_items = [item for item in selected if item["track_type"] == track]
        if not track_items:
            report_lines.append(f"- {labels['none_selected']}")
        for item in track_items:
            report_lines.append(f"- {md_link(item)}")
            report_lines.append(
                f"  - {labels['source']}: {item.get('source', 'unknown')} | Score: {item.get('quality_score', item.get('score', 0))}"
            )
            report_lines.append(f"  - {labels['why_selected']}: {item.get('selection_reason', 'matched query')}")
            report_lines.append(f"  - {labels['summary']}: {item.get('summary') or 'No summary available.'}")
            report_lines.append(f"  - {labels['query']}: `{item.get('query', '')}`")

    newsletter_lines = [
        labels["newsletter_title"],
        "",
        f"{labels['quality']}: {quality}",
        f"{labels['selected']}: " + ", ".join(f"{topic}={counts.get(topic, 0)}" for topic in topics),
        "",
    ]
    for track in topics:
        track_items = [item for item in selected if item["track_type"] == track]
        if not track_items:
            newsletter_lines.append(f"- {track}: {labels['no_strong_item']}")
            continue
        for item in track_items[:2]:
            summary = item.get("summary") or ""
            if len(summary) > 170:
                summary = summary[:167].rstrip() + "..."
            newsletter_lines.append(f"- {track}: {md_link(item)} - {summary}")
    return "\n".join(report_lines), "\n".join(newsletter_lines)


def run_research_digest(
    chat_id: int,
    mode: str = "auto",
    max_results_per_query: int = DEFAULT_MAX_RESULTS_PER_QUERY,
) -> PipelineResult:
    config = app_config.load_app_config()
    user = db_users.ensure_user(chat_id=chat_id, db_path=config.db_path)
    run_language = normalize_language(str(user.get("language") or config.default_language))
    topics_for_run = normalize_topics_for_run(user.get("topics"))
    run_id = db.log_run(
        user_id=int(user["id"]),
        quality_status="running",
        selected_counts={track: 0 for track in topics_for_run},
        db_path=config.db_path,
    )
    debug_dir = project_path("debug") / f"{slug_timestamp()}__v3-{run_id}"
    context = {
        "run_id": f"v3-{run_id}",
        "chat_id": chat_id,
        "thread_id": f"chat-{chat_id}",
        "timestamp": utc_now(),
    }

    try:
        queries = build_queries(user)
        write_json(
            debug_dir / "01_input.json",
            "input",
            mode,
            context,
            {"user": {"id": user["id"], "name": user["name"], "language": user["language"], "topics": user["topics"]}, "queries": queries},
        )
        candidates, retrieval_trace = retrieve_candidates(queries, mode, max_results_per_query, run_language, config.db_path)
        write_json(
            debug_dir / "02_retrieval.json",
            "retrieval",
            retrieval_trace["mode_used"],
            context,
            {"candidate_count": len(candidates), "trace": retrieval_trace, "candidate_preview": candidates[:10]},
        )
        validated, rejected, reason_counts = validate_candidates(candidates)
        write_json(
            debug_dir / "02_validator.json",
            "validator",
            retrieval_trace["mode_used"],
            context,
            {
                "candidate_count": len(candidates),
                "valid_count": len(validated),
                "rejected_count": len(rejected),
                "reason_counts": reason_counts,
                "tracks_seen": sorted({item["track_type"] for item in candidates}),
                "validated_preview": validated[:10],
                "rejected_preview": rejected[:10],
            },
        )
        selected = select_items(validated, topics_for_run)
        counts = selected_counts(selected, topics_for_run)
        flags = quality_flags(counts, retrieval_trace["mode_used"], reason_counts, topics_for_run)
        quality = quality_status(flags)
        report, newsletter = build_outputs(user, selected, counts, quality, topics_for_run, run_language)
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
                    "run_id": f"v3-{run_id}",
                    "selected_counts": counts,
                    "reason_counts": reason_counts,
                    "quality_gate_status": {"status": quality, "selected": counts, "flags": flags},
                    "quality_flags_summary": flags,
                    "retrieval_trace": retrieval_trace,
                },
            },
        )
        db.update_run_summary(
            run_id=run_id,
            report_path=str(report_path.relative_to(app_config.PROJECT_ROOT)),
            newsletter_path=str(newsletter_path.relative_to(app_config.PROJECT_ROOT)),
            quality_status=quality,
            selected_counts=counts,
            db_path=config.db_path,
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
        )
    except Exception as exc:
        write_json(debug_dir / "error.json", "error", mode, context, {"error": str(exc)})
        db.update_run_summary(
            run_id=run_id,
            quality_status="error",
            selected_counts={track: 0 for track in topics_for_run},
            db_path=config.db_path,
        )
        raise


def format_console_summary(result: PipelineResult) -> str:
    counts = ", ".join(f"{track}={count}" for track, count in result.selected_counts.items())
    if result.language == "it":
        return (
            "Digest Personal Research Agent v3 completato "
            f"(run_id={result.run_id}, quality={result.quality_status}, mode={result.mode}). "
            f"Selezionati: {counts}. "
            f"Report: {result.report_path}. Newsletter: {result.newsletter_path}. Debug: {result.debug_dir}"
        )
    if result.language == "nl":
        return (
            "Personal Research Agent v3 digest voltooid "
            f"(run_id={result.run_id}, quality={result.quality_status}, mode={result.mode}). "
            f"Geselecteerd: {counts}. "
            f"Rapport: {result.report_path}. Nieuwsbrief: {result.newsletter_path}. Debug: {result.debug_dir}"
        )
    return (
        "Personal Research Agent v3 digest complete "
        f"(run_id={result.run_id}, quality={result.quality_status}, mode={result.mode}). "
        f"Selected: {counts}. "
        f"Report: {result.report_path}. Newsletter: {result.newsletter_path}. Debug: {result.debug_dir}"
    )
