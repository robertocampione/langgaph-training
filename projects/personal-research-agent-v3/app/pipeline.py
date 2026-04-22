"""Console research pipeline for Personal Research Agent v3."""

from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from app import config as app_config
from app import db
from app import db_users


TRACKS = ("news", "events", "bitcoin")
DEFAULT_MAX_RESULTS_PER_QUERY = 2
TAVILY_ENDPOINT = "https://api.tavily.com/search"


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


def build_queries(user: dict[str, Any], max_topics: int | None = None) -> list[dict[str, str]]:
    topics = [topic for topic in user["topics"] if topic in TRACKS]
    if not topics:
        topics = list(app_config.DEFAULT_TOPICS)
    if max_topics is not None:
        topics = topics[:max_topics]

    query_templates = {
        "news": [
            "recent Netherlands Limburg Maastricht news",
            "latest Limburg Netherlands local news Maastricht",
        ],
        "events": [
            "upcoming family friendly events Maastricht Limburg this weekend",
            "Maastricht Limburg weekend events concerts exhibitions upcoming",
        ],
        "bitcoin": [
            "latest Bitcoin market technical community update",
            "Bitcoin Core recent issue pull request market news",
        ],
    }
    queries: list[dict[str, str]] = []
    for topic in topics:
        for query in query_templates.get(topic, []):
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
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Tavily retrieval failed: {exc}") from exc
    results = data.get("results", [])
    return results if isinstance(results, list) else []


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
    db_path: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    cache_hits = 0
    live_errors: list[str] = []
    fallback_chain: list[str] = []
    effective_mode = mode
    if mode == "auto":
        effective_mode = "live" if os.getenv("TAVILY_API_KEY", "").strip() else "fixture"
        fallback_chain.append(f"auto->{effective_mode}")

    for query_item in queries:
        track_type = query_item["track_type"]
        if effective_mode == "live":
            try:
                results = tavily_search(query_item["query"], max_results_per_query)
            except RuntimeError as exc:
                live_errors.append(str(exc))
                results = fixture_results(track_type)
                fallback_chain.append("live->fixture_fallback")
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
                "summary": str(result.get("content") or result.get("summary") or "").strip(),
                "score": float(result.get("score") or 0.0),
                "source": domain_from_url(url),
            }
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
        "retriever": "tavily" if mode == "live" or effective_mode == "live" else "fixture",
        "retrieval_provider": "tavily" if mode == "live" or effective_mode == "live" else "local_fixture",
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


def extract_date(text: str) -> datetime | None:
    match = re.search(r"(20\d{2})[-/](\d{2})[-/](\d{2})", text)
    if not match:
        return None
    year, month, day = (int(part) for part in match.groups())
    try:
        return datetime(year, month, day, tzinfo=timezone.utc)
    except ValueError:
        return None


def reject_reason(candidate: dict[str, Any], now: datetime) -> tuple[str | None, str]:
    url = candidate["url"].lower()
    title = candidate["title"].lower()
    track_type = candidate["track_type"]
    if any(part in url for part in ["/tag/", "/category/", "/topics/", "/search", "/archive"]):
        return "generic_listing", "listing_or_archive_url"
    if track_type == "news" and any(title == word for word in ["news", "latest news", "112"]):
        return "not_article_page", "static_news_title"
    if track_type == "bitcoin" and url.rstrip("/").endswith(("github.com/bitcoin/bitcoin", "bitcoin.org")):
        return "low_value_bitcoin", "root_or_overview_page"

    date = extract_date(url + " " + title)
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


def select_items(validated: list[dict[str, Any]], per_track: int = 2) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for track in TRACKS:
        track_items = [item for item in validated if item["track_type"] == track]
        selected.extend(sorted(track_items, key=lambda item: item.get("score", 0), reverse=True)[:per_track])
    return selected


def selected_counts(items: list[dict[str, Any]]) -> dict[str, int]:
    return {track: sum(1 for item in items if item["track_type"] == track) for track in TRACKS}


def quality_flags(counts: dict[str, int], mode_used: str, reason_counts: dict[str, int]) -> list[str]:
    flags: list[str] = []
    if mode_used == "fixture_fallback":
        flags.append("retrieval_fallback")
    for track in TRACKS:
        if counts.get(track, 0) == 0:
            flags.append(f"missing_{track}")
    if reason_counts.get("low_value_bitcoin", 0) > 0:
        flags.append("bitcoin_low_value_rejected")
    if reason_counts.get("not_recent", 0) > 0:
        flags.append("stale_items_rejected")
    return flags


def quality_status(flags: list[str]) -> str:
    return "ok" if not any(flag.startswith("missing_") or flag == "retrieval_fallback" for flag in flags) else "warn"


def md_link(item: dict[str, Any]) -> str:
    return f"[{item['title']}]({item['url']})"


def build_outputs(user: dict[str, Any], selected: list[dict[str, Any]], counts: dict[str, int], quality: str) -> tuple[str, str]:
    report_lines = [
        "# Personal Research Agent v3 Report",
        "",
        f"User: {user['name']} | Language: {user['language']} | Quality: {quality}",
        "",
        "## Selected Items",
    ]
    for track in TRACKS:
        report_lines.extend(["", f"### {track.title()}"])
        track_items = [item for item in selected if item["track_type"] == track]
        if not track_items:
            report_lines.append("- No item selected in this run.")
        for item in track_items:
            report_lines.append(f"- {md_link(item)} - {item.get('summary') or 'No summary available.'}")

    newsletter_lines = [
        "# Personal Research Agent v3 Newsletter",
        "",
        f"Quality: {quality}",
        f"Selected: news={counts['news']}, events={counts['events']}, bitcoin={counts['bitcoin']}",
        "",
    ]
    for item in selected[:5]:
        newsletter_lines.append(f"- {item['track_type']}: {md_link(item)}")
    return "\n".join(report_lines), "\n".join(newsletter_lines)


def run_research_digest(
    chat_id: int,
    mode: str = "auto",
    max_results_per_query: int = DEFAULT_MAX_RESULTS_PER_QUERY,
) -> PipelineResult:
    config = app_config.load_app_config()
    user = db_users.ensure_user(chat_id=chat_id, db_path=config.db_path)
    run_id = db.log_run(
        user_id=int(user["id"]),
        quality_status="running",
        selected_counts={track: 0 for track in TRACKS},
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
        candidates, retrieval_trace = retrieve_candidates(queries, mode, max_results_per_query, config.db_path)
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
        selected = select_items(validated)
        counts = selected_counts(selected)
        flags = quality_flags(counts, retrieval_trace["mode_used"], reason_counts)
        quality = quality_status(flags)
        report, newsletter = build_outputs(user, selected, counts, quality)
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
        )
    except Exception as exc:
        write_json(debug_dir / "error.json", "error", mode, context, {"error": str(exc)})
        db.update_run_summary(
            run_id=run_id,
            quality_status="error",
            selected_counts={track: 0 for track in TRACKS},
            db_path=config.db_path,
        )
        raise


def format_console_summary(result: PipelineResult) -> str:
    counts = ", ".join(f"{track}={count}" for track, count in result.selected_counts.items())
    return (
        "Personal Research Agent v3 digest complete "
        f"(run_id={result.run_id}, quality={result.quality_status}, mode={result.mode}). "
        f"Selected: {counts}. "
        f"Report: {result.report_path}. Newsletter: {result.newsletter_path}. Debug: {result.debug_dir}"
    )
