"""Interpretation layer for readable, actionable output items."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from app import db
from app import llm


DEFAULT_INTERPRETATION_CACHE_TTL_HOURS = 24


@dataclass(frozen=True)
class InterpretationConfig:
    max_items_to_output: int
    max_tokens_per_run: int
    max_llm_items_per_run: int
    llm_score_threshold: float = 0.8
    interpretation_cache_ttl_hours: int = DEFAULT_INTERPRETATION_CACHE_TTL_HOURS


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    return now_utc().replace(microsecond=0).isoformat()


def _as_iso(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat()


def _estimate_tokens(text: str) -> int:
    return max(1, len(text) // 4)


def _language_pack(language: str) -> dict[str, Any]:
    packs = {
        "it": {
            "news_label": "Notizie",
            "events_label": "Eventi",
            "bitcoin_label": "Bitcoin",
            "finance_label": "Finanza",
            "why_prefix": "Perche conta",
            "action_prefix": "Azione",
            "default_action": "Continua a monitorare gli aggiornamenti principali.",
            "topic_action": {
                "news": "Monitora gli aggiornamenti locali e istituzionali.",
                "events": "Valuta se partecipare o salvare la data.",
                "bitcoin": "Monitora il prossimo aggiornamento tecnico.",
                "finance": "Controlla eventuali impatti su mercato o portafoglio.",
            },
        },
        "nl": {
            "news_label": "Nieuws",
            "events_label": "Evenementen",
            "bitcoin_label": "Bitcoin",
            "finance_label": "Financien",
            "why_prefix": "Waarom belangrijk",
            "action_prefix": "Actie",
            "default_action": "Blijf de belangrijkste updates volgen.",
            "topic_action": {
                "news": "Volg lokale en institutionele updates.",
                "events": "Overweeg deelname of sla de datum op.",
                "bitcoin": "Volg de volgende technische update.",
                "finance": "Controleer markt- of portfolio-impact.",
            },
        },
        "en": {
            "news_label": "News",
            "events_label": "Events",
            "bitcoin_label": "Bitcoin",
            "finance_label": "Finance",
            "why_prefix": "Why it matters",
            "action_prefix": "Action",
            "default_action": "Keep tracking key follow-up updates.",
            "topic_action": {
                "news": "Monitor local and institutional updates.",
                "events": "Consider attending or saving the date.",
                "bitcoin": "Track the next technical update.",
                "finance": "Check potential market or portfolio impact.",
            },
        },
    }
    return packs.get(language, packs["en"])


def _category_emoji(category: str) -> str:
    mapping = {
        "news": "📰",
        "events": "🎉",
        "bitcoin": "₿",
        "finance": "📊",
    }
    return mapping.get(category, "•")


def _default_why(item: dict[str, Any], language: str) -> str:
    pack = _language_pack(language)
    category = str(item.get("category") or item.get("track_type") or "news").lower()
    source = str(item.get("source") or "unknown source")
    if language == "it":
        if category == "events":
            return f"Aggiornamento utile per pianificare attivita locali. Fonte: {source}."
        if category == "bitcoin":
            return f"Segnale tecnico o di mercato rilevante per Bitcoin. Fonte: {source}."
        if category == "finance":
            return f"Possibile impatto su mercati, aziende o decisioni economiche. Fonte: {source}."
        return f"Notizia con potenziale impatto su contesto locale o macro. Fonte: {source}."
    if language == "nl":
        if category == "events":
            return f"Nuttig om lokale planning en agenda te verbeteren. Bron: {source}."
        if category == "bitcoin":
            return f"Relevant technisch of marktsignaal voor Bitcoin. Bron: {source}."
        if category == "finance":
            return f"Mogelijke impact op markt, bedrijven of beslissingen. Bron: {source}."
        return f"Nieuws met mogelijke lokale of macro impact. Bron: {source}."
    if category == "events":
        return f"Useful for local planning and scheduling. Source: {source}."
    if category == "bitcoin":
        return f"Relevant technical or market signal for Bitcoin. Source: {source}."
    if category == "finance":
        return f"Potential impact on market, companies, or decisions. Source: {source}."
    return f"News with potential local or macro impact. Source: {source}."


def _default_action(item: dict[str, Any], language: str) -> str:
    pack = _language_pack(language)
    category = str(item.get("category") or item.get("track_type") or "news").lower()
    return str(pack["topic_action"].get(category, pack["default_action"]))


def _normalize_text(value: str) -> str:
    return " ".join(value.strip().lower().split())


def _short_summary(item: dict[str, Any], language: str) -> str:
    summary = str(item.get("summary") or "").strip()
    title = str(item.get("title") or "").strip()
    source = str(item.get("source") or "unknown source")
    category = str(item.get("category") or item.get("track_type") or "news").strip()
    if not summary:
        summary = title
    summary_is_weak = len(summary) < 70 or _normalize_text(summary) == _normalize_text(title)
    if summary_is_weak and title:
        if language == "it":
            summary = f"{title}. Aggiornamento recente sul tema {category}, pubblicato da {source}."
        elif language == "nl":
            summary = f"{title}. Recente update over {category}, gepubliceerd door {source}."
        else:
            summary = f"{title}. Recent update on {category}, published by {source}."
    if len(summary) > 220:
        summary = summary[:217].rstrip() + "..."
    return summary


def deterministic_interpretation(item: dict[str, Any], language: str) -> dict[str, str]:
    return {
        "short_summary": _short_summary(item, language),
        "why_it_matters": _default_why(item, language),
        "suggested_action": _default_action(item, language),
        "interpretation_mode": "deterministic",
        "llm_provider": "",
        "llm_model": "",
    }


def _cache_key(item: dict[str, Any], language: str) -> str:
    url = str(item.get("url") or "")
    item_id = str(item.get("item_id") or "")
    return f"interpretation:{language}:{url or item_id}"


def _cache_is_recent(cached_at: str, ttl_hours: int) -> bool:
    try:
        ts = datetime.fromisoformat(cached_at)
    except ValueError:
        return False
    return ts >= now_utc() - timedelta(hours=ttl_hours)


def get_cached_interpretation(item: dict[str, Any], language: str, ttl_hours: int, db_path: str) -> dict[str, Any] | None:
    cached = db.get_cache_value(_cache_key(item, language), db_path=db_path)
    if not cached:
        return None
    cached_at = str(cached.get("cached_at") or "")
    if not cached_at or not _cache_is_recent(cached_at, ttl_hours):
        return None
    if "short_summary" not in cached or "why_it_matters" not in cached:
        return None
    return cached


def set_cached_interpretation(item: dict[str, Any], language: str, enriched_fields: dict[str, str], db_path: str) -> None:
    payload = {
        "cached_at": now_iso(),
        "short_summary": enriched_fields.get("short_summary", ""),
        "why_it_matters": enriched_fields.get("why_it_matters", ""),
        "suggested_action": enriched_fields.get("suggested_action", ""),
        "interpretation_mode": enriched_fields.get("interpretation_mode", "deterministic"),
        "llm_provider": enriched_fields.get("llm_provider", ""),
        "llm_model": enriched_fields.get("llm_model", ""),
    }
    db.set_cache_value(_cache_key(item, language), payload, db_path=db_path)


def _llm_enabled() -> bool:
    return llm.llm_enabled()


def call_llm_interpretation(item: dict[str, Any], language: str) -> dict[str, str] | None:
    if not _llm_enabled():
        return None
    user_prompt = (
        "You are enriching a news digest item. Return JSON only with keys: "
        "short_summary, why_it_matters, suggested_action. "
        f"Language={language}. "
        "short_summary must be informative (1-2 sentences, not headline repetition). "
        "why_it_matters should be concrete and specific. "
        "suggested_action must be practical and short.\n\n"
        f"Title: {item.get('title', '')}\n"
        f"Source: {item.get('source', '')}\n"
        f"Category: {item.get('category', item.get('track_type', 'news'))}\n"
        f"Snippet: {item.get('summary', '')}\n"
        f"URL: {item.get('url', '')}\n"
    )
    response = llm.call_llm(
        role="utility",
        system_prompt="Return strictly valid compact JSON.",
        user_prompt=user_prompt,
        temperature=0.2,
        timeout_seconds=18,
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

    short_summary = str(parsed.get("short_summary") or "").strip()
    why_it_matters = str(parsed.get("why_it_matters") or "").strip()
    suggested_action = str(parsed.get("suggested_action") or "").strip()
    if not short_summary or not why_it_matters:
        return None
    return {
        "short_summary": short_summary[:260],
        "why_it_matters": why_it_matters[:320],
        "suggested_action": suggested_action[:220],
        "interpretation_mode": "llm",
        "llm_provider": str(response.get("provider") or ""),
        "llm_model": str(response.get("model") or ""),
    }


def should_call_llm(item: dict[str, Any], budget_ctx: dict[str, Any], config: InterpretationConfig) -> bool:
    if not _llm_enabled() or not llm.configured_providers():
        return False
    if int(budget_ctx.get("llm_calls", 0)) >= config.max_llm_items_per_run:
        return False
    final_score = float(item.get("final_score", item.get("quality_score", item.get("score", 0.0))))
    if final_score < config.llm_score_threshold:
        return False
    estimate = _estimate_tokens(
        f"{item.get('title', '')}\n{item.get('summary', '')}\n{item.get('source', '')}\n{item.get('url', '')}"
    ) + 320
    used = int(budget_ctx.get("tokens_used_estimate", 0))
    if used + estimate > config.max_tokens_per_run:
        budget_ctx["budget_exceeded"] = True
        return False
    budget_ctx["next_llm_estimate"] = estimate
    return True


def enrich_items(
    selected_items: list[dict[str, Any]],
    user_context: dict[str, Any],
    budget_ctx: dict[str, Any],
    config: InterpretationConfig,
    db_path: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    language = str(user_context.get("language") or "en").strip().lower() or "en"
    max_items = max(1, int(config.max_items_to_output))
    enriched_items: list[dict[str, Any]] = []
    cache_hits = 0
    llm_calls = 0
    llm_errors = 0
    skipped_items = 0
    providers_used: set[str] = set()
    models_used: set[str] = set()

    for item in selected_items[:max_items]:
        normalized_item = {
            **item,
            "category": str(item.get("category") or item.get("track_type") or "news").lower(),
        }
        wants_llm = should_call_llm(normalized_item, budget_ctx, config)

        cached = get_cached_interpretation(
            normalized_item,
            language=language,
            ttl_hours=config.interpretation_cache_ttl_hours,
            db_path=db_path,
        )
        cached_mode = str((cached or {}).get("interpretation_mode") or "").strip().lower()
        cached_has_llm = cached_mode == "llm"
        if cached and (cached_has_llm or not wants_llm):
            cache_hits += 1
            enriched = {
                **normalized_item,
                "short_summary": str(cached.get("short_summary") or _short_summary(normalized_item, language)),
                "why_it_matters": str(cached.get("why_it_matters") or _default_why(normalized_item, language)),
                "suggested_action": str(cached.get("suggested_action") or _default_action(normalized_item, language)),
                "interpretation_mode": str(cached.get("interpretation_mode") or "cached"),
                "llm_provider": str(cached.get("llm_provider") or ""),
                "llm_model": str(cached.get("llm_model") or ""),
                "language": language,
            }
            enriched_items.append(enriched)
            continue

        fields = deterministic_interpretation(normalized_item, language)
        if wants_llm:
            llm_response = call_llm_interpretation(normalized_item, language)
            if llm_response:
                llm_calls += 1
                fields = llm_response
                if llm_response.get("llm_provider"):
                    providers_used.add(str(llm_response["llm_provider"]))
                if llm_response.get("llm_model"):
                    models_used.add(str(llm_response["llm_model"]))
                estimate = int(budget_ctx.get("next_llm_estimate", 0))
                budget_ctx["tokens_used_estimate"] = int(budget_ctx.get("tokens_used_estimate", 0)) + estimate
                budget_ctx["llm_calls"] = int(budget_ctx.get("llm_calls", 0)) + 1
            else:
                llm_errors += 1
                skipped_items += 1
                budget_ctx["llm_fallbacks"] = int(budget_ctx.get("llm_fallbacks", 0)) + 1
        else:
            skipped_items += 1

        set_cached_interpretation(normalized_item, language, fields, db_path=db_path)
        enriched_items.append(
            {
                **normalized_item,
                "short_summary": fields["short_summary"],
                "why_it_matters": fields["why_it_matters"],
                "suggested_action": fields.get("suggested_action", ""),
                "interpretation_mode": fields.get("interpretation_mode", "deterministic"),
                "llm_provider": fields.get("llm_provider", ""),
                "llm_model": fields.get("llm_model", ""),
                "language": language,
            }
        )

    trace = {
        "language": language,
        "cache_hits": cache_hits,
        "llm_calls": llm_calls,
        "llm_errors": llm_errors,
        "skipped_items": skipped_items,
        "tokens_used_estimate": int(budget_ctx.get("tokens_used_estimate", 0)),
        "max_tokens_per_run": config.max_tokens_per_run,
        "max_llm_items_per_run": config.max_llm_items_per_run,
        "budget_exceeded": bool(budget_ctx.get("budget_exceeded", False)),
        "llm_enabled": bool(_llm_enabled() and llm.configured_providers()),
        "llm_providers_configured": llm.configured_providers(),
        "llm_providers_used": sorted(providers_used),
        "llm_models_used": sorted(models_used),
    }
    return enriched_items, trace


def format_for_telegram(enriched_items: list[dict[str, Any]], user_language: str, max_items: int = 5) -> str:
    language = str(user_language or "en").strip().lower() or "en"
    pack = _language_pack(language)
    lines: list[str] = []
    for item in enriched_items[: max(1, max_items)]:
        category = str(item.get("category") or item.get("track_type") or "news").lower()
        emoji = _category_emoji(category)
        label_key = f"{category}_label"
        category_label = str(pack.get(label_key) or category.title())
        lines.append(f"{emoji} {category_label}")
        lines.append(str(item.get("title") or "Untitled"))
        lines.append(str(item.get("short_summary") or ""))
        lines.append(f"{pack['why_prefix']}: {item.get('why_it_matters', '')}")
        action = str(item.get("suggested_action") or "")
        if action:
            lines.append(f"{pack['action_prefix']}: {action}")
        lines.append(str(item.get("url") or ""))
        lines.append("")
    return "\n".join(lines).strip()
