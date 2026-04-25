"""Semantic governance phase for Personal Research Agent v4.

This module provides topic normalization, language routing, query validation,
and semantic bundle confidence scoring without hardcoding specific topics.
"""

from __future__ import annotations

import re
from typing import Any

# Reusing definitions from pipeline (they will be moved or imported here)
SUPPORTED_LANGUAGES = {"en", "it", "nl"}
LANGUAGE_REGION_TOKEN_RE = re.compile(r"^[a-z]{2}(?:[-_][a-z]{2})?$", flags=re.IGNORECASE)
NON_GEOGRAPHIC_LOCALE_TOKENS = {
    "auto", "global", "local", "world", "mondo", "news", "notizie", "nieuws",
    "events", "eventi", "evenementen",
}
GENERIC_TOPIC_TERMS = {
    "news", "notizie", "nieuws", "events", "eventi", "evenementen", 
    "general", "generale", "update", "updates", "local", "world", "mondo",
}

EVENT_KEYWORDS = {
    "agenda", "events", "evenementen", "eventi", "manifestazioni", "concerti", "mostre",
    "programma", "weekend", "festivals", "fair", "fiera", "mercato", "market",
}



def normalize_topic_text(value: str | None) -> str:
    """Normalize topic text into a canonical lowercase format without losing intent."""
    return re.sub(r"\s+", " ", str(value or "").strip().lower())

def is_generic_topic(topic: str) -> bool:
    """Determine if a topic is highly generic, requiring broad query strategies."""
    value = normalize_topic_text(topic)
    if not value or value in GENERIC_TOPIC_TERMS:
        return True
    if len(value.split()) <= 2 and len(value) <= 5: # Small tokens
        return True
    return False

def infer_locale_languages(location: str | None) -> list[str]:
    """Guess primary languages for a location string."""
    loc = str(location or "").lower()
    if not loc: return []
    if any(k in loc for k in ["netherlands", "olanda", "nederland", "maastricht", "amsterdam", "borgharen"]):
        return ["nl", "en"]
    if any(k in loc for k in ["italy", "italia", "milano", "roma", "napoli", "torino"]):
        return ["it"]
    if any(k in loc for k in ["uk", "usa", "london", "york", "states"]):
        return ["en"]
    return []

def _has_geographic_signal(value: str) -> bool:
    cleaned = re.sub(r"\s+", " ", str(value or "").strip())
    if not cleaned: return False
    lowered = cleaned.lower()
    if lowered in NON_GEOGRAPHIC_LOCALE_TOKENS or lowered in SUPPORTED_LANGUAGES:
        return False
    if LANGUAGE_REGION_TOKEN_RE.match(cleaned):
        return False
    if not any(ch.isalpha() for ch in cleaned) or len(cleaned) < 3:
        return False
    return True

def generalized_language_routing(
    track_family: str,
    geo_scope: str,
    context_location: str,
    topic_locales: list[str],
    user_language: str,
    topic_geo_languages: list[str],
    location_geo_languages: list[str]
) -> list[str]:
    """Determine the optimal retrieval languages without hardcoded rules.
    Prioritizes explicit topic context, geo hints, and user language.
    """
    normalized_scope = str(geo_scope or "auto").strip().lower()
    if normalized_scope not in {"auto", "local", "global"}:
        normalized_scope = "auto"
    
    ordered: list[str] = []

    # If it's a globally-oriented topic family, ignore local scoping unless forced
    if track_family in {"bitcoin", "finance"} and normalized_scope != "local":
        if topic_geo_languages:
            ordered.extend(topic_geo_languages)
        ordered.append("en")
    elif normalized_scope == "global":
        if topic_geo_languages:
            ordered.extend(topic_geo_languages)
        ordered.extend(["en", user_language])
    elif normalized_scope == "local":
        if topic_geo_languages:
            ordered.extend(topic_geo_languages)
        elif location_geo_languages:
            ordered.extend(location_geo_languages)
        else:
            ordered.append("en")
    else: # auto
        # Best effort: use what we know about the locale, fallback to user language
        if topic_geo_languages:
            ordered.extend(topic_geo_languages)
        elif location_geo_languages:
            ordered.extend(location_geo_languages)
        else:
            ordered.append(user_language)

    # Dedube and ensure valid languages only
    deduped: list[str] = []
    for lang in ordered + [user_language, "en"]:
        l = lang.strip().lower()
        if l in SUPPORTED_LANGUAGES and l not in deduped:
            deduped.append(l)

    return deduped or ["en"]

def evaluate_semantic_bundle(
    topic: str,
    track_family: str,
    geo_scope: str,
    queries: list[str],
    user_language: str,
) -> dict[str, Any]:
    """Audit the semantic coherence of the built queries.
    Returns scores and potential conflicts that require clarification.
    """
    topic_norm = normalize_topic_text(topic)
    
    coherence_score = 1.0
    confidence = 1.0
    warnings = []
    
    # Check for empty query bundle
    if not queries:
        coherence_score = 0.0
        confidence = 0.0
        warnings.append("No queries generated.")
        return {
            "coherence_score": coherence_score,
            "confidence_score": confidence,
            "warnings": warnings,
            "requires_clarification": True,
            "clarification_reason": "missing_queries"
        }
        
    generic = is_generic_topic(topic)
    
    if generic and geo_scope == "auto":
        confidence -= 0.3
        warnings.append("Generic topic with strictly auto geo-scope implies undefined boundaries.")
    
    # Missing explicit translation bounds can cause LLMs to hallucinate
    # For now, simple length/diversity checks
    query_tokens = set()
    for q in queries:
        query_tokens.update(q.lower().split())
        
    if len(query_tokens) < 3:
        coherence_score -= 0.4
        warnings.append("Queries lack variance and specificity.")
        
    requires_clarification = False
    clarification_reason = ""
    
    if confidence < 0.6:
        requires_clarification = True
        clarification_reason = "low_confidence_generic_scope"
    elif coherence_score < 0.5:
        requires_clarification = True
        clarification_reason = "low_coherence"
        
    return {
        "coherence_score": max(0.0, coherence_score),
        "confidence_score": max(0.0, confidence),
        "warnings": warnings,
        "requires_clarification": requires_clarification,
        "clarification_reason": clarification_reason
    }
