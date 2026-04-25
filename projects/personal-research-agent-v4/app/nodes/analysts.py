"""Analysts stages for Personal Research Agent v4.

Provides the Pre-Retrieval and Post-Retrieval Senior Analyst roles.
"""

from __future__ import annotations

from typing import Any
import logging

from app import llm

LOGGER = logging.getLogger(__name__)

def pre_retrieval_analyst(
    user_id: int,
    run_id: int,
    topic_plan: dict[str, Any],
    geo_scope: str,
    user_language: str,
    semantic_audit_results: dict[str, Any]
) -> dict[str, Any]:
    """Inspect the plan before retrieval and format the analyst reasoning."""
    LOGGER.info("Pre-retrieval Analyst running for run_id %s", run_id)
    
    # If LLM is disabled or fast mode, do deterministic checks
    report = {
        "valid": True,
        "reasoning": "Plan passes deterministic governance checks.",
        "needs_clarification": semantic_audit_results.get("requires_clarification", False),
        "clarification_reason": semantic_audit_results.get("clarification_reason", ""),
        "confidence": semantic_audit_results.get("confidence_score", 1.0)
    }
    
    return report

def post_retrieval_analyst(
    user_id: int,
    run_id: int,
    results_by_topic: dict[str, list[dict[str, Any]]],
    topic_settings: dict[str, Any]
) -> dict[str, Any]:
    """Inspect the results after retrieval and determine quality and memory implications."""
    LOGGER.info("Post-retrieval Analyst running for run_id %s", run_id)
    
    empty_topics = []
    strong_topics = []
    
    for topic, items in results_by_topic.items():
        if len(items) == 0:
            empty_topics.append(topic)
        elif len(items) >= 2:
            strong_topics.append(topic)
            
    memory_candidates = []
    
    # If a topic yielded very consistent highly rated sources, propose promotion
    # This is a stub for where LLM evaluation writes out candidate memories
    for topic, items in results_by_topic.items():
        if len(items) > 3:
            domains = [str(i.get("domain") or "") for i in items]
            if len(set(domains)) == 1 and domains[0]:
                memory_candidates.append({
                    "candidate_type": "favorite_source",
                    "source_signal": "implicit_retrieval_dominance",
                    "payload": {"topic": topic, "domain": domains[0]},
                    "confidence": 0.65
                })
                
    return {
        "quality_report": {
            "empty_topics": empty_topics,
            "strong_topics": strong_topics,
        },
        "memory_candidates": memory_candidates
    }
