"""State schema for the v4 research graph."""

from __future__ import annotations

from typing import Any, TypedDict


class ResearchGraphState(TypedDict, total=False):
    chat_id: int
    mode: str
    max_results_per_query: int
    user: dict[str, Any]
    profile: dict[str, Any]
    temporary_contexts: list[dict[str, Any]]
    topic_plan: dict[str, list[str]]
    result: dict[str, Any]
    report: str
    newsletter: str
    debug_dir: str
    quality_status: str
    workflow_logs: list[dict[str, Any]]
    errors: list[str]
