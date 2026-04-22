"""State schema for the v3 research graph."""

from __future__ import annotations

from typing import Any, TypedDict


class ResearchGraphState(TypedDict, total=False):
    chat_id: int
    mode: str
    max_results_per_query: int
    user: dict[str, Any]
    result: dict[str, Any]
    report: str
    newsletter: str
    debug_dir: str
    quality_status: str
    errors: list[str]

