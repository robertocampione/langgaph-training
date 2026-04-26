"""State schema for the v4 research graph."""

from __future__ import annotations

from typing import Any, TypedDict


class ResearchGraphState(TypedDict, total=False):
    # ── Core identifiers ──────────────────────────────────────────────────────
    chat_id: int
    mode: str
    max_results_per_query: int
    # ── User context (loaded in load_user_context) ────────────────────────────
    user: dict[str, Any]
    profile: dict[str, Any]
    temporary_contexts: list[dict[str, Any]]
    topic_plan: dict[str, list[str]]
    topic_settings: dict[str, Any]
    # ── Intake gate (evaluated in check_intake_gate) ──────────────────────────
    intake_required: bool                   # True if hard gate is active
    intake_gate_detail: dict[str, Any]      # {missing_profile_fields, insufficient_topics}
    # ── Pipeline output (Legacy wrapper) ──────────────────────────────────────
    result: dict[str, Any]
    report: str
    newsletter: str
    debug_dir: str
    quality_status: str
    # ── Execution Metadata ────────────────────────────────────────────────────
    run_id: int
    cost_trace: dict[str, Any]
    selected_counts: dict[str, int]
    # ── Explicit Multi-Agent States ───────────────────────────────────────────
    semantic_audit_results: dict[str, Any]  # Governance check
    analyst_pre_report: dict[str, Any]      # Planning phase report
    clarification_needed: bool              # Trigger HITL
    clarification_request: dict[str, Any]   # Data payload for the HITL question
    query_bundles: list[dict[str, Any]]     # Generated parallel query bundles
    branch_results: dict[str, list[dict[str, Any]]] # Fan-out results
    merged_results: dict[str, list[dict[str, Any]]] # Fan-in deduped results
    validation_report: dict[str, Any]       # Trace of validate_candidates
    analyst_post_report: dict[str, Any]     # Quality Check post-retrieval
    promoted_memories: int                  # Result of memory promotion
    # ── Retrieval diagnostics (populated in quality_guard) ────────────────────
    retrieval_stats: dict[str, Any]         # {coverage_pct, topics_empty, topics_ok}
    quality_guard_passed: bool              # True when quality_status == "ok"
    # ── Final Outputs ─────────────────────────────────────────────────────────
    report_path: str
    newsletter_path: str
    telegram_compact: str
    enriched_items: list[dict[str, Any]]
    # ── Plumbing ──────────────────────────────────────────────────────────────
    workflow_logs: list[dict[str, Any]]
    errors: list[str]
