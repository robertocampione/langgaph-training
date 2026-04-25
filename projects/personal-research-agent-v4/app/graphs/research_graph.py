"""LangGraph Studio entrypoint for Personal Research Agent v4.

Graph topology (v4.1 — conditional routing):

  load_user_context
       │
  preload_profile_memory
       │
  check_intake_gate ──── [intake_required=True] ──► intake_blocked ──► finalize_output
       │ [intake_required=False]
  run_pipeline
       │
  quality_guard ──── [quality_status=error] ──► finalize_output
       │             [quality_status=warn]  ──► quality_warn_log ──► finalize_output
       │             [quality_status=ok]
  finalize_output
       │
      END

Key improvements over v4.0:
- check_intake_gate: evaluates hard gate BEFORE starting the pipeline run,
  surfaces intake_required + intake_gate_detail in the graph state.
- quality_guard: evaluates output quality, computes retrieval_stats (coverage_pct,
  topics_empty) and quality_guard_passed, logs them as a workflow entry.
- quality_warn_log: lightweight logging node for warn-level quality runs.
- All conditional edges use typed routing functions — no magic strings in
  add_conditional_edges() call sites.
"""

from __future__ import annotations

import logging
from typing import Any

from langgraph.graph import END, StateGraph

from app import config as app_config
from app import db
from app import db_users
from app import pipeline
from app.state.research_state import ResearchGraphState


LOGGER = logging.getLogger(__name__)


# ── Node: load_user_context ──────────────────────────────────────────────────

def load_user_context(state: ResearchGraphState) -> ResearchGraphState:
    """Ensure the user record exists and load basic identifiers into state."""
    chat_id = int(state.get("chat_id") or 0)
    LOGGER.info("Graph load_user_context chat_id=%d", chat_id)
    config = app_config.load_app_config()
    user = db_users.ensure_user(chat_id=chat_id, db_path=config.runtime_db_path)
    return {"chat_id": chat_id, "user": user, "errors": []}


# ── Node: preload_profile_memory ─────────────────────────────────────────────

def preload_profile_memory(state: ResearchGraphState) -> ResearchGraphState:
    """Load profile, temporary_contexts, and topic_plan into state."""
    config = app_config.load_app_config()
    user = state.get("user") or {}
    user_id = int(user.get("id") or 0)
    if user_id <= 0:
        return {"errors": ["missing_user_context"]}
    profile = db.get_profile(user_id=user_id, db_path=config.runtime_db_path) or {}
    temporary_contexts = db.list_active_temporary_contexts(
        user_id=user_id, db_path=config.runtime_db_path
    )
    topic_plan = pipeline.build_topic_plan(
        user_id=user_id,
        topics=pipeline.normalize_topics_for_run(user.get("topics")),
        db_path=config.runtime_db_path,
    )
    db.append_workflow_log(
        user_id=user_id,
        run_id=None,
        workflow_name="research_graph",
        step="preload_profile_memory",
        status="ok",
        payload={"temporary_contexts": len(temporary_contexts), "topic_plan": topic_plan},
        db_path=config.runtime_db_path,
    )
    return {
        "profile": profile,
        "temporary_contexts": temporary_contexts,
        "topic_plan": topic_plan,
    }


# ── Node: check_intake_gate ──────────────────────────────────────────────────

def check_intake_gate(state: ResearchGraphState) -> ResearchGraphState:
    """Evaluate the hard intake gate before running the pipeline."""
    config = app_config.load_app_config()
    user = state.get("user") or {}
    user_id = int(user.get("id") or 0)
    LOGGER.info("Graph check_intake_gate user_id=%d", user_id)
    if user_id <= 0:
        return {"intake_required": False, "intake_gate_detail": {}}

    profile = state.get("profile") or db.get_profile(
        user_id=user_id, db_path=config.runtime_db_path
    ) or {}
    topics_for_run = [
        pipeline.normalize_topic_text(t)
        for t in pipeline.normalize_topics_for_run(user.get("topics"))
    ]
    # Re-use ensure_topic_settings so topic_settings are always fresh
    location = pipeline.active_location_context(
        profile, state.get("temporary_contexts") or []
    )
    topic_settings, _ = pipeline.ensure_topic_settings(
        user_id=user_id,
        topics=topics_for_run,
        profile=profile,
        context_location=location,
        user_language=pipeline.normalize_language(
            str(user.get("language") or config.default_language)
        ),
        db_path=config.runtime_db_path,
    )
    gate = pipeline.intake_hard_gate_status(
        user=user, profile=profile, topic_settings=topic_settings
    )
    intake_required = bool(gate.get("required"))
    detail: dict[str, Any] = {
        "missing_profile_fields": list(gate.get("missing_profile_fields") or []),
        "insufficient_topics": list(gate.get("insufficient_topics") or []),
    }
    db.append_workflow_log(
        user_id=user_id,
        run_id=None,
        workflow_name="research_graph",
        step="check_intake_gate",
        status="intake_required" if intake_required else "ok",
        payload={"gate": gate, "intake_required": intake_required},
        db_path=config.runtime_db_path,
    )
    return {"intake_required": intake_required, "intake_gate_detail": detail}


def _route_after_intake_check(state: ResearchGraphState) -> str:
    """Conditional edge: route to intake_blocked if gate is active, else run_pipeline."""
    if state.get("intake_required"):
        return "intake_blocked"
    return "run_pipeline"


# ── Node: intake_blocked ─────────────────────────────────────────────────────

def intake_blocked(state: ResearchGraphState) -> ResearchGraphState:
    """Placeholder node reached when intake is required.

    In CLI/LangGraph Studio use this node to surface what is missing.
    The Telegram bot handles the intake dialogue independently via its own
    session state; this node is for headless/graph inspection flows.
    """
    config = app_config.load_app_config()
    user = state.get("user") or {}
    user_id = int(user.get("id") or 0)
    detail = state.get("intake_gate_detail") or {}
    if user_id > 0:
        db.append_workflow_log(
            user_id=user_id,
            run_id=None,
            workflow_name="research_graph",
            step="intake_blocked",
            status="blocked",
            payload=detail,
            db_path=config.runtime_db_path,
        )
    missing = detail.get("missing_profile_fields") or []
    insufficient = detail.get("insufficient_topics") or []
    error_msg = (
        f"INTAKE_REQUIRED: missing_profile_fields={missing}, "
        f"insufficient_topics={insufficient}"
    )
    return {
        "quality_status": "intake_required",
        "errors": [error_msg],
        "quality_guard_passed": False,
    }


# ── Node: run_pipeline ───────────────────────────────────────────────────────

def run_pipeline(state: ResearchGraphState) -> ResearchGraphState:
    """Execute the full research pipeline and populate result/report/newsletter."""
    config = app_config.load_app_config()
    chat_id = int(state.get("chat_id") or 0)
    mode = str(state.get("mode") or "fixture")
    LOGGER.info("Graph run_pipeline chat_id=%d mode=%s", chat_id, mode)
    max_results = int(
        state.get("max_results_per_query") or pipeline.DEFAULT_MAX_RESULTS_PER_QUERY
    )
    try:
        result = pipeline.run_research_digest(
            chat_id=chat_id,
            mode=mode,
            max_results_per_query=max_results,
        )
    except Exception as exc:
        user = state.get("user") or {}
        user_id = int(user.get("id") or 0)
        if user_id > 0:
            db.append_workflow_log(
                user_id=user_id,
                run_id=None,
                workflow_name="research_graph",
                step="run_pipeline",
                status="error",
                payload={"error": str(exc), "mode": mode},
                db_path=config.runtime_db_path,
            )
        return {"errors": [str(exc)], "quality_status": "error", "quality_guard_passed": False}

    user = state.get("user") or {}
    user_id = int(user.get("id") or 0)
    if user_id > 0:
        db.append_workflow_log(
            user_id=user_id,
            run_id=int(result.run_id),
            workflow_name="research_graph",
            step="run_pipeline",
            status="ok",
            payload={"quality_status": result.quality_status, "mode": result.mode},
            db_path=config.runtime_db_path,
        )
    return {
        "result": {
            "run_id": result.run_id,
            "report_path": result.report_path,
            "newsletter_path": result.newsletter_path,
            "selected_counts": result.selected_counts,
            "mode": result.mode,
            "quality_flags": result.quality_flags,
            "cost_trace": result.cost_trace,
        },
        "report": result.report,
        "newsletter": result.newsletter,
        "debug_dir": result.debug_dir,
        "quality_status": result.quality_status,
    }


# ── Node: quality_guard ───────────────────────────────────────────────────────

def quality_guard(state: ResearchGraphState) -> ResearchGraphState:
    """Evaluate retrieval quality and populate retrieval_stats + quality_guard_passed.

    Computes:
    - ``coverage_pct``: fraction of topics that have ≥1 selected item
    - ``topics_empty``: list of topics with 0 items
    - ``topics_ok``: list of topics with ≥1 item
    - ``quality_guard_passed``: True when quality_status == "ok"
    """
    config = app_config.load_app_config()
    user = state.get("user") or {}
    user_id = int(user.get("id") or 0)
    quality_status = str(state.get("quality_status") or "unknown")
    result = state.get("result") or {}
    selected_counts: dict[str, int] = dict(result.get("selected_counts") or {})

    topics_empty = [t for t, c in selected_counts.items() if int(c or 0) == 0]
    topics_ok = [t for t, c in selected_counts.items() if int(c or 0) > 0]
    total = len(selected_counts) or 1
    coverage_pct = round(len(topics_ok) / total, 3)
    guard_passed = quality_status == "ok"

    retrieval_stats: dict[str, Any] = {
        "coverage_pct": coverage_pct,
        "topics_empty": topics_empty,
        "topics_ok": topics_ok,
        "quality_status": quality_status,
        "quality_flags": list(result.get("quality_flags") or []),
    }
    if user_id > 0:
        db.append_workflow_log(
            user_id=user_id,
            run_id=int(result.get("run_id") or 0) or None,
            workflow_name="research_graph",
            step="quality_guard",
            status="ok" if guard_passed else quality_status,
            payload=retrieval_stats,
            db_path=config.runtime_db_path,
        )
    return {
        "retrieval_stats": retrieval_stats,
        "quality_guard_passed": guard_passed,
    }


def _route_after_quality_guard(state: ResearchGraphState) -> str:
    """Conditional edge: route based on quality_status after quality_guard."""
    quality = str(state.get("quality_status") or "")
    if quality == "error":
        return "finalize_output"
    if quality in {"warn", "intake_required"}:
        return "quality_warn_log"
    return "finalize_output"


# ── Node: quality_warn_log ────────────────────────────────────────────────────

def quality_warn_log(state: ResearchGraphState) -> ResearchGraphState:
    """Log a quality warning and surface retrieval gaps to the caller.

    This node does not block the graph — it simply appends an audit log entry
    and passes state through to finalize_output.
    """
    config = app_config.load_app_config()
    user = state.get("user") or {}
    user_id = int(user.get("id") or 0)
    retrieval_stats = state.get("retrieval_stats") or {}

    if user_id > 0:
        db.append_workflow_log(
            user_id=user_id,
            run_id=int((state.get("result") or {}).get("run_id") or 0) or None,
            workflow_name="research_graph",
            step="quality_warn_log",
            status="warn",
            payload={
                "topics_empty": retrieval_stats.get("topics_empty", []),
                "coverage_pct": retrieval_stats.get("coverage_pct", 0.0),
                "quality_flags": retrieval_stats.get("quality_flags", []),
            },
            db_path=config.runtime_db_path,
        )
    return {}  # pass-through, no state mutation needed


# ── Node: finalize_output ─────────────────────────────────────────────────────

def finalize_output(state: ResearchGraphState) -> ResearchGraphState:
    """Terminal node: normalise quality_status and ensure errors list is clean."""
    errors = state.get("errors") or []
    if errors:
        return {"quality_status": state.get("quality_status") or "error"}
    return {"quality_status": state.get("quality_status", "unknown")}


# ── Graph assembly ─────────────────────────────────────────────────────────────

builder = StateGraph(ResearchGraphState)

# Nodes
builder.add_node("load_user_context", load_user_context)
builder.add_node("preload_profile_memory", preload_profile_memory)
builder.add_node("check_intake_gate", check_intake_gate)
builder.add_node("intake_blocked", intake_blocked)
builder.add_node("run_pipeline", run_pipeline)
builder.add_node("quality_guard", quality_guard)
builder.add_node("quality_warn_log", quality_warn_log)
builder.add_node("finalize_output", finalize_output)

# Entry point and linear edges
builder.set_entry_point("load_user_context")
builder.add_edge("load_user_context", "preload_profile_memory")
builder.add_edge("preload_profile_memory", "check_intake_gate")

# Conditional edge: intake gate
builder.add_conditional_edges(
    "check_intake_gate",
    _route_after_intake_check,
    {
        "intake_blocked": "intake_blocked",
        "run_pipeline": "run_pipeline",
    },
)

# intake_blocked → finalize (no further processing possible without intake)
builder.add_edge("intake_blocked", "finalize_output")

# run_pipeline → quality_guard (always, to measure coverage)
builder.add_edge("run_pipeline", "quality_guard")

# Conditional edge: quality routing
builder.add_conditional_edges(
    "quality_guard",
    _route_after_quality_guard,
    {
        "finalize_output": "finalize_output",
        "quality_warn_log": "quality_warn_log",
    },
)

# quality_warn_log always flows to finalize
builder.add_edge("quality_warn_log", "finalize_output")

# Terminal
builder.add_edge("finalize_output", END)

graph = builder.compile()
