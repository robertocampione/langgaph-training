"""LangGraph Studio entrypoint for Personal Research Agent v4.

Graph topology (v4.2 — explicit multi-agent routing):

  load_user_context
       │
  preload_profile_memory
       │
  check_intake_gate ──── [intake_required=True] ──► intake_blocked ──► finalize_output
       │ [intake_required=False]
  semantic_governance
       │
  pre_retrieval_analyst
       │
  check_clarification ── [needs_clarification=True] ──► hitl_clarification ──► finalize_output
       │ [needs_clarification=False]
  parallel_retrieval
       │
  post_retrieval_analyst
       │
  memory_promotion
       │
  generation
       │
  quality_guard ──── [quality_status=error] ──► finalize_output
       │             [quality_status=warn]  ──► quality_warn_log ──► finalize_output
       │             [quality_status=ok]
  finalize_output
       │
      END
"""

from __future__ import annotations

import logging
from typing import Any
import uuid

from langgraph.graph import END, StateGraph

from app import config as app_config
from app import db
from app import db_users
from app import pipeline
from app.state.research_state import ResearchGraphState
from app.nodes import semantic_governance as gov
from app.nodes import analysts
from app.nodes import clarification
from app.nodes import retrieval
from app.nodes import generation
from app.memory import promotion

LOGGER = logging.getLogger(__name__)

# ── Base Context Nodes ────────────────────────────────────────────────────────

def load_user_context(state: ResearchGraphState) -> ResearchGraphState:
    chat_id = int(state.get("chat_id") or 0)
    config = app_config.load_app_config()
    user = db_users.ensure_user(chat_id=chat_id, db_path=config.runtime_db_path)
    
    # Initialize run_id
    run_id = db.log_run(
        user_id=int(user["id"]),
        quality_status="running",
        selected_counts={},
        db_path=config.runtime_db_path,
    )
    
    debug_dir = pipeline.project_path("debug") / f"{pipeline.slug_timestamp()}__v4-{run_id}"
    debug_dir.mkdir(parents=True, exist_ok=True)
    
    db.append_execution_log(
        user_id=int(user["id"]),
        run_id=run_id,
        stage="graph_start",
        status="ok",
        message="graph_started",
        payload={"chat_id": chat_id},
        db_path=config.runtime_db_path,
    )
    
    return {
        "chat_id": chat_id, 
        "user": user, 
        "run_id": run_id, 
        "debug_dir": str(debug_dir),
        "errors": []
    }

def preload_profile_memory(state: ResearchGraphState) -> ResearchGraphState:
    config = app_config.load_app_config()
    user = state.get("user") or {}
    user_id = int(user.get("id") or 0)
    profile = db.get_profile(user_id=user_id, db_path=config.runtime_db_path) or {}
    temporary_contexts = db.list_active_temporary_contexts(user_id=user_id, db_path=config.runtime_db_path)
    # The topic plan will be built dynamically in governance, but we load setting traces here.
    return {"profile": profile, "temporary_contexts": temporary_contexts}

def check_intake_gate(state: ResearchGraphState) -> ResearchGraphState:
    config = app_config.load_app_config()
    user = state.get("user") or {}
    user_id = int(user.get("id") or 0)
    if user_id <= 0:
        return {"intake_required": False, "intake_gate_detail": {}}
    profile = state.get("profile") or {}
    topics_for_run = [pipeline.normalize_topic_text(t) for t in pipeline.normalize_topics_for_run(user.get("topics"))]
    location = pipeline.active_location_context(profile, state.get("temporary_contexts") or [])
    topic_settings, _ = pipeline.ensure_topic_settings(
        user_id=user_id, topics=topics_for_run, profile=profile, context_location=location,
        user_language=pipeline.normalize_language(str(user.get("language") or config.default_language)),
        db_path=config.runtime_db_path,
    )
    gate = pipeline.intake_hard_gate_status(user=user, profile=profile, topic_settings=topic_settings)
    intake_required = bool(gate.get("required"))
    return {
        "intake_required": intake_required, 
        "intake_gate_detail": {
            "missing_profile_fields": list(gate.get("missing_profile_fields") or []),
            "insufficient_topics": list(gate.get("insufficient_topics") or [])
        }
    }

def intake_blocked(state: ResearchGraphState) -> ResearchGraphState:
    error_msg = f"INTAKE_REQUIRED: {state.get('intake_gate_detail')}"
    return {"quality_status": "intake_required", "errors": [error_msg], "quality_guard_passed": False}

# ── Explicit Multi-Agent Nodes ────────────────────────────────────────────────

def semantic_governance_node(state: ResearchGraphState) -> ResearchGraphState:
    config = app_config.load_app_config()
    user = state.get("user") or {}
    profile = state.get("profile") or {}
    temporary_contexts = state.get("temporary_contexts") or []
    
    topics_for_run = [gov.normalize_topic_text(t) for t in pipeline.normalize_topics_for_run(user.get("topics"))]
    location = pipeline.active_location_context(profile, temporary_contexts)
    run_language = pipeline.normalize_language(str(user.get("language") or config.default_language))
    
    topic_settings, trace = pipeline.ensure_topic_settings(
        user_id=int(user["id"]), topics=topics_for_run, profile=profile, 
        context_location=location, user_language=run_language, db_path=config.runtime_db_path
    )
    
    topic_plan = pipeline.build_topic_plan(
        user_id=int(user["id"]), topics=topics_for_run, db_path=config.runtime_db_path, topic_settings=topic_settings
    )
    
    # Run generalized governance evaluation
    semantic_audit_results = {}
    query_bundles = []
    
    for topic_name, subtopics in topic_plan.items():
        # Read properties from topic_settings we just fetched/generated
        item_settings = topic_settings.get(topic_name, {})
        track_family = pipeline.infer_track_family(topic_name)
        geo_scope = item_settings.get("geo_scope", "auto")
        search_query_language = item_settings.get("search_query_language", run_language)
        
        # Priority to user/llm optimized queries, otherwise use basic pipeline building
        queries = item_settings.get("optimized_search_queries")
        if not queries:
            queries = [f"{topic_name} ultime notizie" if run_language == "it" else f"{topic_name} news"]
            
        q_bundle = {
            "topic_name": topic_name,
            "optimized_search_queries": queries,
            "search_query_language": search_query_language,
            "geo_scope": geo_scope,
            "track_family": track_family,
        }
        query_bundles.append(q_bundle)
        
        audit = gov.evaluate_semantic_bundle(
            topic=topic_name,
            track_family=track_family,
            geo_scope=geo_scope,
            queries=queries,
            user_language=run_language
        )
        semantic_audit_results[topic_name] = audit

    db.append_workflow_log(
        user_id=int(user["id"]), run_id=None,
        workflow_name="research_graph", step="semantic_governance", status="ok",
        payload={"audit": semantic_audit_results}, db_path=config.runtime_db_path
    )
    
    return {
        "semantic_audit_results": semantic_audit_results, 
        "query_bundles": query_bundles, 
        "topic_plan": topic_plan, 
        "topic_settings": topic_settings
    }

def pre_retrieval_analyst_node(state: ResearchGraphState) -> ResearchGraphState:
    user = state.get("user") or {}
    semantic_audit = state.get("semantic_audit_results") or {}
    
    # Pass down the first item requires clarification if any
    needs_clarification = False
    clarification_reason = ""
    for k, v in semantic_audit.items():
        if v.get("requires_clarification"):
            needs_clarification = True
            clarification_reason = v.get("clarification_reason")
            break
            
    analyst_pre_report = analysts.pre_retrieval_analyst(
        user_id=int(user["id"]),
        run_id=0,
        topic_plan={},
        geo_scope="world",
        user_language="en",
        semantic_audit_results={"requires_clarification": needs_clarification, "clarification_reason": clarification_reason}
    )
    return {"analyst_pre_report": analyst_pre_report}

def _route_after_pre_analyst(state: ResearchGraphState) -> str:
    report = state.get("analyst_pre_report") or {}
    if report.get("needs_clarification"):
        return "hitl_clarification"
    return "parallel_retrieval"

def hitl_clarification_node(state: ResearchGraphState) -> ResearchGraphState:
    config = app_config.load_app_config()
    user = state.get("user") or {}
    report = state.get("analyst_pre_report") or {}
    clarification_req = clarification.evaluate_clarification_need(report, {})
    
    session_id = str(uuid.uuid4())
    clarification_req["clarification_session_id"] = session_id
    
    db.append_workflow_log(
        user_id=int(user.get("id") or 0), run_id=None,
        workflow_name="research_graph", step="hitl_clarification", status="paused",
        payload={"clarification_request": clarification_req}, db_path=config.runtime_db_path
    )
    
    return {
        "clarification_needed": True, 
        "clarification_request": clarification_req, 
        "quality_status": "hitl_blocked",
        "quality_guard_passed": False
    }

async def parallel_retrieval_node(state: ResearchGraphState) -> ResearchGraphState:
    config = app_config.load_app_config()
    mode = str(state.get("mode") or "fixture")
    max_results = int(state.get("max_results_per_query") or pipeline.DEFAULT_MAX_RESULTS_PER_QUERY)
    query_bundles = state.get("query_bundles") or []
    
    def _execute_search(query: str, language: str, max_res: int, track_type: str = "general") -> list[dict[str, Any]]:
        q_payload = [{"query": query, "query_language": language, "retrieval_languages": [language], "track_type": track_type}]
        cands, _ = pipeline.retrieve_candidates(q_payload, mode, max_res, language, config.runtime_db_path)
        return cands

    try:
        branch_results = await retrieval.fan_out_topic_branches(
            topic_plan=query_bundles,
            max_results_per_query=max_results,
            execute_search_fn=_execute_search
        )
        merged_results_dict = retrieval.fan_in_merge_and_dedupe(
            branch_results=branch_results,
            dedupe_fn=pipeline.dedupe_candidates
        )
    except Exception as exc:
        return {"errors": [str(exc)], "quality_status": "error", "quality_guard_passed": False}
        
    return {
        "branch_results": branch_results,
        "merged_results": merged_results_dict,
        "quality_status": "running"
    }

def post_retrieval_analyst_node(state: ResearchGraphState) -> ResearchGraphState:
    user = state.get("user") or {}
    results_by_topic = state.get("merged_results") or {}
    
    analyst_post_report = analysts.post_retrieval_analyst(
        user_id=int(user.get("id") or 0),
        run_id=int(state.get("run_id") or 0),
        results_by_topic=results_by_topic,
        topic_settings={}
    )
    return {"analyst_post_report": analyst_post_report}

def memory_promotion_node(state: ResearchGraphState) -> ResearchGraphState:
    config = app_config.load_app_config()
    user = state.get("user") or {}
    user_id = int(user.get("id") or 0)
    run_id = int(state.get("run_id") or 0)
    
    post_report = state.get("analyst_post_report") or {}
    candidates = post_report.get("memory_candidates") or []
    
    promoted = promotion.evaluate_and_promote_candidates(
        user_id=user_id, run_id=run_id, candidates=candidates, db_path=config.runtime_db_path
    )
    return {"promoted_memories": promoted}

# ── Old Guard Nodes ───────────────────────────────────────────────────────────

def quality_guard(state: ResearchGraphState) -> ResearchGraphState:
    quality_status = str(state.get("quality_status") or "unknown")
    selected_counts = dict(state.get("selected_counts") or {})
    topics_empty = [t for t, c in selected_counts.items() if int(c or 0) == 0]
    total = len(selected_counts) or 1
    coverage_pct = round((total - len(topics_empty)) / total, 3)
    guard_passed = quality_status == "ok"
    retrieval_stats = {"coverage_pct": coverage_pct, "topics_empty": topics_empty, "quality_status": quality_status}
    return {"retrieval_stats": retrieval_stats, "quality_guard_passed": guard_passed}

def quality_warn_log(state: ResearchGraphState) -> ResearchGraphState:
    return {}

def finalize_output(state: ResearchGraphState) -> ResearchGraphState:
    errors = state.get("errors") or []
    return {"quality_status": "error" if errors else state.get("quality_status", "unknown")}

# ── Routing ───────────────────────────────────────────────────────────────────

def _route_after_intake_check(state: ResearchGraphState) -> str:
    return "intake_blocked" if state.get("intake_required") else "semantic_governance"

def _route_after_quality_guard(state: ResearchGraphState) -> str:
    quality = str(state.get("quality_status") or "")
    if quality == "error": return "finalize_output"
    if quality in {"warn", "intake_required"}: return "quality_warn_log"
    return "finalize_output"


# ── Graph assembly ────────────────────────────────────────────────────────────

builder = StateGraph(ResearchGraphState)

builder.add_node("load_user_context", load_user_context)
builder.add_node("preload_profile_memory", preload_profile_memory)
builder.add_node("check_intake_gate", check_intake_gate)
builder.add_node("intake_blocked", intake_blocked)
builder.add_node("semantic_governance", semantic_governance_node)
builder.add_node("pre_retrieval_analyst", pre_retrieval_analyst_node)
builder.add_node("hitl_clarification", hitl_clarification_node)
builder.add_node("parallel_retrieval", parallel_retrieval_node)
builder.add_node("post_retrieval_analyst", post_retrieval_analyst_node)
builder.add_node("generation", generation.generation_node)
builder.add_node("memory_promotion", memory_promotion_node)
builder.add_node("quality_guard", quality_guard)
builder.add_node("quality_warn_log", quality_warn_log)
builder.add_node("finalize_output", finalize_output)

builder.set_entry_point("load_user_context")
builder.add_edge("load_user_context", "preload_profile_memory")
builder.add_edge("preload_profile_memory", "check_intake_gate")

builder.add_conditional_edges(
    "check_intake_gate",
    _route_after_intake_check,
    {"intake_blocked": "intake_blocked", "semantic_governance": "semantic_governance"}
)

builder.add_edge("intake_blocked", "finalize_output")
builder.add_edge("semantic_governance", "pre_retrieval_analyst")

builder.add_conditional_edges(
    "pre_retrieval_analyst",
    _route_after_pre_analyst,
    {"hitl_clarification": "hitl_clarification", "parallel_retrieval": "parallel_retrieval"}
)

builder.add_edge("hitl_clarification", "finalize_output")
builder.add_edge("parallel_retrieval", "post_retrieval_analyst")
builder.add_edge("post_retrieval_analyst", "generation")
builder.add_edge("generation", "memory_promotion")
builder.add_edge("memory_promotion", "quality_guard")

builder.add_conditional_edges(
    "quality_guard",
    _route_after_quality_guard,
    {"finalize_output": "finalize_output", "quality_warn_log": "quality_warn_log"}
)

builder.add_edge("quality_warn_log", "finalize_output")
builder.add_edge("finalize_output", END)

graph = builder.compile()
