"""LangGraph Studio entrypoint for Personal Research Agent v4."""

from __future__ import annotations

from langgraph.graph import END, StateGraph

from app import config as app_config
from app import db
from app import db_users
from app import pipeline
from app.state.research_state import ResearchGraphState


def load_user_context(state: ResearchGraphState) -> ResearchGraphState:
    config = app_config.load_app_config()
    chat_id = int(state.get("chat_id") or 0)
    user = db_users.ensure_user(chat_id=chat_id, db_path=config.runtime_db_path)
    return {"chat_id": chat_id, "user": user, "errors": []}


def preload_profile_memory(state: ResearchGraphState) -> ResearchGraphState:
    config = app_config.load_app_config()
    user = state.get("user") or {}
    user_id = int(user.get("id") or 0)
    if user_id <= 0:
        return {"errors": ["missing_user_context"]}
    profile = db.get_profile(user_id=user_id, db_path=config.runtime_db_path) or {}
    temporary_contexts = db.list_active_temporary_contexts(user_id=user_id, db_path=config.runtime_db_path)
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
    return {"profile": profile, "temporary_contexts": temporary_contexts, "topic_plan": topic_plan}


def run_pipeline(state: ResearchGraphState) -> ResearchGraphState:
    config = app_config.load_app_config()
    chat_id = int(state.get("chat_id") or 0)
    mode = str(state.get("mode") or "fixture")
    max_results = int(state.get("max_results_per_query") or pipeline.DEFAULT_MAX_RESULTS_PER_QUERY)
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
        return {"errors": [str(exc)], "quality_status": "error"}
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
        },
        "report": result.report,
        "newsletter": result.newsletter,
        "debug_dir": result.debug_dir,
        "quality_status": result.quality_status,
    }


def finalize_output(state: ResearchGraphState) -> ResearchGraphState:
    errors = state.get("errors") or []
    if errors:
        return {"quality_status": "error"}
    return {"quality_status": state.get("quality_status", "unknown")}


builder = StateGraph(ResearchGraphState)
builder.add_node("load_user_context", load_user_context)
builder.add_node("preload_profile_memory", preload_profile_memory)
builder.add_node("run_pipeline", run_pipeline)
builder.add_node("finalize_output", finalize_output)
builder.set_entry_point("load_user_context")
builder.add_edge("load_user_context", "preload_profile_memory")
builder.add_edge("preload_profile_memory", "run_pipeline")
builder.add_edge("run_pipeline", "finalize_output")
builder.add_edge("finalize_output", END)

graph = builder.compile()
