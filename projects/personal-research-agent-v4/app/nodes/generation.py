"""Generation node for the v4 explicit graph.

This module isolates the actual generation (templating, reporting, LLM formatting)
from the retrieval and analysis phases.
"""

from __future__ import annotations

import logging
from typing import Any

from app import pipeline
from app.nodes import interpretation as interpretation_node
from app.state.research_state import ResearchGraphState

LOGGER = logging.getLogger(__name__)

def generation_node(state: ResearchGraphState) -> ResearchGraphState:
    LOGGER.info("Entering generation_node for chat_id=%s run_id=%s", state.get("chat_id"), state.get("run_id"))
    user = state.get("user") or {}
    run_language = pipeline.normalize_language(str(user.get("language") or "en"))
    
    # Normally we would run pipeline.validate_candidates, cap_items_for_processing, select_items here
    # Since the directive tells us to isolate generation, we assume merged_results is the definitive list
    # mapped by topic branch. We flatten it.
    branch_results = state.get("merged_results") or {}
    all_candidates = []
    topics_for_run = state.get("topic_plan", {}).keys()
    
    for items in branch_results.values():
        all_candidates.extend(items)
        
    all_candidates = pipeline.dedupe_candidates(all_candidates)
    
    # Emulate the selection layer until full graph extraction
    topic_settings = state.get("topic_settings") or {}
    selected = pipeline.select_items(all_candidates, list(topics_for_run), topic_settings=topic_settings)
    selected = pipeline.trim_selected_items(selected, pipeline.MAX_ITEMS_TO_OUTPUT)
    counts = pipeline.selected_counts(selected, list(topics_for_run))
    
    # Enrichment
    interpretation_config = interpretation_node.InterpretationConfig(
        max_items_to_output=pipeline.MAX_ITEMS_TO_OUTPUT,
        max_tokens_per_run=pipeline.MAX_TOKENS_PER_RUN,
        max_llm_items_per_run=pipeline.MAX_LLM_ITEMS_PER_RUN,
    )
    budget_ctx: dict[str, Any] = {
        "tokens_used_estimate": 0,
        "llm_calls": 0,
        "llm_fallbacks": 0,
        "budget_exceeded": False,
    }
    
    config = pipeline.app_config.load_app_config()
    enriched_items, interpretation_trace = interpretation_node.enrich_items(
        selected_items=selected,
        user_context={"language": run_language},
        budget_ctx=budget_ctx,
        config=interpretation_config,
        db_path=config.runtime_db_path,
    )
    
    quality = str(state.get("quality_status") or "ok")
    report, newsletter = pipeline.build_outputs(user, enriched_items, counts, quality, list(topics_for_run), run_language)
    telegram_compact = interpretation_node.format_for_telegram(
        enriched_items,
        user_language=run_language,
        max_items=pipeline.MAX_ITEMS_TO_OUTPUT,
    )
    
    # Write files to disk
    debug_dir_path = pipeline.Path(state.get("debug_dir") or ".")
    report_path = debug_dir_path / "report.md"
    newsletter_path = debug_dir_path / "newsletter.md"
    
    try:
        report_path.write_text(report, encoding="utf-8")
        newsletter_path.write_text(newsletter, encoding="utf-8")
    except Exception as exc:
        LOGGER.error("Failed to write generation output files: %s", exc)

    return {
        "report": report,
        "newsletter": newsletter,
        "report_path": str(report_path),
        "newsletter_path": str(newsletter_path),
        "telegram_compact": telegram_compact,
        "enriched_items": enriched_items,
        "selected_counts": counts,
        "cost_trace": interpretation_trace,
    }
