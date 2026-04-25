"""Retrieval mechanisms and fan-out/fan-in parallel execution for v4.

Refactors the pipeline search calls into explicit stages.
"""

from __future__ import annotations

import logging
from typing import Any

from app import config as app_config
# In a real heavy implementation, this would use asyncio.gather for true fan-out.
# We will stub the architectural refactor so that pipeline delegates here.

import asyncio

LOGGER = logging.getLogger(__name__)

async def fan_out_topic_branches(
    topic_plan: list[dict[str, Any]],
    max_results_per_query: int,
    execute_search_fn: Any  # Pass the pipeline's internal search func
) -> dict[str, list[dict[str, Any]]]:
    """Execute parallel retrieval branches per topic using asyncio for true fan-out."""
    LOGGER.info("Starting async fan_out_topic_branches across %d topics", len(topic_plan))
    
    async def _single_query_task(topic_name: str, query: str, language: str, track_family: str):
        LOGGER.debug("Starting retrieval task for topic %s: %s", topic_name, query)
        try:
            # Since execute_search_fn is likely synchronous (urllib), we run it in a thread
            return await asyncio.to_thread(execute_search_fn, query, language, max_results_per_query, topic_name)
        except Exception as exc:
            LOGGER.error("Failed retrieval on topic %s, query %s: %s", topic_name, query, exc)
            return []

    tasks = []
    task_metadata = [] # To map results back to topics

    for plan in topic_plan:
        topic_name = plan.get("topic_name", "")
        if not topic_name:
            continue
        queries = plan.get("optimized_search_queries") or []
        language = plan.get("search_query_language") or "en"
        track_family = plan.get("track_family") or "general"
        
        for query in queries:
            tasks.append(_single_query_task(topic_name, query, language, track_family))
            task_metadata.append(topic_name)

    all_results = await asyncio.gather(*tasks)
    
    # Merge results back into a dict grouped by topic
    results: dict[str, list[dict[str, Any]]] = {plan.get("topic_name", ""): [] for plan in topic_plan if plan.get("topic_name")}
    
    for topic_name, candidates in zip(task_metadata, all_results):
        results[topic_name].extend(candidates or [])
        
    return results

def fan_in_merge_and_dedupe(
    branch_results: dict[str, list[dict[str, Any]]],
    dedupe_fn: Any
) -> dict[str, list[dict[str, Any]]]:
    """Merge and deduplicate items across branches cleanly."""
    LOGGER.info("Executing fan_in_merge_and_dedupe")
    merged = {}
    for topic, items in branch_results.items():
        # Dedupe internally per topic branch
        merged[topic] = dedupe_fn(items)
    return merged
