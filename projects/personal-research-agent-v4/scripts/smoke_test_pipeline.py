#!/usr/bin/env python3
"""Run a bounded v4 pipeline smoke test and validate trace evidence."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app import pipeline  # noqa: E402


from app.graphs import research_graph
from app.state.research_state import ResearchGraphState

def validate_static_rejections() -> None:
    cases = [
        {
            "track_type": "news",
            "title": "Limburg Breaking News Headlines Today",
            "url": "https://ground.news/interest/limburg-netherlands",
            "summary": "Headline listing.",
            "source": "ground.news",
        },
        {
            "track_type": "events",
            "title": "Event calendar in Maastricht",
            "url": "https://www.visitzuidlimburg.com/govisit/events/?city=maastricht",
            "summary": "Calendar listing.",
            "source": "visitzuidlimburg.com",
        },
    ]
    _, rejected, reason_counts = pipeline.validate_candidates(cases)
    assert len(rejected) == len(cases)
    assert reason_counts

async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chat-id", type=int, default=100000001)
    parser.add_argument("--mode", choices=("fixture", "live", "web_fallback", "auto"), default="fixture")
    parser.add_argument("--max-results-per-query", type=int, default=1)
    args = parser.parse_args()

    validate_static_rejections()
    
    initial_state = ResearchGraphState(
        chat_id=args.chat_id,
        mode=args.mode,
        max_results_per_query=args.max_results_per_query,
    )
    
    print(f"Invoking graph for chat_id={args.chat_id} in mode={args.mode}...")
    final_state = await research_graph.graph.ainvoke(initial_state)
    
    assert final_state.get("user")
    assert final_state.get("topic_plan")
    
    # 1. Semantic Governance Output
    assert "semantic_audit_results" in final_state
    
    # 2. Retrieval fan-out testing
    assert "merged_results" in final_state
    
    # 3. Quality Guard & Quality Flags
    assert "quality_status" in final_state
    
    print(
        "pipeline_smoke=pass "
        f"mode={args.mode} quality={final_state.get('quality_status')} "
        f"max_results_per_query={args.max_results_per_query}"
    )

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
