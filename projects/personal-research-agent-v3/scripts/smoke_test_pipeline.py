#!/usr/bin/env python3
"""Run a bounded v3 pipeline smoke test and validate trace evidence."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app import pipeline  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chat-id", type=int, default=100000001)
    parser.add_argument("--mode", choices=("fixture", "live"), default="fixture")
    parser.add_argument("--max-results-per-query", type=int, default=1)
    args = parser.parse_args()

    result = pipeline.run_research_digest(
        chat_id=args.chat_id,
        mode=args.mode,
        max_results_per_query=args.max_results_per_query,
    )
    debug_dir = Path(result.debug_dir)
    retrieval = json.loads((debug_dir / "02_retrieval.json").read_text(encoding="utf-8"))
    output = json.loads((debug_dir / "02_output.json").read_text(encoding="utf-8"))
    final_output = json.loads((debug_dir / "final_output.json").read_text(encoding="utf-8"))
    trace = retrieval["payload"]["trace"]

    assert trace["max_results_per_query"] == args.max_results_per_query
    assert trace["result_cap_total"] == trace["query_count"] * args.max_results_per_query
    assert "cache_hits" in trace
    assert "fallback_chain" in trace
    assert "reasoning_active" in trace
    assert "quality_gate_status" in output["payload"]
    assert "quality_flags_summary" in final_output["payload"]["trace_payload"]

    print(
        "pipeline_smoke=pass "
        f"mode={result.mode} quality={result.quality_status} "
        f"run_id={result.run_id} max_results_per_query={args.max_results_per_query}"
    )


if __name__ == "__main__":
    main()

