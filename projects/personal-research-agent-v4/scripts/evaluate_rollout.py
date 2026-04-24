#!/usr/bin/env python3
"""Evaluate recent debug runs and export rollout gate metrics."""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app import config as app_config  # noqa: E402
from app import db  # noqa: E402


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def run_dirs(debug_root: Path, limit: int) -> list[Path]:
    dirs = [path for path in debug_root.iterdir() if path.is_dir() and "__v4-" in path.name]
    dirs.sort(key=lambda p: p.name, reverse=True)
    return dirs[:limit]


def evaluate_run(debug_dir: Path) -> dict[str, Any]:
    validator = read_json(debug_dir / "02_validator.json")
    output = read_json(debug_dir / "02_output.json")
    final_output = read_json(debug_dir / "final_output.json")
    payload_validator = validator.get("payload", {})
    payload_output = output.get("payload", {})
    payload_final = final_output.get("payload", {})
    trace_payload = payload_final.get("trace_payload", {})
    final_newsletter = str(payload_final.get("final_newsletter") or "")
    retrieval_trace = trace_payload.get("retrieval_trace", {})
    selected_items = payload_output.get("selected_items_scored", [])

    informative = 1 if len(final_newsletter) >= 450 else 0
    homepage_leakage = int(payload_validator.get("reason_counts", {}).get("not_article_page", 0))
    location_hits = int(payload_output.get("selected_counts", {}).get("events", 0))
    llm_calls = int(payload_output.get("cost_trace", {}).get("llm_calls", 0))
    mode_used = str(retrieval_trace.get("mode_used") or output.get("mode") or "unknown")
    return {
        "run_dir": debug_dir.name,
        "informative_summary": informative,
        "homepage_leakage_rejections": homepage_leakage,
        "location_relevant_event_hits": location_hits,
        "llm_calls": llm_calls,
        "mode_used": mode_used,
        "selected_items": len(selected_items),
    }


def aggregate(results: list[dict[str, Any]], feedback_profile: dict[str, Any]) -> dict[str, Any]:
    if not results:
        return {
            "sample_size": 0,
            "kpis": {},
            "go_no_go": {"status": "no_data", "reasons": ["No debug runs found"]},
        }
    informative_rate = sum(row["informative_summary"] for row in results) / len(results)
    leakage_avg = statistics.mean(row["homepage_leakage_rejections"] for row in results)
    location_avg = statistics.mean(row["location_relevant_event_hits"] for row in results)
    llm_calls_avg = statistics.mean(row["llm_calls"] for row in results)
    feedback_totals = feedback_profile.get("totals", {}) if isinstance(feedback_profile, dict) else {}
    likes = int(feedback_totals.get("like", 0))
    dislikes = int(feedback_totals.get("dislike", 0))
    feedback_trend = likes - dislikes

    reasons: list[str] = []
    if informative_rate < 0.7:
        reasons.append("informative_summary_rate_below_threshold")
    if leakage_avg > 3:
        reasons.append("homepage_leakage_too_high")
    if location_avg < 0.5:
        reasons.append("location_relevance_too_low")
    if feedback_trend < 0:
        reasons.append("negative_feedback_trend")
    status = "go" if not reasons else "no_go"

    return {
        "sample_size": len(results),
        "kpis": {
            "informative_summary_rate": round(informative_rate, 4),
            "homepage_leakage_avg_rejections": round(leakage_avg, 4),
            "location_relevant_event_hits_avg": round(location_avg, 4),
            "llm_calls_avg": round(llm_calls_avg, 4),
            "feedback_trend": feedback_trend,
        },
        "go_no_go": {"status": status, "reasons": reasons},
    }


def write_rollout_report(payload: dict[str, Any]) -> Path:
    rollout_dir = app_config.resolve_project_path("kb_logs/rollout")
    rollout_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = rollout_dir / f"{ts}__rollout_eval.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def main() -> None:
    config = app_config.load_app_config()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=10, help="Number of recent debug runs to evaluate.")
    parser.add_argument("--chat-id", type=int, default=100000001, help="User chat id for feedback trend lookup.")
    args = parser.parse_args()

    debug_root = app_config.resolve_project_path("debug")
    runs = run_dirs(debug_root, max(1, args.limit))
    results = [evaluate_run(path) for path in runs if (path / "02_validator.json").exists() and (path / "02_output.json").exists()]
    user = db.get_user_by_chat_id(chat_id=args.chat_id, db_path=config.runtime_db_path)
    feedback_profile = {}
    if user:
        feedback_profile = db.feedback_profile_for_user(user_id=int(user["id"]), db_path=config.runtime_db_path)
    summary = aggregate(results, feedback_profile)
    payload = {
        "created_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "evaluated_runs": results,
        "summary": summary,
    }
    report_path = write_rollout_report(payload)
    print("rollout_evaluation=" + json.dumps(summary, sort_keys=True))
    print(f"rollout_report_path={report_path}")


if __name__ == "__main__":
    main()
