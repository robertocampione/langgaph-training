"""Smoke tests for graph nodes (Fix D/E): check_intake_gate routing and quality_guard stats.

No external APIs called — uses mocked db and pipeline calls.

Run with:
    cd projects/personal-research-agent-v4
    python -m pytest tests/test_graph_nodes.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.graphs.research_graph import (
    _route_after_intake_check,
    _route_after_quality_guard,
    check_intake_gate,
    quality_guard,
    intake_blocked,
)
from app.state.research_state import ResearchGraphState


# ── _route_after_intake_check ─────────────────────────────────────────────────


def test_route_no_intake_required():
    state: ResearchGraphState = {"intake_required": False}
    assert _route_after_intake_check(state) == "run_pipeline"


def test_route_intake_required():
    state: ResearchGraphState = {"intake_required": True}
    assert _route_after_intake_check(state) == "intake_blocked"


def test_route_intake_missing_key():
    """Missing key defaults to False → run_pipeline."""
    state: ResearchGraphState = {}
    assert _route_after_intake_check(state) == "run_pipeline"


# ── _route_after_quality_guard ────────────────────────────────────────────────


def test_route_quality_ok():
    state: ResearchGraphState = {"quality_status": "ok"}
    assert _route_after_quality_guard(state) == "finalize_output"


def test_route_quality_warn():
    state: ResearchGraphState = {"quality_status": "warn"}
    assert _route_after_quality_guard(state) == "quality_warn_log"


def test_route_quality_error():
    state: ResearchGraphState = {"quality_status": "error"}
    assert _route_after_quality_guard(state) == "finalize_output"


def test_route_quality_intake_required():
    state: ResearchGraphState = {"quality_status": "intake_required"}
    assert _route_after_quality_guard(state) == "quality_warn_log"


def test_route_quality_missing():
    """Unknown quality_status defaults to finalize_output."""
    state: ResearchGraphState = {}
    assert _route_after_quality_guard(state) == "finalize_output"


# ── check_intake_gate ─────────────────────────────────────────────────────────


def test_check_intake_gate_no_user():
    """With no user in state, intake_required is False (can't evaluate gate)."""
    state: ResearchGraphState = {"chat_id": 0, "user": {}}
    with patch("app.graphs.research_graph.app_config.load_app_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(runtime_db_path="/tmp/test.db", default_language="en")
        result = check_intake_gate(state)
    assert result.get("intake_required") is False
    assert isinstance(result.get("intake_gate_detail"), dict)


def test_check_intake_gate_not_required():
    """When pipeline.intake_hard_gate_status returns required=False, route to run_pipeline."""
    user = {"id": 1, "language": "en", "topics": ["bitcoin"]}
    state: ResearchGraphState = {
        "chat_id": 123,
        "user": user,
        "profile": {"language": "en", "home_location": "Maastricht"},
        "temporary_contexts": [],
    }

    gate_result = {"required": False, "missing_profile_fields": [], "insufficient_topics": []}

    with (
        patch("app.graphs.research_graph.app_config.load_app_config") as mock_cfg,
        patch("app.graphs.research_graph.db.get_profile", return_value={"language": "en", "home_location": "Maastricht"}),
        patch("app.graphs.research_graph.db.list_active_temporary_contexts", return_value=[]),
        patch("app.graphs.research_graph.pipeline.ensure_topic_settings", return_value=({}, {})),
        patch("app.graphs.research_graph.pipeline.intake_hard_gate_status", return_value=gate_result),
        patch("app.graphs.research_graph.pipeline.active_location_context", return_value="Maastricht"),
        patch("app.graphs.research_graph.pipeline.normalize_topics_for_run", return_value=["bitcoin"]),
        patch("app.graphs.research_graph.pipeline.normalize_topic_text", side_effect=lambda x: x),
        patch("app.graphs.research_graph.pipeline.normalize_language", return_value="en"),
        patch("app.graphs.research_graph.db.append_workflow_log"),
    ):
        mock_cfg.return_value = MagicMock(runtime_db_path="/tmp/test.db", default_language="en")
        result = check_intake_gate(state)

    assert result["intake_required"] is False
    assert _route_after_intake_check(result) == "run_pipeline"


def test_check_intake_gate_required():
    """When pipeline.intake_hard_gate_status returns required=True, route to intake_blocked."""
    user = {"id": 2, "language": "en", "topics": ["news"]}
    state: ResearchGraphState = {
        "chat_id": 456,
        "user": user,
        "profile": {},
        "temporary_contexts": [],
    }

    gate_result = {
        "required": True,
        "missing_profile_fields": ["home_location"],
        "insufficient_topics": ["news"],
    }

    with (
        patch("app.graphs.research_graph.app_config.load_app_config") as mock_cfg,
        patch("app.graphs.research_graph.db.get_profile", return_value={}),
        patch("app.graphs.research_graph.db.list_active_temporary_contexts", return_value=[]),
        patch("app.graphs.research_graph.pipeline.ensure_topic_settings", return_value=({}, {})),
        patch("app.graphs.research_graph.pipeline.intake_hard_gate_status", return_value=gate_result),
        patch("app.graphs.research_graph.pipeline.active_location_context", return_value=""),
        patch("app.graphs.research_graph.pipeline.normalize_topics_for_run", return_value=["news"]),
        patch("app.graphs.research_graph.pipeline.normalize_topic_text", side_effect=lambda x: x),
        patch("app.graphs.research_graph.pipeline.normalize_language", return_value="en"),
        patch("app.graphs.research_graph.db.append_workflow_log"),
    ):
        mock_cfg.return_value = MagicMock(runtime_db_path="/tmp/test.db", default_language="en")
        result = check_intake_gate(state)

    assert result["intake_required"] is True
    assert "news" in result["intake_gate_detail"]["insufficient_topics"]
    assert _route_after_intake_check(result) == "intake_blocked"


# ── intake_blocked ────────────────────────────────────────────────────────────


def test_intake_blocked_sets_quality_status():
    state: ResearchGraphState = {
        "user": {"id": 1},
        "intake_gate_detail": {
            "missing_profile_fields": ["home_location"],
            "insufficient_topics": ["news"],
        },
    }
    with (
        patch("app.graphs.research_graph.app_config.load_app_config") as mock_cfg,
        patch("app.graphs.research_graph.db.append_workflow_log"),
    ):
        mock_cfg.return_value = MagicMock(runtime_db_path="/tmp/test.db")
        result = intake_blocked(state)

    assert result["quality_status"] == "intake_required"
    assert result["quality_guard_passed"] is False
    assert len(result["errors"]) == 1


# ── quality_guard ─────────────────────────────────────────────────────────────


def test_quality_guard_computes_coverage_ok():
    state: ResearchGraphState = {
        "user": {"id": 1},
        "quality_status": "ok",
        "result": {
            "run_id": 42,
            "selected_counts": {"bitcoin": 2, "news": 1, "events": 0},
            "quality_flags": [],
        },
    }
    with (
        patch("app.graphs.research_graph.app_config.load_app_config") as mock_cfg,
        patch("app.graphs.research_graph.db.append_workflow_log"),
    ):
        mock_cfg.return_value = MagicMock(runtime_db_path="/tmp/test.db")
        result = quality_guard(state)

    stats = result["retrieval_stats"]
    assert stats["coverage_pct"] == round(2 / 3, 3)  # 2 of 3 topics have results
    assert "events" in stats["topics_empty"]
    assert "bitcoin" in stats["topics_ok"]
    assert result["quality_guard_passed"] is True


def test_quality_guard_computes_coverage_warn():
    state: ResearchGraphState = {
        "user": {"id": 1},
        "quality_status": "warn",
        "result": {
            "run_id": 43,
            "selected_counts": {"bitcoin": 0, "news": 2, "events": 0},
            "quality_flags": ["missing_bitcoin", "missing_events"],
        },
    }
    with (
        patch("app.graphs.research_graph.app_config.load_app_config") as mock_cfg,
        patch("app.graphs.research_graph.db.append_workflow_log"),
    ):
        mock_cfg.return_value = MagicMock(runtime_db_path="/tmp/test.db")
        result = quality_guard(state)

    stats = result["retrieval_stats"]
    assert stats["coverage_pct"] == round(1 / 3, 3)
    assert result["quality_guard_passed"] is False
    assert _route_after_quality_guard({**state, **result}) == "quality_warn_log"
