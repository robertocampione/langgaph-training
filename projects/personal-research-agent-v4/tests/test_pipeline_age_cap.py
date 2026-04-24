"""Unit tests for pipeline Fix A: dynamic age cap and validate_candidates with topic_settings.

Run with:
    cd projects/personal-research-agent-v4
    python -m pytest tests/test_pipeline_age_cap.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path

# Make sure the project root is on the path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from datetime import datetime, timezone
from unittest.mock import patch

from app.pipeline import (
    MAX_BITCOIN_AGE_DAYS,
    MAX_FINANCE_AGE_DAYS,
    MAX_NEWS_AGE_DAYS,
    _age_cap_days_for_track,
    reject_reason,
    validate_candidates,
)


# ── _age_cap_days_for_track tests ─────────────────────────────────────────────


def test_age_cap_global_fallback_news():
    """Without topic_settings, falls back to MAX_NEWS_AGE_DAYS."""
    assert _age_cap_days_for_track("news") == MAX_NEWS_AGE_DAYS


def test_age_cap_global_fallback_bitcoin():
    """Without topic_settings, falls back to MAX_BITCOIN_AGE_DAYS."""
    assert _age_cap_days_for_track("bitcoin") == MAX_BITCOIN_AGE_DAYS


def test_age_cap_global_fallback_finance():
    """Without topic_settings, falls back to MAX_FINANCE_AGE_DAYS."""
    assert _age_cap_days_for_track("finance") == MAX_FINANCE_AGE_DAYS


def test_age_cap_honours_topic_settings_bitcoin():
    """topic_settings.time_window_days overrides the global constant."""
    settings = {"bitcoin": {"time_window_days": 30}}
    cap = _age_cap_days_for_track("bitcoin", topic_settings=settings)
    assert cap == 30, f"Expected 30, got {cap}"


def test_age_cap_honours_topic_settings_finance():
    """time_window_days=14 for finance should return 14 not MAX_FINANCE_AGE_DAYS."""
    settings = {"finance": {"time_window_days": 14}}
    cap = _age_cap_days_for_track("finance", topic_settings=settings)
    assert cap == 14


def test_age_cap_honours_track_type_over_family():
    """track_type key takes priority over track_family key."""
    settings = {
        "bitcoin": {"time_window_days": 7},
        "finanza e bitcoin": {"time_window_days": 30},
    }
    cap = _age_cap_days_for_track("bitcoin", topic_settings=settings, track_type="finanza e bitcoin")
    assert cap == 30, f"track_type='finanza e bitcoin' should win, got {cap}"


def test_age_cap_zero_time_window_falls_back():
    """time_window_days=0 should fall back to global constant."""
    settings = {"bitcoin": {"time_window_days": 0}}
    cap = _age_cap_days_for_track("bitcoin", topic_settings=settings)
    assert cap == MAX_BITCOIN_AGE_DAYS


def test_age_cap_none_topic_settings():
    """None topic_settings should fall back to global constant."""
    cap = _age_cap_days_for_track("bitcoin", topic_settings=None)
    assert cap == MAX_BITCOIN_AGE_DAYS


def test_age_cap_unknown_track_family():
    """Unknown family falls back to MAX_NEWS_AGE_DAYS."""
    cap = _age_cap_days_for_track("sports")
    assert cap == MAX_NEWS_AGE_DAYS


# ── reject_reason tests with topic_settings ───────────────────────────────────


def _make_candidate(
    track_type: str,
    track_family: str,
    age_days: int,
    url: str = "https://example.com/article/bitcoin-market-update",
    title: str = "Bitcoin Market Update",
    has_excerpt: bool = True,
    has_date: bool = True,
) -> dict:
    now = datetime.now(timezone.utc)
    from datetime import timedelta
    pub_date = (now - timedelta(days=age_days)).isoformat() if has_date else None
    return {
        "url": url,
        "track_type": track_type,
        "track_family": track_family,
        "title": title,
        "summary": "This is a summary about bitcoin market movements and price action.",
        "article_text_excerpt": (
            "Bitcoin price rose sharply today as institutional buyers entered the market. "
            "The total market cap surpassed $1 trillion again." if has_excerpt else ""
        ),
        "published_at": pub_date,
        "source": "coindesk.com",
        "source_type": "article",
        "published_at_confidence": 0.9 if has_date else 0.0,
    }


def test_bitcoin_rejected_stale_with_global_cap():
    """Bitcoin article 20 days old is rejected with global cap of 7 days."""
    now = datetime.now(timezone.utc)
    candidate = _make_candidate("bitcoin", "bitcoin", age_days=20)
    reason, detail = reject_reason(candidate, now, topic_settings=None)
    assert reason == "not_recent", f"Expected not_recent, got {reason}"
    # age_days may be 19 or 20 depending on millisecond timing, check prefix only
    assert detail.startswith("bitcoin_age_days="), f"Unexpected detail: {detail}"
    age = int(detail.split("=")[1])
    assert age >= 10, f"Expected age >= 10, got {age}"


def test_bitcoin_accepted_with_extended_window():
    """Bitcoin article 20 days old is accepted when topic_settings.time_window_days=30."""
    now = datetime.now(timezone.utc)
    candidate = _make_candidate("bitcoin", "bitcoin", age_days=20)
    settings = {"bitcoin": {"time_window_days": 30}}
    reason, detail = reject_reason(candidate, now, topic_settings=settings)
    assert reason is None, f"Should be accepted with 30-day window, got reason={reason} detail={detail}"


def test_bitcoin_rejected_with_10day_window_at_20days():
    """Bitcoin article 20 days old is still rejected with time_window_days=10."""
    now = datetime.now(timezone.utc)
    candidate = _make_candidate("bitcoin", "bitcoin", age_days=20)
    settings = {"bitcoin": {"time_window_days": 10}}
    reason, _ = reject_reason(candidate, now, topic_settings=settings)
    assert reason == "not_recent"


# ── validate_candidates with topic_settings ───────────────────────────────────


def test_validate_candidates_passes_topic_settings_to_reject_reason():
    """validate_candidates propagates topic_settings so age caps are dynamic."""
    now = datetime.now(timezone.utc)
    # 20-day old bitcoin article — should fail with global cap but pass with 30-day window
    candidate = _make_candidate("bitcoin", "bitcoin", age_days=20)
    settings_expanded = {"bitcoin": {"time_window_days": 30}}

    valid_tight, rejected_tight, counts_tight = validate_candidates(
        [candidate], topic_settings=None
    )
    valid_wide, rejected_wide, counts_wide = validate_candidates(
        [candidate], topic_settings=settings_expanded
    )

    assert len(rejected_tight) == 1, "Should be rejected with default 7-day cap"
    assert counts_tight.get("not_recent", 0) == 1

    assert len(valid_wide) == 1, "Should pass with 30-day cap from topic_settings"
    assert len(rejected_wide) == 0


def test_validate_candidates_empty_list():
    """validate_candidates handles empty input cleanly."""
    valid, rejected, counts = validate_candidates([])
    assert valid == []
    assert rejected == []
    assert counts == {}
