"""Long-term memory promotion layer for Personal Research Agent v4.

Handles evaluating generated memory candidates and safely promoting them
to persistent user profile settings or source preferences.
"""

from __future__ import annotations

import logging
from typing import Any

from app import db

LOGGER = logging.getLogger(__name__)

def evaluate_and_promote_candidates(
    user_id: int,
    run_id: int,
    candidates: list[dict[str, Any]],
    db_path: str | None = None
) -> int:
    """Evaluate memory candidates from the run and promote safe ones.
    
    Returns the number of promotions made.
    """
    total_promoted = 0
    for candidate in candidates:
        cand_id = db.append_memory_candidate(
            user_id=user_id,
            run_id=run_id,
            candidate_type=candidate.get("candidate_type", "unknown"),
            source_signal=candidate.get("source_signal", "unknown"),
            payload=candidate.get("payload", {}),
            confidence=candidate.get("confidence", 0.5),
            status="pending",
            db_path=db_path
        )
        
        # Promotion Policy
        if candidate.get("confidence", 0) >= 0.6:
            # Promote to source preferences
            if candidate.get("candidate_type") == "favorite_source":
                domain = candidate.get("payload", {}).get("domain")
                if domain:
                    db.set_source_preference(
                        user_id=user_id,
                        domain=domain,
                        preference="prioritize",
                        db_path=db_path
                    )
                    db.append_memory_promotion(
                        user_id=user_id,
                        candidate_id=cand_id,
                        promotion_reason="High confidence implicit retrieval dominance",
                        target_table="user_source_prefs",
                        payload={"domain": domain, "preference": "prioritize"},
                        db_path=db_path
                    )
                    total_promoted += 1
                    
    return total_promoted
