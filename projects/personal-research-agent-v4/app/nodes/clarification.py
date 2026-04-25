"""Clarification mechanism for Personal Research Agent v4.

Triggers a dynamic HITL (Human-In-The-Loop) question when ambiguity is high.
"""

from __future__ import annotations

import logging
from typing import Any

from app import llm

LOGGER = logging.getLogger(__name__)

def evaluate_clarification_need(
    analyst_report: dict[str, Any],
    topic_settings: dict[str, Any]
) -> dict[str, Any]:
    """Decide if a clarification is strictly needed."""
    if not analyst_report.get("needs_clarification", False):
        return {"trigger_clarification": False}
        
    reason = analyst_report.get("clarification_reason", "unknown")
    
    # Formulate a targeted question instead of a generic "what do you mean?"
    question = ""
    if reason == "missing_queries":
        question = "I couldn't generate strong search parameters. Can you provide more specific keywords?"
    elif reason == "low_confidence_generic_scope":
        question = "This topic seems very broad. Should I focus strictly on local news, or do you want a global overview?"
    elif reason == "low_coherence":
        question = "The queries seem mixed or conflicting. Which aspect of this topic is most important to you?"
    else:
        question = "Could you clarify what specific angle you want me to research for this topic?"
        
    return {
        "trigger_clarification": True,
        "ambiguity_type": reason,
        "question_text": question
    }
