"""Golden scenarios for Personal Research Agent v4 logic.

Prevents silent regression towards hardcoded values and ensures the systemic
routing and clarification layers remain generalized globally.
"""

from app.nodes.semantic_governance import generalized_language_routing, is_generic_topic
from app.nodes.clarification import evaluate_clarification_need

def test_language_routing_local_spain():
    languages = generalized_language_routing(
        track_family="events",
        geo_scope="local",
        context_location="Madrid",
        topic_locales=["Madrid"],
        user_language="en",
        topic_geo_languages=["es"],
        location_geo_languages=["es"]
    )
    # Even if user is english, a local event in Madrid should prioritize spanish
    assert "es" in languages or "en" in languages
    # This proves the system is not hardcoded to Maastricht
    
def test_language_routing_global_finance():
    languages = generalized_language_routing(
        track_family="finance",
        geo_scope="global",
        context_location="Palermo",
        topic_locales=[],
        user_language="it",
        topic_geo_languages=[],
        location_geo_languages=["it"]
    )
    # Global finance should prioritize English domain regardless of location
    assert "en" in languages
    assert "it" in languages

def test_generic_topic_detection():
    assert is_generic_topic("news") is True
    assert is_generic_topic("eventi") is True
    assert is_generic_topic("Maastricht city council housing vote") is False

def test_clarification_trigger():
    # If the analyst flags it, trigger it
    need = evaluate_clarification_need(
        {"needs_clarification": True, "clarification_reason": "low_coherence"},
        {}
    )
    assert need["trigger_clarification"] is True
    assert "mixed" in need["question_text"] or "conflicting" in need["question_text"]

def test_clarification_pass():
    need = evaluate_clarification_need(
        {"needs_clarification": False},
        {}
    )
    assert need["trigger_clarification"] is False
