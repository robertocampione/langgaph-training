# Module 7: Personal Research Digest

## Goal
Build a notebook-first LangGraph learning module that creates a personal research digest for:
- Netherlands and local news (Limburg, Maastricht, Borgharen)
- Next-weekend nearby events
- Bitcoin market, technical, and community updates
- Multilingual recap in English, Italian, and Dutch

This module is intentionally simple and explainable, with architecture choices that can scale later.

## V1 Scope
- Notebook-first implementation (`personal-research-digest.ipynb`)
- Tavily-only retrieval
- Parallel analyst nodes (news/events/bitcoin)
- Lightweight local memory (`data/seen_items.json`, `data/user_preferences.json`)
- Deterministic normalization, deduplication, ranking, and fallback behavior

## Out of Scope (V1)
- LangGraph Studio graph parity
- Scheduler/cron
- Calendar or email integrations
- Persistent DB and multi-user support

## Files
- `personal-research-digest.ipynb`: Main implementation and demo
- `prompts/*.md`: Prompt pack for planner/news/events/bitcoin/personalizer/output
- `data/seen_items.json`: URL/title based seen-item memory
- `data/user_preferences.json`: Local personalization preferences

## Setup
From `langchain-langgraph-tutorial/`:

```bash
pip install -r requirements.txt
```

Set environment variables (example):

```bash
export GOOGLE_API_KEY="..."
export TAVILY_API_KEY="..."
```

## Run
Start Jupyter and open the notebook:

```bash
jupyter notebook module-7/personal-research-digest.ipynb
```

Run cells top-to-bottom.

## Deterministic Guardrails
- Pydantic schema validation for normalized items
- Explicit fallback text when a section has no items
- Deterministic dedupe/ranking logic in code
- Notebook includes assertions for schema/flow and acceptance scenarios

## Roadmap
- Phase 2: richer memory heuristics and stronger relevance tuning
- Phase 3: structured delivery outputs (email/calendar candidate payloads)
- Phase 4: integrations (calendar/email)
- Phase 5: migrate notebook logic into reusable package layout
