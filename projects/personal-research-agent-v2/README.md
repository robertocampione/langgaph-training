# Personal Research Agent V2

Notebook-first LangGraph project for higher-quality personal research digests with retrieval hardening, source trust policies, validation, ranking, and model-role strategy.

## V2 Goals
- Improve retrieval quality for news, events, and Bitcoin signals.
- Enforce trust/freshness and reject irrelevant static/reference pages.
- Add validator + ranking/personalization stages with explainable scoring.
- Keep output demo-friendly and comparable with V1 (report + newsletter + multilingual recap).

## Project Structure
- `personal-research-agent-v2.ipynb`: Main implementation and demo notebook.
- `prompts/`: Planner, analyst, validator, personalizer, and output prompts.
- `config/model_config.json`: FAST/QUALITY/REASONING model routing.
- `config/source_policies.json`: per-domain allow/block/freshness rules.
- `data/seen_items.json`: novelty memory.
- `data/user_preferences.json`: location/category personalization defaults.
- `debug/last_run_trace.json`: execution trace and scoring diagnostics.
- `debug/rejected_items.json`: structured validator rejections.

## Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Create `.env` from `.env.example` and set keys.

## Run
From this folder:
```bash
jupyter notebook personal-research-agent-v2.ipynb
```
Run cells top-to-bottom.

## Notes
- Reasoning model is routed to OpenRouter/DeepSeek by config.
- If reasoning credentials are unavailable, validator falls back to `quality_model` and logs this in trace output.
