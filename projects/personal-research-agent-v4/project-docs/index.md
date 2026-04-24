# Personal Research Agent v4 Docs

This folder contains project-owned notes and intentional sample artifacts for v4.

## Local Run

From `projects/personal-research-agent-v4`:

```bash
python3 scripts/init_db.py
python3 app/main.py --chat-id 100000001 --mode fixture
```

The project loads `.env` directly from the v4 root. VS Code's `python.terminal.useEnvFile` setting is optional for these commands.

Manual shell fallback:

```bash
set -a
source .env
set +a
```

Bounded live smoke test:

```bash
python3 app/main.py --chat-id 100000001 --mode live --max-results-per-query 1 --no-fallback
```

Trace-validating smoke test:

```bash
python3 scripts/smoke_test_pipeline.py --mode live --max-results-per-query 1
```

Rollout evaluation:

```bash
python3 scripts/evaluate_rollout.py --limit 10 --chat-id 100000001
```

The smoke test also checks deterministic rejection of known weak page shapes: generic news aggregators, event-calendar listings, and Bitcoin newsletter/index pages.

## Telegram

Run a configuration-only check without a token:

```bash
python3 app/tools/telegram_bot.py --dry-run
```

When `.env` contains `TELEGRAM_TOKEN`, the dry-run output should include `token_configured=True`.

After `/run`, testers can send `/feedback 5 useful digest` to persist run-level feedback in SQLite.

Useful control commands now include `/profile`, `/location`, `/travel`, `/sources`, `/subtopics`, `/memory`, `/memory_clear`, and `/onboard`.

## Notebook Harness

Generate a notebook from debug artifacts:

```bash
python3 scripts/generate_test_notebook.py \
  --debug-dir debug/some-run \
  --style-file docs/notebook_style.md \
  --output project-docs/run_analysis.ipynb
```

`sample_v4_debug_analysis.ipynb` is generated from a v4-owned debug run.

## LangGraph Studio

The Studio graph is named `research` and is configured by `langgraph.json`. Use fixture mode for deterministic checks:

```json
{
  "chat_id": 100000001,
  "mode": "fixture",
  "max_results_per_query": 1
}
```
