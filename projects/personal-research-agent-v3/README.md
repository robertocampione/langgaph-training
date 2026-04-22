# Personal Research Agent v3

Personal Research Agent v3 is a standalone LangGraph-oriented project for producing personal research digests with multi-user preferences, persistent SQLite-backed memory, Telegram interaction, and reproducible Jupyter analysis.

## Goals

- Support multiple testers with individual language and topic preferences.
- Persist user preferences, run summaries, feedback, article metadata, and cache entries.
- Keep execution cheap and controllable through explicit pipeline inputs.
- Use Telegram as the first real interaction layer.
- Generate repeatable notebook analyses from debug artifacts.

## Project Structure

- `app/`: Python package for the agent, adapters, state, prompts, memory, and tools.
- `config/`: Project-owned configuration such as sample users.
- `db/`: Local SQLite database files. Generated database files are ignored by Git.
- `debug/`: Disposable run traces and validation artifacts.
- `project-docs/`: Project-specific documentation and intentional sample outputs.
- `scripts/`: Deterministic setup and analysis scripts.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Project commands load `.env` from this folder directly, so they do not require VS Code terminal environment injection.

If VS Code shows `An environment file is configured but terminal environment injection is disabled`, enable `python.terminal.useEnvFile` in VS Code settings or continue using the project commands as-is. For manual shell loading:

```bash
set -a
source .env
set +a
```

## Run

From this folder:

```bash
python3 scripts/init_db.py
python3 app/main.py --chat-id 100000001 --mode fixture
```

The console pipeline loads user preferences from SQLite, runs bounded retrieval, writes a report/newsletter, records debug artifacts, and updates the `runs` table.

Use fixture mode for deterministic local testing. Use live mode for a bounded Tavily smoke test:

```bash
python3 app/main.py --chat-id 100000001 --mode live --max-results-per-query 1 --no-fallback
```

Or run the trace-validating smoke script:

```bash
python3 scripts/smoke_test_pipeline.py --mode fixture --max-results-per-query 1
python3 scripts/smoke_test_pipeline.py --mode live --max-results-per-query 1
```

The pipeline prefers source-specific queries and rejects common low-value listing pages such as search pages, event calendars, generic news aggregators, and Bitcoin index pages. If the live retriever is unavailable or capped, the run falls back to fixture mode and records `quality=warn` with a `retrieval_fallback` trace flag.

## Users and Persistence

Initial testers are configured in `config/users.json`. Run `python3 scripts/init_db.py` after editing that file to insert missing users without dropping existing data.

The SQLite database defaults to `db/personal_research_agent.sqlite`. Override it with:

```bash
DB_PATH=/path/to/agent.sqlite python3 app/main.py --chat-id 100000001
```

The persistence layer stores users, run summaries, article metadata, feedback, and generic cache values.

Run outputs are written under `debug/<timestamp>__v3-<run-id>/`. The SQLite `runs` table stores the relative report/newsletter paths and selected counts.

## Telegram Bot

Create a bot with BotFather, set `TELEGRAM_TOKEN` in `.env`, initialize the DB, and start polling:

```bash
python3 scripts/init_db.py
python3 app/tools/telegram_bot.py
```

Useful commands:

- `/start`: register the chat and show current preferences.
- `/run` or `/news`: run the current agent pipeline for that chat.
- `/topics news events bitcoin`: replace the preferred topic list.
- `/language en`: set the preferred language. Supported values are `en`, `it`, and `nl`.
- `/feedback 5 useful digest`: rate the latest run from 1 to 5 and store notes.

For deterministic local validation without a token:

```bash
python3 app/tools/telegram_bot.py --dry-run
```

With a populated `.env`, dry-run should report `token_configured=True` without printing the token.

## Jupyter Test Harness

Generate a notebook from a debug run:

```bash
python3 scripts/generate_test_notebook.py \
  --debug-dir debug/some-run \
  --style-file docs/notebook_style.md \
  --output project-docs/run_analysis.ipynb
```

The generated notebook summarizes inputs, validation counts, selected categories, rejection reasons, and output lengths. Generated notebooks are ignored by default unless intentionally named as `project-docs/sample_*.ipynb`.

Current samples:

- `project-docs/sample_legacy_v2_debug_analysis.ipynb`: legacy bootstrap sample generated from v2 debug artifacts.
- `project-docs/sample_v3_debug_analysis.ipynb`: v3-native sample generated from a bounded live v3 debug run.

## LangGraph Studio

The v3 graph wraps the same console pipeline exposed by `app/main.py`.

From this folder:

```bash
langgraph dev
```

The graph name is `research`. A minimal invocation state is:

```json
{
  "chat_id": 100000001,
  "mode": "fixture",
  "max_results_per_query": 1
}
```
