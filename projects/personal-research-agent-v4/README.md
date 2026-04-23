# Personal Research Agent v4

Personal Research Agent v4 is a standalone LangGraph-oriented project for producing personal research digests with multi-user preferences, persistent memory, Telegram interaction, and reproducible Jupyter analysis.

## Goals

- Support multiple testers with individual language and topic preferences.
- Persist user preferences, run summaries, feedback, article metadata, and cache entries.
- Keep execution cheap and controllable through explicit pipeline inputs.
- Use Telegram as the first real interaction layer.
- Generate repeatable notebook analyses from debug artifacts.

## Project Structure

- `app/`: Python package for the agent, adapters, state, prompts, memory, and tools.
- `config/`: Project-owned configuration such as sample users.
- `db/`: Local database files (SQLite for quick local simulation; Postgres-first target for runtime).
- `debug/`: Disposable run traces and validation artifacts.
- `kb_logs/`: Commit-friendly knowledge/verification logs for AI checks and rollout evidence.
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

## Postgres Runtime (Recommended)

Start dedicated project DB:

```bash
docker compose -f docker-compose.postgres.yml up -d
docker compose -f docker-compose.postgres.yml ps
```

Stop:

```bash
docker compose -f docker-compose.postgres.yml down
```

Set runtime env in `.env`:

```bash
DATABASE_URL=postgresql://pra_v4:pra_v4_dev_password@127.0.0.1:5434/pra_v4
```

SQLite remains available for notebook/simulation flows by leaving `DATABASE_URL` empty.

### LLM Routing (Generic Names)

The interpretation layer now uses generic role-based names:

- `LLM_PROVIDER`: default provider (`google`, `openrouter`, `openai`)
- `LLM_FALLBACK_PROVIDERS`: comma-separated fallback order
- `LLM_MODEL`: global fallback model
- `LLM_UTILITY_MODEL`, `LLM_REASONING_MODEL`, `LLM_WEB_MODEL`: role-specific models
- `LLM_UTILITY_PROVIDER`, `LLM_REASONING_PROVIDER`, `LLM_WEB_PROVIDER`: optional role-specific provider overrides
- `LLM_ENABLED`: hard on/off switch for LLM calls

Supported keys:

- `GOOGLE_API_KEY`
- `OPENROUTER_API_KEY`
- `OPENAI_API_KEY`

Backward compatibility remains for `INTERPRET_WITH_LLM` and `PRA_FAST_MODEL`.

## Run

From this folder:

```bash
python3 scripts/init_db.py
python3 app/main.py --chat-id 100000001 --mode fixture
```

The console pipeline loads user/profile preferences, applies temporary memory, runs bounded retrieval, writes report/newsletter/debug artifacts, and updates run/log tables.

Use fixture mode for deterministic local testing. Use live mode for a bounded Tavily smoke test:

```bash
python3 app/main.py --chat-id 100000001 --mode live --max-results-per-query 1 --no-fallback
```

Or run the trace-validating smoke script:

```bash
python3 scripts/smoke_test_pipeline.py --mode fixture --max-results-per-query 1
python3 scripts/smoke_test_pipeline.py --mode live --max-results-per-query 1
```

The pipeline prefers source-specific queries and rejects common low-value listing pages such as search pages, event calendars, generic news aggregators, and Bitcoin index pages. It enriches candidates with article text and applies profile/topic/source/feedback signals. If live retrieval is unavailable, it falls back to deterministic mode and marks quality gates.

## Users and Persistence

Initial testers are configured in `config/users.json`. Run `python3 scripts/init_db.py` after editing that file to insert missing users without dropping existing data.

Default local SQLite path: `db/personal_research_agent.sqlite`. Override it with:

```bash
DB_PATH=/path/to/agent.sqlite python3 app/main.py --chat-id 100000001
```

The persistence layer stores users, run summaries, article metadata, feedback, cache, user profile/versioned events/facts, topic graph, source preferences, temporary contexts, onboarding sessions, and execution/workflow logs.

Run outputs are written under `debug/<timestamp>__v4-<run-id>/`. Durable verification logs are written under `kb_logs/` and rollout evaluations under `kb_logs/rollout/`.

SQLite -> Postgres migration:

```bash
python3 scripts/migrate_sqlite_to_postgres.py --sqlite-path db/personal_research_agent.sqlite
```

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
- `/profile`: show profile + memory summary.
- `/location Maastricht`: update home location.
- `/travel Madrid 7`: apply temporary travel context for location-sensitive topics.
- `/sources add nltimes.nl` / `/sources deny example.com`: source preferences.
- `/subtopics promote bitcoin regulation`: tune topic graph weights.
- `/memory` / `/memory_clear`: inspect and clear temporary memory.
- `/onboard`: restart onboarding survey.
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

Current sample:

- `project-docs/sample_v4_debug_analysis.ipynb`: v4-native sample generated from a bounded debug run.

## Rollout Evaluation

Evaluate recent runs and export KPI gates:

```bash
python3 scripts/evaluate_rollout.py --limit 10 --chat-id 100000001
```

## LangGraph Studio

The v4 graph wraps the same console pipeline exposed by `app/main.py`.

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
