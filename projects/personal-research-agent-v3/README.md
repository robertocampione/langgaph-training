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

## Run

From this folder:

```bash
python3 scripts/init_db.py
python3 app/main.py --chat-id 100000001
```

The current stub loads the user preferences from SQLite, logs a run summary, and prints a deterministic readiness message. Later directives add the Telegram adapter and notebook harness.

## Users and Persistence

Initial testers are configured in `config/users.json`. Run `python3 scripts/init_db.py` after editing that file to insert missing users without dropping existing data.

The SQLite database defaults to `db/personal_research_agent.sqlite`. Override it with:

```bash
DB_PATH=/path/to/agent.sqlite python3 app/main.py --chat-id 100000001
```

The persistence layer stores users, run summaries, article metadata, feedback, and generic cache values.

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

For deterministic local validation without a token:

```bash
python3 app/tools/telegram_bot.py --dry-run
```

## Jupyter Test Harness

Generate a notebook from a debug run:

```bash
python3 scripts/generate_test_notebook.py \
  --debug-dir debug/some-run \
  --style-file docs/notebook_style.md \
  --output project-docs/run_analysis.ipynb
```

The generated notebook summarizes inputs, validation counts, selected categories, rejection reasons, and output lengths. Generated notebooks are ignored by default unless intentionally named as `project-docs/sample_*.ipynb`.
