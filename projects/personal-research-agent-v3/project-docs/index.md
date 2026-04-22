# Personal Research Agent v3 Docs

This folder contains project-owned notes and intentional sample artifacts for v3.

## Local Run

From `projects/personal-research-agent-v3`:

```bash
python3 scripts/init_db.py
python3 app/main.py --chat-id 100000001
```

## Telegram

Run a configuration-only check without a token:

```bash
python3 app/tools/telegram_bot.py --dry-run
```

Future docs should cover notebook-based test analysis, feedback handling, and demo preparation.

## Notebook Harness

Generate a notebook from debug artifacts:

```bash
python3 scripts/generate_test_notebook.py \
  --debug-dir debug/some-run \
  --style-file docs/notebook_style.md \
  --output project-docs/run_analysis.ipynb
```
