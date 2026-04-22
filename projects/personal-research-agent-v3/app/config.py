"""Runtime configuration for Personal Research Agent v3."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = "db/personal_research_agent.sqlite"
DEFAULT_LANGUAGE = "it"
DEFAULT_TOPICS = ("news", "events", "bitcoin")


@dataclass(frozen=True)
class AppConfig:
    db_path: str
    default_language: str
    default_topics: tuple[str, ...]
    telegram_token_configured: bool


def resolve_project_path(path: str | Path) -> Path:
    value = Path(path)
    if value.is_absolute():
        return value
    return PROJECT_ROOT / value


def load_environment(env_file: str | Path | None = None, override: bool = False) -> Path | None:
    """Load a simple dotenv file into os.environ without printing secrets."""
    env_path = resolve_project_path(env_file or ".env")
    if not env_path.exists():
        return None

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        if key in os.environ and not override:
            continue
        os.environ[key] = value.strip().strip("'\"")
    return env_path


def parse_topics(value: str | None) -> tuple[str, ...]:
    if not value:
        return DEFAULT_TOPICS
    topics = tuple(topic.strip() for topic in value.split(",") if topic.strip())
    return topics or DEFAULT_TOPICS


def load_app_config(env_file: str | Path | None = None) -> AppConfig:
    load_environment(env_file)
    db_path_value = os.getenv("DB_PATH", DEFAULT_DB_PATH)
    return AppConfig(
        db_path=str(resolve_project_path(db_path_value)),
        default_language=os.getenv("DEFAULT_LANGUAGE", DEFAULT_LANGUAGE),
        default_topics=parse_topics(os.getenv("DEFAULT_TOPICS")),
        telegram_token_configured=bool(os.getenv("TELEGRAM_TOKEN", "").strip()),
    )


def get_telegram_token() -> str:
    return os.getenv("TELEGRAM_TOKEN", "").strip()
