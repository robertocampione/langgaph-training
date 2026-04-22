#!/usr/bin/env python3
"""Create the Personal Research Agent v3 SQLite database."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app import db  # noqa: E402
from app import db_users  # noqa: E402
from app import config as app_config  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", default=None, help="SQLite DB path. Defaults to DB_PATH or db/personal_research_agent.sqlite.")
    parser.add_argument("--users", default=db_users.DEFAULT_USERS_PATH, help="JSON user config path.")
    parser.add_argument("--skip-users", action="store_true", help="Create tables without seeding users.")
    args = parser.parse_args()

    config = app_config.load_app_config()
    db_path = args.db_path or config.db_path
    db.initialize_database(db_path)
    seeded = [] if args.skip_users else db_users.seed_users_from_config(args.users, db_path)
    resolved_path = db.get_db_path(db_path)
    print(f"initialized_db={resolved_path}")
    print(f"seeded_users={len(seeded)}")


if __name__ == "__main__":
    main()
