#!/usr/bin/env python3
"""Deterministically upgrade the SQLite database schema for habit tracking.

This script is useful for CI or manual operations when you want an explicit
upgrade action separate from init.

Notes:
- db_connection.txt remains the canonical connection reference and is not rewritten.
- The upgrade is idempotent; running multiple times is safe.

Usage:
  python3 upgrade_db.py
"""

from __future__ import annotations

import os

from schema_migrations import ensure_schema, get_latest_schema_version

DB_NAME = "myapp.db"


def _read_canonical_db_path_from_db_connection_txt() -> str | None:
    try:
        if not os.path.exists("db_connection.txt"):
            return None
        with open("db_connection.txt", "r", encoding="utf-8") as f:
            for line in f:
                if line.strip().lower().startswith("# file path:"):
                    _, value = line.split(":", 1)
                    path = value.strip()
                    return path if path else None
    except Exception:
        return None
    return None


def main() -> None:
    print("Starting SQLite upgrade...")

    canonical_path = _read_canonical_db_path_from_db_connection_txt()
    db_path = canonical_path or os.path.abspath(DB_NAME)

    latest = get_latest_schema_version()
    result = ensure_schema(db_path=db_path, project_name="habit-tracker", expected_latest_version=latest)

    print("Upgrade complete.")
    print(f"DB: {result.db_path}")
    print(f"from_version={result.from_version} to_version={result.to_version} applied={list(result.applied_versions)}")


if __name__ == "__main__":
    main()
