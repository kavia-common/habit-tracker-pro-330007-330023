#!/usr/bin/env python3
"""Initialize and deterministically upgrade the SQLite database for habit tracking.

This script is the database container entrypoint used to ensure that:
- the SQLite file exists and is accessible
- the Habit Tracker schema exists and is upgraded deterministically
- seed data is present (idempotent)

Important:
- db_connection.txt is treated as the canonical connection reference and is NOT rewritten
  by this script. If paths change, update db_connection.txt explicitly.

Usage:
  python3 init_db.py
"""

from __future__ import annotations

import os
import sqlite3
from typing import Optional

from schema_migrations import ensure_schema, get_latest_schema_version

DB_NAME = "myapp.db"


def _read_canonical_db_path_from_db_connection_txt() -> Optional[str]:
    """
    Best-effort extraction of the canonical DB file path from db_connection.txt.

    We keep this resilient because db_connection.txt is a human-maintained reference.
    If parsing fails, we fall back to DB_NAME in the current directory.
    """
    try:
        if not os.path.exists("db_connection.txt"):
            return None

        with open("db_connection.txt", "r", encoding="utf-8") as f:
            for line in f:
                if line.strip().lower().startswith("# file path:"):
                    # Format from this repo's db_connection.txt:
                    # "# File path: /abs/path/to/myapp.db"
                    _, value = line.split(":", 1)
                    path = value.strip()
                    return path if path else None
    except Exception:
        # Don't fail init if this cannot be read.
        return None
    return None


def _ensure_sqlite_env_file(db_path: str) -> None:
    """Write db_visualizer/sqlite.env so the DB viewer points to the correct file."""
    os.makedirs("db_visualizer", exist_ok=True)
    try:
        with open("db_visualizer/sqlite.env", "w", encoding="utf-8") as f:
            f.write(f'export SQLITE_DB="{db_path}"\n')
        print("Environment variables saved to db_visualizer/sqlite.env")
    except Exception as e:
        print(f"Warning: Could not save environment variables: {e}")


def _verify_db_accessible(db_path: str) -> None:
    """Verify the SQLite DB file can be opened and queried."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("SELECT 1")
    finally:
        conn.close()


def main() -> None:
    """Run init/upgrade flow."""
    print("Starting SQLite habit-tracker setup...")

    canonical_path = _read_canonical_db_path_from_db_connection_txt()
    db_path = canonical_path or os.path.abspath(DB_NAME)

    print(f"Database path (canonical-preferred): {db_path}")

    db_exists = os.path.exists(db_path)
    if db_exists:
        print("SQLite database already exists. Verifying accessibility...")
    else:
        print("SQLite database does not exist. It will be created.")

    try:
        _verify_db_accessible(db_path)
        print("Database is accessible.")
    except Exception as e:
        # If file didn't exist, sqlite3.connect will create it; so accessibility errors
        # likely indicate directory/path issues.
        print(f"Warning: Could not verify database accessibility: {e}")

    # Deterministically ensure schema + seeds.
    latest = get_latest_schema_version()
    result = ensure_schema(db_path=db_path, project_name="habit-tracker", expected_latest_version=latest)

    # Keep the DB viewer pointing at the same DB file.
    _ensure_sqlite_env_file(db_path)

    print("\nSQLite setup complete!")
    print(f"Database: {os.path.basename(db_path)}")
    print(f"Location: {db_path}")
    print("")
    print("To use with Node.js viewer, run: source db_visualizer/sqlite.env")
    print("")
    print("Database schema status:")
    print(f"  Previous version: {result.from_version}")
    print(f"  Current version:  {result.to_version}")
    print(f"  Applied migrations: {list(result.applied_versions)}")
    print(f"  Tables: {list(result.tables)}")
    print("\nScript completed successfully.")


if __name__ == "__main__":
    main()
