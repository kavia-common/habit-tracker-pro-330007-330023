#!/usr/bin/env python3
"""
Deterministic SQLite schema management for the Habit Tracker database.

This module provides a single canonical flow to:
- ensure required tables exist
- apply schema upgrades deterministically via a small migration registry
- seed initial data deterministically
- keep app metadata in app_info

Design goals:
- Deterministic: the same input DB ends up with the same schema/data
- Idempotent: safe to run multiple times
- Traceable: prints clear progress logs, and stores schema version in DB
"""

from __future__ import annotations

import datetime as _dt
import sqlite3
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple


@dataclass(frozen=True)
class MigrationResult:
    """Result of a schema ensure/upgrade run."""
    db_path: str
    from_version: int
    to_version: int
    applied_versions: Tuple[int, ...]
    tables: Tuple[str, ...]


def _utc_iso_now() -> str:
    """Return a UTC ISO timestamp with seconds precision."""
    return _dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _get_user_version(conn: sqlite3.Connection) -> int:
    cur = conn.execute("PRAGMA user_version")
    row = cur.fetchone()
    return int(row[0]) if row else 0


def _set_user_version(conn: sqlite3.Connection, version: int) -> None:
    conn.execute(f"PRAGMA user_version = {int(version)}")


def _ensure_pragmas(conn: sqlite3.Connection) -> None:
    # Foreign key enforcement must be enabled per connection in SQLite.
    conn.execute("PRAGMA foreign_keys = ON")
    # WAL improves concurrency for typical API workloads (readers + writers).
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")


def _ensure_base_tables(conn: sqlite3.Connection) -> None:
    """
    Ensure metadata tables used by the project exist.

    app_info is retained from the template and used as a stable place to store
    project metadata and schema version (in addition to PRAGMA user_version).
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_info (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            key TEXT UNIQUE NOT NULL,
            value TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """.strip()
    )


def _upsert_app_info(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO app_info (key, value) VALUES (?, ?)",
        (key, value),
    )


def _list_tables(conn: sqlite3.Connection) -> List[str]:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    )
    return [r[0] for r in cur.fetchall()]


def _migration_1_create_habit_tables(conn: sqlite3.Connection) -> None:
    """
    Create core habit tracking tables + indexes.

    Tables:
      - habits: the habit definition
      - habit_completions: day-level completion markers for a habit

    Invariants:
      - habit_completions has unique (habit_id, completed_date)
      - completed_date stored as ISO date 'YYYY-MM-DD' (text) to remain timezone-agnostic
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS habits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            description TEXT,
            color TEXT,
            icon TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            archived INTEGER NOT NULL DEFAULT 0 CHECK (archived IN (0, 1))
        )
        """.strip()
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS habit_completions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            habit_id INTEGER NOT NULL,
            completed_date TEXT NOT NULL, -- ISO date: YYYY-MM-DD
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (habit_id) REFERENCES habits(id) ON DELETE CASCADE,
            UNIQUE (habit_id, completed_date)
        )
        """.strip()
    )

    # Helpful indexes for common queries:
    # - list habits quickly
    # - fetch completions by habit + date range or by date across habits
    conn.execute("CREATE INDEX IF NOT EXISTS idx_habits_archived ON habits(archived)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_habit_completions_habit_date ON habit_completions(habit_id, completed_date)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_habit_completions_date ON habit_completions(completed_date)"
    )


def _migration_2_seed_initial_data(conn: sqlite3.Connection) -> None:
    """
    Seed minimal deterministic data.

    Seeding rules:
    - Only seed if there are zero habits.
    - Use fixed names so seeds are stable across runs.
    """
    cur = conn.execute("SELECT COUNT(1) FROM habits")
    habit_count = int(cur.fetchone()[0])

    if habit_count > 0:
        return

    # Deterministic seed habits.
    seed_habits = [
        ("Drink Water", "Aim for 8 glasses a day", "#3b82f6", "water"),
        ("Walk", "Take a 20 minute walk", "#06b6d4", "walk"),
        ("Read", "Read 10 pages", "#64748b", "book"),
    ]
    for name, description, color, icon in seed_habits:
        conn.execute(
            """
            INSERT INTO habits (name, description, color, icon, created_at, updated_at, archived)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0)
            """.strip(),
            (name, description, color, icon),
        )


MigrationFn = Callable[[sqlite3.Connection], None]

# Deterministic, ordered migrations registry:
# version -> migration
_MIGRATIONS: Dict[int, MigrationFn] = {
    1: _migration_1_create_habit_tables,
    2: _migration_2_seed_initial_data,
}


# PUBLIC_INTERFACE
def get_latest_schema_version() -> int:
    """
    Return the latest schema version supported by this code.

    Returns:
        int: latest version number (monotonically increasing).
    """
    return max(_MIGRATIONS.keys(), default=0)


# PUBLIC_INTERFACE
def ensure_schema(
    *,
    db_path: str,
    project_name: str = "habit-tracker",
    expected_latest_version: Optional[int] = None,
) -> MigrationResult:
    """
    Ensure the SQLite database at `db_path` has the Habit Tracker schema.

    Contract:
      Inputs:
        - db_path: path to SQLite file (will be created if missing)
        - project_name: stored in app_info
        - expected_latest_version: if provided, validates that code supports at least this version
      Outputs:
        - MigrationResult describing applied migrations and final version
      Errors:
        - ValueError if expected_latest_version is greater than supported
        - sqlite3.Error for DB errors (propagated with context at the caller boundary)
      Side effects:
        - Creates/updates SQLite file at db_path
        - Creates/updates tables/indexes
        - Writes app_info keys: project_name, schema_version, last_migrated_at_utc
    """
    latest = get_latest_schema_version()
    if expected_latest_version is not None and expected_latest_version > latest:
        raise ValueError(
            f"Expected schema version {expected_latest_version} is not supported by this code "
            f"(latest supported is {latest})."
        )

    print(f"[ensure_schema] db_path={db_path} latest_supported={latest}")

    conn = sqlite3.connect(db_path)
    try:
        _ensure_pragmas(conn)
        _ensure_base_tables(conn)

        from_version = _get_user_version(conn)
        print(f"[ensure_schema] current_version={from_version}")

        applied: List[int] = []

        # Apply migrations deterministically in version order.
        for version in sorted(_MIGRATIONS.keys()):
            if version <= from_version:
                continue
            print(f"[ensure_schema] applying_migration version={version} ...")
            _MIGRATIONS[version](conn)
            _set_user_version(conn, version)
            applied.append(version)

        to_version = _get_user_version(conn)

        # Keep app_info in sync as a human-readable place for metadata.
        _upsert_app_info(conn, "project_name", project_name)
        _upsert_app_info(conn, "schema_version", str(to_version))
        _upsert_app_info(conn, "last_migrated_at_utc", _utc_iso_now())

        conn.commit()

        tables = tuple(_list_tables(conn))
        print(
            f"[ensure_schema] done from_version={from_version} to_version={to_version} "
            f"applied={applied} tables={tables}"
        )

        return MigrationResult(
            db_path=db_path,
            from_version=from_version,
            to_version=to_version,
            applied_versions=tuple(applied),
            tables=tables,
        )
    finally:
        conn.close()
