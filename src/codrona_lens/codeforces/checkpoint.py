"""Crash-safe progress store.

SQLite rather than a JSON file: writes are atomic, a half-written checkpoint is
impossible, and progress can be queried while the run is still going.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

from __future__ import annotations

import pathlib
import sqlite3
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime

SCHEMA = """
CREATE TABLE IF NOT EXISTS user_progress (
    handle     TEXT PRIMARY KEY,
    band       TEXT NOT NULL,
    rating     INTEGER,
    state      TEXT NOT NULL DEFAULT 'pending',
    rows       INTEGER NOT NULL DEFAULT 0,
    pages      INTEGER NOT NULL DEFAULT 0,
    person_rows INTEGER NOT NULL DEFAULT 0,
    attempts   INTEGER NOT NULL DEFAULT 0,
    error      TEXT,
    updated_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_state ON user_progress(state);
CREATE TABLE IF NOT EXISTS run_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""

PENDING = "pending"
DONE = "done"
SKIPPED = "skipped"
ERROR = "error"


@dataclass(frozen=True)
class PendingUser:
    handle: str
    band: str
    rating: int | None


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


class Checkpoint:
    def __init__(self, path: pathlib.Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(path)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> Checkpoint:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def register_cohort(self, members: list[tuple[str, str, int | None]]) -> int:
        """Insert cohort members, leaving any already-recorded progress intact."""
        cur = self._conn.executemany(
            "INSERT OR IGNORE INTO user_progress(handle, band, rating, state, updated_at) "
            "VALUES (?, ?, ?, 'pending', ?)",
            [(h, b, r, _now()) for h, b, r in members],
        )
        self._conn.commit()
        return cur.rowcount

    def pending(self, *, limit_per_band: int | None = None) -> Iterator[PendingUser]:
        rows = self._conn.execute(
            "SELECT handle, band, rating FROM user_progress "
            "WHERE state = 'pending' ORDER BY band, handle"
        ).fetchall()
        seen: dict[str, int] = {}
        for row in rows:
            band = str(row["band"])
            if limit_per_band is not None and seen.get(band, 0) >= limit_per_band:
                continue
            seen[band] = seen.get(band, 0) + 1
            yield PendingUser(str(row["handle"]), band, row["rating"])

    def mark_done(self, handle: str, *, rows: int, pages: int, person_rows: int) -> None:
        self._conn.execute(
            "UPDATE user_progress SET state='done', rows=?, pages=?, person_rows=?, "
            "error=NULL, updated_at=? WHERE handle=?",
            (rows, pages, person_rows, _now(), handle),
        )
        self._conn.commit()

    def mark_skipped(self, handle: str, reason: str) -> None:
        self._conn.execute(
            "UPDATE user_progress SET state='skipped', error=?, updated_at=? WHERE handle=?",
            (reason[:500], _now(), handle),
        )
        self._conn.commit()

    def mark_error(self, handle: str, reason: str) -> None:
        self._conn.execute(
            "UPDATE user_progress SET state='error', attempts=attempts+1, error=?, "
            "updated_at=? WHERE handle=?",
            (reason[:500], _now(), handle),
        )
        self._conn.commit()

    def reset_errors(self) -> int:
        cur = self._conn.execute("UPDATE user_progress SET state='pending' WHERE state='error'")
        self._conn.commit()
        return cur.rowcount

    def band_summary(self) -> list[sqlite3.Row]:
        return self._conn.execute(
            "SELECT band, "
            "  COUNT(*) AS total, "
            "  SUM(state='done') AS done, "
            "  SUM(state='skipped') AS skipped, "
            "  SUM(state='error') AS errors, "
            "  SUM(rows) AS rows, "
            "  SUM(person_rows) AS person_rows, "
            "  SUM(pages) AS pages "
            "FROM user_progress GROUP BY band ORDER BY band"
        ).fetchall()

    def set_meta(self, key: str, value: str) -> None:
        self._conn.execute(
            "INSERT INTO run_meta(key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        self._conn.commit()
