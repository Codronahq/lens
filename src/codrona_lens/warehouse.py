"""Warehouse connections, and the session settings that must not be host-dependent.

Every DuckDB connection in this project opens through here, for one reason:
``dim_user.registered_at`` is TIMESTAMP WITH TIME ZONE and ``year()`` resolves
against the session zone, so a laptop in Asia/Kolkata and a container in UTC read
different years from the same row while every count stays identical.

dbt's connections are pinned in ``profiles.yml``. Nothing carries that pin to a
connection opened directly from Python: ``duckdb.connect`` takes its zone from
the host, reads no profile, and until this module existed both direct call sites
- the observatory exporter and the Databricks slice builder - ran unpinned. The
slice builder derives ``registration_year`` on such a connection.

The database path lives here too. It was duplicated in both callers and the two
copies had already diverged: one honoured CODRONA_DUCKDB and the other did not,
which is the defect that made the exporter fail inside the Airflow image against
a home directory holding nothing, immediately after every dbt model had
succeeded against the real warehouse.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

from __future__ import annotations

import os
import pathlib

import duckdb

# Aliased so the signatures below fit one line at any line-length setting.
Connection = duckdb.DuckDBPyConnection

# A mapping rather than a literal, so a second setting is added in one place and
# is asserted by the same gate.
SESSION_SETTINGS: dict[str, str] = {"TimeZone": "UTC"}

DEFAULT_RELATIVE_PATH = ("codrona-data", "warehouse", "codrona.duckdb")


def default_database() -> pathlib.Path:
    """Resolve the warehouse path from the environment, falling back to $HOME.

    CODRONA_DUCKDB wins. The fallback is a convenience for an interactive shell
    on the machine that built the warehouse, and is wrong everywhere else - in a
    container $HOME is the image's service user.
    """
    from_env = os.environ.get("CODRONA_DUCKDB")
    if from_env:
        return pathlib.Path(from_env).expanduser()
    return pathlib.Path.home().joinpath(*DEFAULT_RELATIVE_PATH)


def apply_session_settings(con: Connection) -> None:
    """Pin the session settings on an already-open connection.

    Separate from ``connect`` so a test can hand it a deliberately wrong
    connection and assert this clears it. A test that only opened a fresh
    connection would pass on a UTC host with the pin deleted.
    """
    for name, value in SESSION_SETTINGS.items():
        con.execute(f"SET {name}='{value}'")


def connect(database: pathlib.Path | str, *, read_only: bool = True) -> Connection:
    """Open the warehouse with the session settings already applied."""
    con = duckdb.connect(str(database), read_only=read_only)
    apply_session_settings(con)
    return con


def connect_memory() -> Connection:
    """An in-memory connection, pinned the same way.

    These read Parquet and JSON rather than the warehouse, and none derives a
    date today. They are pinned anyway: the cost is one statement, and the
    alternative is a rule that covers the call sites somebody happened to
    notice, which is how the defect this module exists for survived.
    """
    con = duckdb.connect()
    apply_session_settings(con)
    return con
