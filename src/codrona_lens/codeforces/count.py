"""Count the Codeforces landing zone.

This module settles the public corpus claim (codrona.md §14 Canonical Numbers),
so it is a committed artifact with tests rather than a throwaway script.

It reads the zone with DuckDB and deliberately NOT with Spark. An independent
engine is what makes the global team-dedupe a confirmation rather than an echo
of the same code path, and DuckDB has no hidden-file rule, so a regression in
the Spark filename handling shows up here as a disagreement instead of hiding.

Files are enumerated with ``pathlib.iterdir`` -- never a glob, since the shell
and Hadoop both hide the same leading characters -- and handed to DuckDB as an
explicit list. The run fails if the number of files DuckDB actually read is not
the number found on disk. That comparison is the check whose absence let a 1.4%
sampling bias survive 18 tests, a full-zone run and a green pre-commit sweep.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
from typing import Any, TypeAlias

import duckdb

from codrona_lens import warehouse
from codrona_lens.codeforces.migrate import DEFAULT_USER_STATUS_DIR, SUFFIX

# codrona.md §6: CodeNet person-level submissions, licence verified 2026-08-03.
CODENET_PERSON_LEVEL = 13_916_868

# Mirrors normalize.cf_submissions.DEFAULT_REPORT_DIR. Redeclared rather than
# imported because that module imports pyspark at module scope and this tool
# stays engine-independent on purpose.
DEFAULT_REPORT_DIR = pathlib.Path("~/codrona-data/lake/_reports").expanduser()

Connection: TypeAlias = duckdb.DuckDBPyConnection

HIDDEN_PREFIXES = ("_", ".")
ENCODED_PREFIX = "%"

_STAGE_SQL = """
CREATE TEMP TABLE cf_rows AS
SELECT
    id,
    author.teamId AS team_id,
    len(author.members) AS member_count,
    filename AS src_file
FROM read_json(
    ?,
    format = 'newline_delimited',
    compression = 'gzip',
    columns = {
        id: 'BIGINT',
        author: 'STRUCT(teamId BIGINT, members STRUCT(handle VARCHAR)[])'
    },
    filename = true
)
"""

_TOTALS_SQL = """
SELECT
    count(*) AS raw_rows,
    count(DISTINCT src_file) AS files_read,
    count(DISTINCT id) AS unique_submissions
FROM cf_rows
"""

_PERSON_SQL = """
WITH per_id AS (
    SELECT
        id,
        min(team_id) AS team_id,
        min(member_count) AS member_count
    FROM cf_rows
    GROUP BY id
)
SELECT
    count(*) FILTER (WHERE team_id IS NULL) AS solo,
    count(*) FILTER (WHERE team_id IS NOT NULL) AS team,
    count(*) FILTER (
        WHERE team_id IS NOT NULL AND member_count = 1
    ) AS one_member_team,
    count(*) FILTER (
        WHERE team_id IS NULL OR member_count = 1
    ) AS person_level
FROM per_id
"""


class CountIntegrityError(RuntimeError):
    """Raised when DuckDB did not read every file present on disk."""


def landing_files(user_status_dir: pathlib.Path) -> list[pathlib.Path]:
    """Every landing file, including ones a glob or bare ``ls`` would hide."""
    found: list[pathlib.Path] = []
    for shard in sorted(user_status_dir.iterdir()):
        if not shard.is_dir():
            continue
        for path in sorted(shard.iterdir()):
            if path.name.endswith(SUFFIX):
                found.append(path)
    return found


def hidden_files(files: list[pathlib.Path]) -> list[pathlib.Path]:
    """Files Spark would silently skip. Must be empty after migration."""
    return [p for p in files if p.name.startswith(HIDDEN_PREFIXES)]


def encoded_files(files: list[pathlib.Path]) -> list[pathlib.Path]:
    """Files whose leading character was percent-encoded by the collector."""
    return [p for p in files if p.name.startswith(ENCODED_PREFIX)]


def _connect(memory_limit: str, temp_dir: pathlib.Path | None) -> Connection:
    con = warehouse.connect_memory()
    con.execute(f"SET memory_limit = '{memory_limit}'")
    if temp_dir is not None:
        temp_dir.mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory = '{temp_dir}'")
    return con


def count_corpus(
    user_status_dir: pathlib.Path = DEFAULT_USER_STATUS_DIR,
    memory_limit: str = "4GB",
    temp_dir: pathlib.Path | None = None,
) -> dict[str, Any]:
    """Count the landing zone and return a report dict.

    Raises ``CountIntegrityError`` if any file on disk produced no rows, since
    a file DuckDB never read is a user silently missing from the corpus.
    """
    files = landing_files(user_status_dir)
    if not files:
        raise CountIntegrityError(f"no {SUFFIX} files under {user_status_dir}")

    con = _connect(memory_limit, temp_dir)
    try:
        con.execute(_STAGE_SQL, [[str(p) for p in files]])
        totals = con.execute(_TOTALS_SQL).fetchone()
        person = con.execute(_PERSON_SQL).fetchone()
        seen = con.execute("SELECT DISTINCT src_file FROM cf_rows").fetchall()
        read_back = {row[0] for row in seen}
    finally:
        con.close()

    assert totals is not None
    assert person is not None
    raw_rows, files_read, unique_submissions = totals
    solo, team, one_member_team, person_level = person

    if files_read != len(files):
        missing = sorted(str(p) for p in files if str(p) not in read_back)
        raise CountIntegrityError(
            f"files_on_disk={len(files)} but files_read={files_read}; "
            f"{len(missing)} produced no rows, first: {missing[:10]}"
        )

    return {
        "generated_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
        "input_dir": str(user_status_dir),
        "duckdb_version": duckdb.__version__,
        "files_on_disk": len(files),
        "files_read": files_read,
        "files_hidden_from_spark": len(hidden_files(files)),
        "files_percent_encoded": len(encoded_files(files)),
        "raw_rows": raw_rows,
        "unique_submissions": unique_submissions,
        "duplicate_rows": raw_rows - unique_submissions,
        "solo_submissions": solo,
        "team_submissions": team,
        "one_member_team_submissions": one_member_team,
        "person_level_submissions": person_level,
        "codenet_person_level": CODENET_PERSON_LEVEL,
        "combined_person_level": person_level + CODENET_PERSON_LEVEL,
    }


def write_report(report: dict[str, Any], report_dir: pathlib.Path) -> pathlib.Path:
    """Write the report as dated JSON so the figure carries provenance."""
    report_dir.mkdir(parents=True, exist_ok=True)
    stamp = report["generated_at"].replace("-", "").replace(":", "")[:15]
    path = report_dir / f"cf_corpus_count_{stamp}.json"
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return path


def print_report(report: dict[str, Any]) -> None:
    order = [
        ("files on disk", "files_on_disk"),
        ("files read", "files_read"),
        ("hidden from spark", "files_hidden_from_spark"),
        ("percent-encoded", "files_percent_encoded"),
        ("raw rows", "raw_rows"),
        ("unique submissions", "unique_submissions"),
        ("duplicate rows", "duplicate_rows"),
        ("solo submissions", "solo_submissions"),
        ("team submissions", "team_submissions"),
        ("one-member teams", "one_member_team_submissions"),
        ("person-level", "person_level_submissions"),
        ("codenet person-level", "codenet_person_level"),
        ("combined person-level", "combined_person_level"),
    ]
    for label, key in order:
        print(f"{label:<22} {report[key]:>15,}")


def build_parser() -> argparse.ArgumentParser:
    zone = DEFAULT_USER_STATUS_DIR
    reports = DEFAULT_REPORT_DIR
    parser = argparse.ArgumentParser(description="Count the Codeforces landing zone.")
    parser.add_argument("--user-status-dir", type=pathlib.Path, default=zone)
    parser.add_argument("--report-dir", type=pathlib.Path, default=reports)
    parser.add_argument("--memory-limit", default="4GB")
    parser.add_argument("--temp-dir", type=pathlib.Path, default=None)
    parser.add_argument("--no-report", action="store_true", help="print only")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = count_corpus(
        user_status_dir=args.user_status_dir,
        memory_limit=args.memory_limit,
        temp_dir=args.temp_dir,
    )
    print_report(report)
    if not args.no_report:
        path = write_report(report, args.report_dir)
        print(f"\nreport {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
