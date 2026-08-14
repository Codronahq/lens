#!/usr/bin/env python3
"""Prepare an identity-free slice of the warehouse for a Databricks notebook.

WHY A SLICE AND NOT THE WAREHOUSE. Databricks Free Edition is serverless-only
with a per-account quota, and exceeding it shuts the workspace's compute down
for the rest of the day. The notebook's purpose - recomputing the observatory
aggregates on a third engine and comparing them to the committed export - needs
the fact columns those aggregates read and nothing else.

That turns out to be cheap. Six columns - one constant within a year, two dense
small integers and three booleans - compress under parquet plus zstd to
36,512,435 B for all 23,607,105 rows, about 1.55 bytes each, against a
scaled-from-silver estimate of 380 MB because silver carries 22 columns
including handles, verdicts and timestamps. So the default is the whole corpus
rather than one year, and every row of obs_activity_by_year can be reproduced
instead of one.

THE FACT SLICE IS ORDERED, AND THE REASON IS SIZE, NOT REPRODUCIBILITY. Sorting
on the output columns lets zstd exploit runs across six low-cardinality fields.
Measured on the real warehouse, the same rows write 63,045,032 B unordered
against 36,512,435 B ordered - 42% smaller, and 0.4s faster, because the two
dense_rank windows have already sorted the data. That matters because the
destination is a quota-limited Databricks Free workspace. Byte-reproducibility
is a side effect rather than the motive: nothing in this repo consumes a
checksum of this file, and a parquet md5 would move on a DuckDB or zstd upgrade
without the data changing at all.

SINGLE-YEAR MODE REFUSES A PARTIAL YEAR; ALL-YEARS MODE DOES NOT. Collection ran
2026-08-06 to 2026-08-09, so 2026 is right-truncated and its counts are an
artefact of when we stopped. Comparing against that one row alone would prove
nothing. Reproducing the published table as a whole is different: the row is
flagged in the export, and matching a flagged row is still exact agreement,
because the flag governs how a number is read rather than whether it reconciles.

NO IDENTITY COLUMN LEAVES THIS MACHINE. fct_submission keys on user_key, which
is the Codeforces handle, and dim_user is keyed the same way. The aggregates
being reproduced need neither: active_users and problems_attempted are DISTINCT
COUNTS, and a distinct count is invariant under any injective relabelling. So
the slice carries dense integer surrogates assigned by dense_rank, and the
mapping back is never written anywhere.

A hash was the obvious alternative and is worse. The set of Codeforces handles
is public, so a hash with a committed salt is reversible by anyone willing to
run it over the handle list - minimisation dressed as anonymisation. A surrogate
has no preimage to recover.

WHAT AGREEMENT IS THEN POSSIBLE. Exact, on three of the five aggregates:
obs_rating_distribution and obs_country_participation from the user slice, and
the 2025 row of obs_activity_by_year from the fact slice. The remaining two need
problem attributes this slice deliberately does not carry.

Run it:

    python3 scripts/make_databricks_slice.py
    python3 scripts/make_databricks_slice.py --year 2024

SPDX-License-Identifier: AGPL-3.0-or-later
"""

from __future__ import annotations

import argparse
import pathlib
import sys
from typing import Any

import duckdb

from codrona_lens import warehouse

DEFAULT_YEAR = 2025


def default_out() -> pathlib.Path:
    return pathlib.Path.home() / "codrona-data" / "exports" / "databricks"


def write_user_slice(con: duckdb.DuckDBPyConnection, out_dir: pathlib.Path) -> int:
    """Rating, country and registration year. No handle, no name, no city."""
    path = out_dir / "dim_user_slice.parquet"
    con.execute(
        f"""
        copy (
            select
                rating,
                nullif(country, '') as country,
                year(registered_at) as registration_year
            from main_marts.dim_user
            where is_current
            order by rating, country, registration_year
        ) to '{path}' (format parquet, compression zstd)
        """
    )
    counted = con.execute("select count(*) from main_marts.dim_user where is_current").fetchone()
    assert counted is not None
    return int(counted[0])


def write_fact_slice(
    con: duckdb.DuckDBPyConnection, out_dir: pathlib.Path, year: int | None
) -> int:
    """The fact columns the aggregates read, with surrogates for handle and problem id.

    Surrogates are assigned over whatever is selected. Distinct counts are
    invariant under an injective relabelling either way, so per-year and
    whole-corpus surrogates both reproduce active_users exactly.
    """
    label = "all" if year is None else str(year)
    path = out_dir / f"fct_submission_{label}_slice.parquet"
    predicate = "" if year is None else f"where submitted_year = {year}"
    con.execute(
        f"""
        copy (
            select
                submitted_year,
                dense_rank() over (order by user_key) as user_ref,
                dense_rank() over (order by problem_key) as problem_ref,
                is_person_level,
                is_accepted,
                is_contest
            from main_marts.fct_submission
            {predicate}
            order by
                submitted_year, is_person_level, is_accepted,
                is_contest, user_ref, problem_ref
        ) to '{path}' (format parquet, compression zstd)
        """
    )
    sql = "select count(*) from main_marts.fct_submission"
    counted = (
        con.execute(sql).fetchone()
        if year is None
        else con.execute(f"{sql} where submitted_year = ?", [year]).fetchone()
    )
    assert counted is not None
    return int(counted[0])


NAMES = (
    "submitted_year",
    "submissions",
    "person_level_submissions",
    "active_users",
    "problems_attempted",
    "accepted",
    "accepted_pct",
    "in_contest",
    "is_partial_year",
)


def expected_activity_rows(
    con: duckdb.DuckDBPyConnection, year: int | None
) -> list[dict[str, Any]]:
    """The export's own rows, so the notebook has an exact target rather than a vibe."""
    columns = ", ".join(NAMES)
    sql = f"select {columns} from main_marts.obs_activity_by_year"
    rows = (
        con.execute(f"{sql} order by submitted_year").fetchall()
        if year is None
        else con.execute(f"{sql} where submitted_year = ?", [year]).fetchall()
    )
    if not rows:
        raise SystemExit("obs_activity_by_year has no matching row - build the warehouse first")
    return [dict(zip(NAMES, row, strict=True)) for row in rows]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Prepare an identity-free Databricks slice.")
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help=f"a single complete year instead of the whole corpus (e.g. {DEFAULT_YEAR})",
    )
    parser.add_argument("--database", type=pathlib.Path, default=None)
    parser.add_argument("--out", type=pathlib.Path, default=None)
    args = parser.parse_args(argv)

    database = args.database or warehouse.default_database()
    out_dir = args.out or default_out()
    if not database.exists():
        print(f"no warehouse at {database}", file=sys.stderr)
        return 1
    out_dir.mkdir(parents=True, exist_ok=True)

    con = warehouse.connect(database)
    try:
        targets = expected_activity_rows(con, args.year)
        if args.year is not None and targets[0]["is_partial_year"]:
            print(
                f"{args.year} is a partial year - its counts are an artefact of when collection "
                "stopped, so agreement against it proves nothing. Pick a complete year.",
                file=sys.stderr,
            )
            return 1
        users = write_user_slice(con, out_dir)
        facts = write_fact_slice(con, out_dir, args.year)
    finally:
        con.close()

    scope = "whole corpus" if args.year is None else f"year {args.year}"
    print(f"user slice   {users:>10,} rows")
    print(f"fact slice   {facts:>10,} rows  ({scope})")
    for path in sorted(out_dir.iterdir()):
        print(f"  {path.name:<40} {path.stat().st_size / 1_048_576:8.1f} MiB")
    print(f"\nrows the notebook must reproduce exactly ({len(targets)}):")
    shown = NAMES[:6]
    widths = {
        name: max(len(name), *(len(str(target[name])) for target in targets)) + 2 for name in shown
    }
    print("  " + "".join(f"{name:>{widths[name]}}" for name in shown))
    for target in targets:
        print("  " + "".join(f"{target[name]!s:>{widths[name]}}" for name in shown))
    print("\nupload these plus exports/observatory/*.json to a Unity Catalog Volume.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
