"""Serialise the observatory aggregates to committed JSON, and verify what was written.

WHY THIS EXISTS. The five ``obs_*`` tables live in a DuckDB file under
``~/codrona-data/``, which is never committed. Nothing off this laptop can read
them - not a notebook, not Databricks, not ``apps/web``. Every remaining Phase 1
item sits behind that, so the aggregates need a form that survives leaving the
machine. Static JSON is the Tier 0 answer from codrona.md section 7: a file
cannot pause, works offline, and needs no service on the hot path.

THE COLUMN SET IS AN ALLOWLIST AND A MISMATCH IS FATAL. codrona.md section 6
places the privacy control at the publication boundary rather than at the load
step, and requires an explicit allowlist so that a column added later is
admitted deliberately instead of shipped by accident. A deny-list would ship
every new column and only stop the ones somebody thought to name. So the export
compares the warehouse's columns against ``ALLOWLIST`` and refuses to write when
they differ in either direction: an unexpected column raises rather than being
dropped quietly, because dropping it quietly is how the next identifying field
reaches a chart with nobody deciding anything.

THE CAVEATS COME FROM THE dbt SCHEMA, NOT FROM THIS FILE. Every one of these
tables is misreadable in a specific way - the activity curve is left-truncated
and is not growth, country slices describe a self-selected stronger
subpopulation, cohort counts are stratified and are never platform totals. Those
warnings are already written in ``models/marts/observatory/_schema.yml``. Copying
them here would create a second copy to drift, so the export reads them and
fails if a table has no description. A figure that travels without its caveat is
how an honest number becomes a false claim.

THERE IS NO TIMESTAMP IN THE OUTPUT. A wall-clock field would make every
regeneration a diff, which destroys the one property a committed artefact has:
that regenerating it and finding no change means nothing moved. Provenance comes
from git and from the pins already carried in the data.

Run it:

    python3 -m codrona_lens.observatory.export
    python3 -m codrona_lens.observatory.export --database target/ci.duckdb \\
        --out target/export-ci --check

SPDX-License-Identifier: AGPL-3.0-or-later
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
import tempfile
from decimal import Decimal
from typing import Any

import duckdb
import yaml

SCHEMA_NAME = "main_marts"

# Mirrors the dbt var of the same name. Restated rather than shared, so the
# artefact gate and the warehouse gate cannot be silenced by one edit.
MIN_COHORT_USERS = 5

# Every column published, per table, in output order. Compared against the
# warehouse in both directions: a missing column and an unexpected one are both
# errors, and neither is recoverable by guessing.
ALLOWLIST: dict[str, tuple[str, ...]] = {
    "obs_rating_distribution": (
        "rating_band",
        "band_order",
        "cohort_users",
        "cohort_share_pct",
        "min_rating",
        "max_rating",
        "mean_rating",
    ),
    "obs_activity_by_year": (
        "submitted_year",
        "submissions",
        "person_level_submissions",
        "active_users",
        "problems_attempted",
        "accepted",
        "accepted_pct",
        "in_contest",
        "is_partial_year",
        "registered_by_then",
        "active_share_pct",
    ),
    "obs_tag_landscape": (
        "tag",
        "problems_with_tag",
        "rated_problems",
        "mean_rating",
        "median_rating",
        "min_rating",
        "max_rating",
        "median_solved_count",
        "from_problemset",
        "from_tiebreak",
    ),
    "obs_country_participation": (
        "country",
        "is_undeclared",
        "cohort_users",
        "cohort_share_pct",
        "mean_rating",
        "median_rating",
        "max_rating",
        "candidate_master_plus",
        "is_reportable",
    ),
    "obs_organization_participation": (
        "organization",
        "cohort_users",
        "mean_rating",
        "median_rating",
        "max_rating",
        "candidate_master_plus",
        "most_common_country",
        "is_reportable",
    ),
}

# Deterministic output order, so regeneration produces byte-identical files.
ORDER_BY: dict[str, str] = {
    "obs_rating_distribution": "band_order",
    "obs_activity_by_year": "submitted_year",
    "obs_tag_landscape": "problems_with_tag desc, tag",
    "obs_country_participation": "cohort_users desc, country",
    "obs_organization_participation": "is_reportable desc, cohort_users desc, organization",
}

# Restated here on purpose. The warehouse guard reads information_schema; this
# reads the shipped file. Two independent statements of one rule is the point -
# a single shared constant would let one edit silence both.
IDENTIFYING_KEYS = frozenset(
    {
        "user_key",
        "user_sk",
        "handle",
        "collected_via_handle",
        "author_handles",
        "first_name",
        "last_name",
        "city",
        "avatar",
        "title_photo",
        "problem_name",
        "problem_title",
        "statement",
    }
)

# Tables whose rows count people, and the column that counts them. Rows below
# the threshold must publish no statistics; see obs_country_participation and
# obs_organization_participation for why the two suppress differently.
PERSON_COUNT_COLUMN: dict[str, str] = {
    "obs_country_participation": "cohort_users",
    "obs_organization_participation": "cohort_users",
}

# Statistic columns that characterise the people in a cell rather than count them.
STATISTIC_KEYS = frozenset(
    {"mean_rating", "median_rating", "max_rating", "candidate_master_plus", "most_common_country"}
)

# The two tables that partition the same population. Their user counts must
# agree; an invariant beats a pinned number, which would go stale on collection 2.
PARTITION_TABLES = ("obs_rating_distribution", "obs_country_participation")


def default_database() -> pathlib.Path:
    return pathlib.Path.home() / "codrona-data" / "warehouse" / "codrona.duckdb"


def repo_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[3]


def load_descriptions(schema_path: pathlib.Path) -> dict[str, str]:
    """Read each model's description from the dbt schema file.

    The caveats live there already. Reading them keeps one copy; a table with no
    description is a table whose warnings would ship blank, so it raises.
    """
    parsed: Any = yaml.safe_load(schema_path.read_text(encoding="utf-8"))
    out: dict[str, str] = {}
    for model in parsed.get("models", []):
        description = " ".join(str(model.get("description", "")).split())
        if description:
            out[str(model["name"])] = description
    missing = sorted(set(ALLOWLIST) - set(out))
    if missing:
        raise SystemExit(f"no description in {schema_path} for: {', '.join(missing)}")
    return out


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    return value


def read_table(con: duckdb.DuckDBPyConnection, table: str) -> list[dict[str, Any]]:
    """Read one table, refusing to proceed if its columns are not exactly the allowlist."""
    expected = ALLOWLIST[table]
    found = tuple(
        row[0]
        for row in con.execute(
            "select column_name from information_schema.columns "
            "where table_schema = ? and table_name = ? order by ordinal_position",
            [SCHEMA_NAME, table],
        ).fetchall()
    )
    if not found:
        raise SystemExit(f"{SCHEMA_NAME}.{table} does not exist - build the warehouse first")
    unexpected = sorted(set(found) - set(expected))
    absent = sorted(set(expected) - set(found))
    if unexpected or absent:
        raise SystemExit(
            f"{table}: column set differs from the allowlist. "
            f"unexpected={unexpected or 'none'} missing={absent or 'none'}. "
            "Publishing a column is a decision - edit ALLOWLIST deliberately."
        )
    columns = ", ".join(expected)
    rows = con.execute(
        f"select {columns} from {SCHEMA_NAME}.{table} order by {ORDER_BY[table]}"
    ).fetchall()
    return [
        {name: _jsonable(value) for name, value in zip(expected, row, strict=True)} for row in rows
    ]


def build_payload(table: str, rows: list[dict[str, Any]], caveat: str) -> dict[str, Any]:
    return {
        "table": table,
        "caveat": caveat,
        "columns": list(ALLOWLIST[table]),
        "row_count": len(rows),
        "rows": rows,
    }


def write_export(database: pathlib.Path, out_dir: pathlib.Path, schema_path: pathlib.Path) -> int:
    descriptions = load_descriptions(schema_path)
    out_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(database), read_only=True)
    try:
        written: dict[str, int] = {}
        for table in ALLOWLIST:
            rows = read_table(con, table)
            payload = build_payload(table, rows, descriptions[table])
            path = out_dir / f"{table}.json"
            path.write_text(
                json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=False) + "\n",
                encoding="utf-8",
            )
            written[table] = len(rows)
    finally:
        con.close()

    manifest = {
        "files": [f"{table}.json" for table in ALLOWLIST],
        "row_counts": written,
        "min_cohort_users": MIN_COHORT_USERS,
        "note": (
            "Aggregates only. Cohort counts are stratified and are never platform totals. "
            "Cells describing fewer than min_cohort_users people publish no statistics."
        ),
    }
    (out_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    return sum(written.values())


def verify_export(out_dir: pathlib.Path) -> list[str]:
    """Check a written export against the publication rules, reading only the files.

    Deliberately independent of the SQL that produced them: the warehouse tests
    gate the tables, and this gates the artefact that leaves the machine. A gate
    that trusts the producer is not a gate.
    """
    problems: list[str] = []
    manifest_path = out_dir / "manifest.json"
    if not manifest_path.exists():
        return [f"{out_dir}: no manifest.json - export not written"]

    totals: dict[str, int] = {}
    for table in ALLOWLIST:
        path = out_dir / f"{table}.json"
        if not path.exists():
            problems.append(f"{table}: file missing")
            continue
        payload: Any = json.loads(path.read_text(encoding="utf-8"))
        rows: list[dict[str, Any]] = payload.get("rows", [])

        if not str(payload.get("caveat", "")).strip():
            problems.append(f"{table}: empty caveat - a figure must travel with its warning")
        if payload.get("row_count") != len(rows):
            problems.append(f"{table}: row_count {payload.get('row_count')} != {len(rows)} rows")
        if payload.get("columns") != list(ALLOWLIST[table]):
            problems.append(f"{table}: columns differ from the allowlist")

        for index, row in enumerate(rows):
            leaked = sorted(set(row) & IDENTIFYING_KEYS)
            if leaked:
                problems.append(f"{table} row {index}: identifying key(s) {leaked}")
            if set(row) != set(ALLOWLIST[table]):
                problems.append(f"{table} row {index}: keys differ from the allowlist")

        count_column = PERSON_COUNT_COLUMN.get(table)
        if count_column:
            totals[table] = sum(int(row.get(count_column, 0)) for row in rows)
            for index, row in enumerate(rows):
                people = row.get(count_column)
                if not isinstance(people, int) or people >= MIN_COHORT_USERS:
                    continue
                published = sorted(
                    key for key in STATISTIC_KEYS & set(row) if row.get(key) is not None
                )
                if published:
                    problems.append(
                        f"{table} row {index}: {people} people but publishes {published}"
                    )

    partition_counts = {
        table: sum(
            int(row.get("cohort_users", 0))
            for row in json.loads((out_dir / f"{table}.json").read_text(encoding="utf-8"))["rows"]
        )
        for table in PARTITION_TABLES
        if (out_dir / f"{table}.json").exists()
    }
    if len(partition_counts) == len(PARTITION_TABLES) and len(set(partition_counts.values())) != 1:
        problems.append(f"population disagreement across tables: {partition_counts}")

    return problems


def compare_current(
    database: pathlib.Path, out_dir: pathlib.Path, schema_path: pathlib.Path
) -> list[str]:
    """Regenerate into a scratch directory and report files that differ.

    The gate in verify_export proves the committed files are internally
    consistent and carry nothing identifying. It cannot prove they still match
    the warehouse: edit a model, rebuild, forget to re-export, and every test
    stays green while the artefact describes the previous warehouse. This is the
    comparison that catches that, and it is only possible where the warehouse
    exists - which is why the caller treats an absent one as unverifiable rather
    than as a pass or a failure.
    """
    with tempfile.TemporaryDirectory() as scratch:
        fresh = pathlib.Path(scratch)
        write_export(database, fresh, schema_path)
        names = [f"{table}.json" for table in ALLOWLIST] + ["manifest.json"]
        differing: list[str] = []
        for name in names:
            committed = out_dir / name
            if not committed.exists():
                differing.append(f"{name}: missing from {out_dir}")
            elif committed.read_bytes() != (fresh / name).read_bytes():
                differing.append(f"{name}: differs from the warehouse")
    return differing


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export the observatory aggregates to JSON.")
    parser.add_argument("--database", type=pathlib.Path, default=None)
    parser.add_argument("--out", type=pathlib.Path, default=None)
    parser.add_argument("--schema", type=pathlib.Path, default=None)
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify the export after writing it, and fail on any problem",
    )
    parser.add_argument(
        "--verify-current",
        action="store_true",
        help="do not write; fail if the committed export differs from the warehouse",
    )
    args = parser.parse_args(argv)

    root = repo_root()
    database = args.database or default_database()
    out_dir = args.out or root / "exports" / "observatory"
    schema_path = args.schema or root / "models" / "marts" / "observatory" / "_schema.yml"

    if args.verify_current:
        if not database.exists():
            # A contributor without the warehouse genuinely cannot regenerate
            # this, so blocking their commit would be wrong. Said out loud
            # rather than passing quietly: this is a maintainer gate.
            print(f"no warehouse at {database} - export freshness NOT verified")
            return 0
        differing = compare_current(database, out_dir, schema_path)
        if differing:
            print(f"{len(differing)} file(s) stale:", file=sys.stderr)
            for name in differing:
                print(f"  {name}", file=sys.stderr)
            print(
                "run: python3 -m codrona_lens.observatory.export --check",
                file=sys.stderr,
            )
            return 1
        print("export is current with the warehouse")
        return 0

    if not database.exists():
        print(f"no warehouse at {database}", file=sys.stderr)
        return 1

    total = write_export(database, out_dir, schema_path)
    print(f"wrote {len(ALLOWLIST)} tables, {total} rows, to {out_dir}")

    if args.check:
        problems = verify_export(out_dir)
        if problems:
            print(f"{len(problems)} problem(s):", file=sys.stderr)
            for problem in problems:
                print(f"  {problem}", file=sys.stderr)
            return 1
        print("export verified: allowlist honoured, no small-cell statistics, population agrees")
    return 0


if __name__ == "__main__":
    sys.exit(main())
