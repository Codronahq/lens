"""Build the IRT response matrix from the warehouse, and gate it with G12.

WHY THIS EXISTS. Item response theory takes one response per person per item.
The warehouse holds submissions and a person submits to one problem many times,
so the collapse is a decision rather than a formality:
``docs/analysis/irt-response-definition.md`` settles it as the first
person-level evidence submission on a (user, problem) pair, ordered by
``submission_key``, valued by ``is_accepted``. Ordering by ``submitted_at``
instead is not merely different - it is non-deterministic, because 332 timestamp
groups tie and ``arg_min`` over them is unstable between runs.

THE TWIN MERGE HAPPENS HERE, BEFORE ANY PARAMETER IS ESTIMATED. Fitting first
and merging later invalidates every parameter, and a wrong merge is undetectable
afterwards because pooled responses carry no record of having come from two
problems. So the emitted matrix carries ``source_problem_key`` alongside the
merged ``problem_key``: the merge is reversible from the artefact itself rather
than only from the code that produced it, which is the property the twins
document argued for and which a boolean flag would not deliver.

G12 IS SPLIT IN TWO, AND THAT SPLIT IS THE POINT. The blueprint specifies six
assertions carrying measured expected values. Pinned unconditionally they cannot
run in CI, which builds synthetic fixtures - the same wall G8 already meets, and
which it answers with the ``real_data`` dbt tag. So the structural rules run
against any dataset and the pinned counts run only under ``--real-data``. A gate
that can execute on one laptop is not in CI, and a gate CI skips entirely is not
a gate; this is the honest boundary between them, stated rather than implied.

THE RECONCILIATION PAIR IS WHAT CATCHES A NAIVE REMAP. A merge must move rows
between keys and never remove any, so responses fall by exactly the duplicate
count while attempts stay identical. A remap that relabels without collapsing
first attempts passes the attempt assertion and fails the response one; a remap
that drops rows does the reverse. Neither is caught by either count alone.

``problem_contest_id`` IS CARRIED BECAUSE THE DIFFICULTY PRIOR NEEDS AUDITING.
Stage A draws difficulty from a prior on ``problem_rating`` and
``log(solved_count)``. Measured 16 Aug 2026 over the 11,764-problem bank, with
rating removed as a factor, ``ln(solved_count)`` still correlates with contest
id at 0.4444 in 1200-1599 and 0.4353 in 1600-1999, and the sign crosses zero
between rating 2700 and 2800. Contest id is a near-perfect stand-in for
publication order - it agrees with the earliest observed submission at 0.988 -
so the covariate carries release date as well as difficulty. Whether that is
contamination or genuine drift in Codeforces' own calibration is a question only
the fitted difficulty can answer, and it cannot be asked at all unless the
artefact carries the date. It is carried, never filtered on: §14 makes the
artefact the boundary between repos, so a consumer that had to join
``dim_problem`` at fit time would be reading the warehouse from outside ``lens``.

THE MATRIX IS NOT BANK-FILTERED. ``in_public_problemset`` is a modelling scope
applied by Stage A, not a property of the response unit, and it is carried as a
column so the filter happens where it is decided. Filtering here would bake a
Stage A choice into the artefact every later stage reads.

THERE ARE THREE ARTEFACTS AND TWO LINKS BETWEEN THEM. The warehouse, the
committed manifest, and the Parquet Stage A reads. ``--verify-current`` rebuilds
from the warehouse and closes both links in one pass, at the cost of a full scan;
it is the maintainer command, run before a fit and before a release.
``--verify-artefact`` closes only the manifest-to-Parquet link, from the Parquet
footer and a JSON file, with no warehouse and no scan - cheap enough to hook.
Neither subsumes the other and running one is not running the other.

Run it:

    python3 -m codrona_lens.responses.matrix --real-data
    python3 -m codrona_lens.responses.matrix --verify-current
    python3 -m codrona_lens.responses.matrix --verify-artefact
    python3 -m codrona_lens.responses.matrix --database target/ci.duckdb \\
        --out target/responses.parquet

SPDX-License-Identifier: AGPL-3.0-or-later
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
from dataclasses import dataclass
from typing import Any

import duckdb

from codrona_lens import warehouse
from codrona_lens.responses import twins

SCHEMA_NAME = "main_marts"

# Measured 2026-08-15 over the 2026-08-06 collection snapshot. Every figure here
# is a real-data expectation and none is checked unless --real-data is passed.
# Sources: docs/analysis/irt-response-definition.md and
# docs/architecture/phase-2-modelling.md section 6.
REAL_DATA_COUNTS: dict[str, int] = {
    "fact_rows": 23_607_105,
    "distinct_submission_keys": 23_607_105,
    "attempts": 22_843_153,
    "unmerged_responses": 11_176_774,
    "merged_responses": 11_158_572,
    "merged_keys": 1_182,
    "twin_gap_matches": 1_183,
    "twin_qualifying": 1_183,
    # Populations the committed rule excludes. Pinned so a change surfaces as a
    # red gate rather than as a quietly different item bank. The first is the
    # one nobody had counted: 43 gap-1 mainline pairs where the PUBLISHED side
    # carries the higher contest id, which the directional query cannot see.
    "twin_reversed_name_matches": 74,
    "twin_reversed_pairs": 43,
    "twin_both_published": 4,
    "twin_both_published_passing_rating": 1,
    "twin_gym_pairs": 606,
}

_EVIDENCE = """
select user_key, problem_key, submission_key, is_accepted,
       participant_type, submitted_at
from {schema}.fct_submission
where is_evidence and is_person_level
"""

_RESPONSES = """
with evidence as ({evidence}),
remapped as (
    select e.user_key,
           e.submission_key,
           e.is_accepted,
           e.participant_type,
           e.submitted_at,
           e.problem_key as source_problem_key,
           coalesce(m.present_key, e.problem_key) as problem_key
    from evidence e
    left join twin_map m on m.absent_key = e.problem_key
),
ranked as (
    select r.*,
           row_number() over (
               partition by r.user_key, r.problem_key
               order by r.submission_key
           ) as rn,
           count(*) over (
               partition by r.user_key, r.problem_key
           ) as attempts,
           max(r.is_accepted::int) over (
               partition by r.user_key, r.problem_key
           ) as ever_ok
    from remapped r
)
select ranked.user_key,
       ranked.problem_key,
       ranked.source_problem_key,
       ranked.submission_key,
       ranked.is_accepted,
       ranked.participant_type,
       ranked.submitted_at,
       ranked.attempts,
       ranked.ever_ok = 1 as ever_accepted,
       problems.in_public_problemset,
       problems.problem_rating,
       problems.solved_count,
       problems.problem_contest_id
from ranked
join {schema}.dim_problem as problems
  on problems.problem_key = ranked.problem_key
 and problems.is_current
where ranked.rn = 1
"""


@dataclass(frozen=True)
class BuildReport:
    """Counts a build must produce, each paired with something it must match."""

    fact_rows: int
    distinct_submission_keys: int
    attempts: int
    unmerged_responses: int
    merged_responses: int
    merged_attempts: int
    merged_keys: int
    twin: twins.TwinMap
    bank_responses: int
    bank_accepted: int
    # (name, DuckDB type) in emission order, read back from the engine rather
    # than restated here, so the manifest describes what the query produced and
    # not what somebody believed it produced. No default: an empty schema in a
    # manifest is a gate that cannot fail, and a default is how one gets there.
    columns: tuple[tuple[str, str], ...]

    @property
    def collapsed(self) -> int:
        return self.unmerged_responses - self.merged_responses


def _scalar(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    row = con.execute(sql).fetchone()
    if row is None or row[0] is None:
        raise SystemExit(f"query returned no value: {sql}")
    return int(row[0])


def _register_twin_map(con: duckdb.DuckDBPyConnection, twin: twins.TwinMap) -> None:
    con.execute("create or replace temp table twin_map (absent_key varchar, present_key varchar)")
    if twin.mapping:
        con.executemany(
            "insert into twin_map values (?, ?)",
            sorted(twin.mapping.items()),
        )


def build(
    con: duckdb.DuckDBPyConnection,
    out_path: pathlib.Path | None,
    *,
    schema: str = SCHEMA_NAME,
) -> BuildReport:
    """Derive the twin map, build the response matrix, and measure both sides."""
    twin = twins.derive(con, schema=schema)
    _register_twin_map(con, twin)

    evidence = _EVIDENCE.format(schema=schema)
    responses = _RESPONSES.format(evidence=evidence, schema=schema)

    fact_rows = _scalar(con, f"select count(*) from {schema}.fct_submission")
    distinct_keys = _scalar(
        con, f"select count(distinct submission_key) from {schema}.fct_submission"
    )
    attempts = _scalar(con, f"select count(*) from ({evidence})")
    unmerged = _scalar(
        con,
        f"select count(*) from (select distinct user_key, problem_key from ({evidence}))",
    )

    con.execute(f"create or replace temp view response_matrix as {responses}")
    columns = tuple(
        (str(row[0]), str(row[1]))
        for row in con.execute("describe select * from response_matrix").fetchall()
    )
    merged = _scalar(con, "select count(*) from response_matrix")
    merged_attempts = _scalar(con, "select sum(attempts) from response_matrix")
    # Absent keys carrying at least one evidence row - NOT keys supplying a
    # surviving first attempt. Two absent keys have evidence yet never win the
    # ordering, so the first-attempt reading undercounts and does not mean
    # "keys actually merged".
    merged_keys = _scalar(
        con,
        "select count(distinct evidence.problem_key) from ("
        f"{evidence}) as evidence "
        "join twin_map on twin_map.absent_key = evidence.problem_key",
    )
    bank_responses = _scalar(con, "select count(*) from response_matrix where in_public_problemset")
    bank_accepted = _scalar(
        con,
        "select count(*) from response_matrix where in_public_problemset and is_accepted",
    )

    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        # ORDER BY is not decoration: without one a COPY is not byte-reproducible,
        # because the parallel writer emits rows in whatever order it finishes.
        con.execute(
            f"copy (select * from response_matrix order by user_key, problem_key) "
            f"to '{out_path}' (format parquet)"
        )

    return BuildReport(
        fact_rows=fact_rows,
        distinct_submission_keys=distinct_keys,
        attempts=attempts,
        unmerged_responses=unmerged,
        merged_responses=merged,
        merged_attempts=merged_attempts,
        merged_keys=merged_keys,
        twin=twin,
        bank_responses=bank_responses,
        bank_accepted=bank_accepted,
        columns=columns,
    )


def check_invariants(report: BuildReport) -> list[str]:
    """G12's structural half. Holds on any dataset, fixtures included."""
    problems: list[str] = []

    if report.distinct_submission_keys != report.fact_rows:
        problems.append(
            f"ordering key not unique: {report.fact_rows} rows carry "
            f"{report.distinct_submission_keys} distinct submission_key values. "
            "row_number() over it is then not a total order."
        )
    if report.merged_attempts != report.attempts:
        problems.append(
            f"merge changed the attempt count: {report.attempts} evidence rows "
            f"became {report.merged_attempts}. A merge moves rows between keys "
            "and must never remove or duplicate any."
        )
    if report.merged_responses > report.unmerged_responses:
        problems.append(
            f"merge grew the response count: {report.unmerged_responses} -> "
            f"{report.merged_responses}. Collapsing pairs cannot add responses."
        )
    if report.merged_keys > len(report.twin.mapping):
        problems.append(
            f"{report.merged_keys} keys were remapped but the twin map holds "
            f"{len(report.twin.mapping)}. A response was routed to a key the map "
            "does not name."
        )
    if report.twin.exactly_one_unrated or report.twin.rating_differs:
        problems.append(
            f"twin rule now filters pairs it did not before: "
            f"{report.twin.exactly_one_unrated} with exactly one side unrated, "
            f"{report.twin.rating_differs} with ratings differing. Both measured "
            "as zero; a nonzero count changes the item bank and needs a decision."
        )
    if report.twin.qualifying != report.twin.gap_matches:
        problems.append(
            f"rating clause now excludes pairs: {report.twin.gap_matches} gap-1 "
            f"name matches, {report.twin.qualifying} qualifying."
        )
    return problems


def check_real_data(report: BuildReport) -> list[str]:
    """G12's pinned half. Runs only against the real warehouse."""
    measured = {
        "fact_rows": report.fact_rows,
        "distinct_submission_keys": report.distinct_submission_keys,
        "attempts": report.attempts,
        "unmerged_responses": report.unmerged_responses,
        "merged_responses": report.merged_responses,
        "merged_keys": report.merged_keys,
        "twin_gap_matches": report.twin.gap_matches,
        "twin_qualifying": report.twin.qualifying,
        "twin_reversed_name_matches": report.twin.audit.reversed_name_matches,
        "twin_reversed_pairs": len(report.twin.audit.reversed_pairs),
        "twin_both_published": len(report.twin.audit.both_published),
        "twin_both_published_passing_rating": len(report.twin.audit.both_published_passing_rating),
        "twin_gym_pairs": report.twin.audit.gym_pairs,
    }
    return [
        f"{name}: measured {measured[name]:,}, expected {expected:,}"
        for name, expected in REAL_DATA_COUNTS.items()
        if measured[name] != expected
    ]


MANIFEST_NOTE = (
    "Counts and shape describing responses.parquet, which is hundreds of "
    "megabytes and lives under ~/codrona-data/ rather than in the repository. "
    "This file is the committed half: it is what a reviewer can read and what "
    "--verify-current compares a fresh build against. The schema block exists "
    "because counts alone cannot fail on a column added, dropped, renamed, "
    "retyped or reordered - every count stays identical through all five. No "
    "timestamp and no byte size, because a field that moves on its own would "
    "make every regeneration a diff and destroy the one property a committed "
    "artefact has - that regenerating it and finding no change means nothing "
    "moved."
)


def build_manifest(report: BuildReport) -> dict[str, Any]:
    """Everything gated, as integers plus two figures derived from them.

    The rates are computed here from two integer counts rather than read back
    from the engine, so the manifest cannot drift from the counts beside it.
    """
    rate = report.bank_accepted / report.bank_responses
    return {
        "artefact": "responses.parquet",
        "note": MANIFEST_NOTE,
        "schema": [{"name": name, "type": kind} for name, kind in report.columns],
        "counts": {
            "fact_rows": report.fact_rows,
            "distinct_submission_keys": report.distinct_submission_keys,
            "attempts": report.attempts,
            "unmerged_responses": report.unmerged_responses,
            "merged_responses": report.merged_responses,
            "merged_attempts": report.merged_attempts,
            "collapsed_duplicates": report.collapsed,
            "merged_keys": report.merged_keys,
            "twin_map_entries": len(report.twin.mapping),
            "bank_responses": report.bank_responses,
            "bank_accepted": report.bank_accepted,
        },
        "twin_rule": {
            "gap_matches": report.twin.gap_matches,
            "qualifying": report.twin.qualifying,
            "rating_agree": report.twin.rating_agree,
            "both_unrated": report.twin.both_unrated,
            "exactly_one_unrated": report.twin.exactly_one_unrated,
            "rating_differs": report.twin.rating_differs,
        },
        "twin_excluded": {
            "reversed_name_matches": report.twin.audit.reversed_name_matches,
            "reversed_passing_rating": len(report.twin.audit.reversed_pairs),
            "both_published": len(report.twin.audit.both_published),
            "both_published_passing_rating": len(report.twin.audit.both_published_passing_rating),
            "gym_pairs": report.twin.audit.gym_pairs,
        },
        "derived": {
            "bank_base_rate_pct": round(100.0 * rate, 4),
            "bank_baseline_brier": round(rate * (1.0 - rate), 6),
        },
    }


def write_manifest(report: BuildReport, path: pathlib.Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(build_manifest(report), indent=2, ensure_ascii=False)
    path.write_text(payload + "\n", encoding="utf-8")


def manifest_schema(manifest: dict[str, Any]) -> list[tuple[str, str]]:
    """The schema block as ordered pairs. Absent reads as absent, not as empty."""
    block = manifest.get("schema")
    if not isinstance(block, list):
        return []
    return [(str(entry["name"]), str(entry["type"])) for entry in block]


def compare_schema(
    expected: list[tuple[str, str]],
    actual: list[tuple[str, str]],
    *,
    expected_label: str,
    actual_label: str,
) -> list[str]:
    """Name the difference rather than print two lists and leave it to a reader.

    Order is compared, not just membership. A reordering leaves every count
    identical and every column present, and it is the one shape change a reader
    scanning a diff is most likely to wave through.
    """
    if expected == actual:
        return []
    if not expected:
        return [f"{expected_label} carries no schema block - regenerate the manifest"]
    problems: list[str] = []
    expected_types = dict(expected)
    actual_types = dict(actual)
    for name in expected_types:
        if name not in actual_types:
            problems.append(f"column {name}: in {expected_label}, absent from {actual_label}")
    for name in actual_types:
        if name not in expected_types:
            problems.append(f"column {name}: in {actual_label}, absent from {expected_label}")
    for name, kind in expected_types.items():
        other = actual_types.get(name)
        if other is not None and other != kind:
            problems.append(f"column {name} type: {expected_label} {kind}, {actual_label} {other}")
    if not problems:
        problems.append(
            f"column ORDER differs: {expected_label} "
            f"{[name for name, _ in expected]}, {actual_label} "
            f"{[name for name, _ in actual]}"
        )
    return problems


def compare_manifest(report: BuildReport, path: pathlib.Path) -> list[str]:
    """Report every count or column where a fresh build disagrees with the file.

    THIS CLOSES ONE LINK AND NOT THE OTHER, WHICH IS WHY ``verify_artefact``
    EXISTS BESIDE IT. There are three things in play - the warehouse, the
    committed manifest, and the Parquet Stage A actually reads - and this
    function compares the first two. It never opens the artefact. So a build
    whose write failed, or an artefact deleted, truncated or produced by some
    other code path, passes here with nothing to show for it. The docstring this
    replaces claimed to catch that and could not.

    IT IS DELIBERATELY NOT A PRE-COMMIT HOOK. Regenerating the observatory means
    five small aggregates; this means a full scan of 23.6 million fact rows with
    two window functions. A gate slow enough to be skipped is a gate that will be
    skipped, so this is a maintainer command run before a fit and before a
    release, and that limit is stated rather than hidden behind a green hook.
    """
    if not path.exists():
        return [f"{path}: no committed manifest - run --check to write one"]
    committed: Any = json.loads(path.read_text(encoding="utf-8"))
    fresh = build_manifest(report)
    problems: list[str] = []
    for section in ("counts", "twin_rule", "twin_excluded", "derived"):
        for name, value in fresh[section].items():
            was = committed.get(section, {}).get(name)
            if was != value:
                problems.append(f"{section}.{name}: committed {was}, measured {value}")
    problems += compare_schema(
        manifest_schema(committed),
        manifest_schema(fresh),
        expected_label="committed",
        actual_label="measured",
    )
    return problems


def read_artefact(path: pathlib.Path) -> tuple[int, list[tuple[str, str]]]:
    """Row count and schema of a Parquet file, from its footer.

    Both come out of Parquet metadata rather than a scan, which is what makes
    this cheap enough to run as a hook while the warehouse rebuild - a full pass
    over 23.6 million fact rows with two window functions - stays a maintainer
    command. ``connect_memory`` rather than ``duckdb.connect`` because G10's
    structural test forbids a second route into DuckDB anywhere under ``src``.
    """
    con = warehouse.connect_memory()
    try:
        row = con.execute("select count(*) from read_parquet(?)", [str(path)]).fetchone()
        if row is None or row[0] is None:
            raise SystemExit(f"could not count rows in {path}")
        described = con.execute("describe select * from read_parquet(?)", [str(path)]).fetchall()
    finally:
        con.close()
    return int(row[0]), [(str(entry[0]), str(entry[1])) for entry in described]


def verify_artefact(path: pathlib.Path, manifest: dict[str, Any]) -> list[str]:
    """Compare the Parquet on disk against a manifest describing it.

    THIS IS THE LINK ``compare_manifest`` CANNOT SEE. It takes a manifest dict
    rather than a path so the same code serves both callers: ``--verify-artefact``
    feeds it the committed file, which is the cheap check a contributor can run,
    and ``--verify-current`` feeds it a manifest built fresh from the warehouse,
    which closes warehouse -> manifest -> artefact at both links in one command.

    A missing artefact is NOT decided here. It is a legitimate state on a
    machine that never built one and a failure on the machine about to fit, and
    only the caller knows which it is - so this raises the question upward
    rather than resolving it into a pass.
    """
    if not path.exists():
        return [f"{path}: artefact absent"]
    rows, columns = read_artefact(path)
    expected_rows = manifest.get("counts", {}).get("merged_responses")
    problems: list[str] = []
    if expected_rows != rows:
        problems.append(f"artefact rows: manifest {expected_rows}, file {rows}")
    problems += compare_schema(
        manifest_schema(manifest),
        columns,
        expected_label="manifest",
        actual_label="artefact",
    )
    return problems


def default_manifest() -> pathlib.Path:
    return repo_root() / "exports" / "model" / "responses.manifest.json"


def repo_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[3]


def default_out() -> pathlib.Path:
    """Artefact path, env-driven. Never the repo - this is hundreds of MB."""
    from_env = os.environ.get("CODRONA_RESPONSES")
    if from_env:
        return pathlib.Path(from_env).expanduser()
    return pathlib.Path.home() / "codrona-data" / "model" / "responses.parquet"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build the IRT response matrix.")
    parser.add_argument("--database", type=pathlib.Path, default=None)
    parser.add_argument("--out", type=pathlib.Path, default=None)
    parser.add_argument("--schema", default=SCHEMA_NAME)
    parser.add_argument(
        "--real-data",
        action="store_true",
        help="also check the pinned counts; fails against synthetic fixtures",
    )
    parser.add_argument(
        "--no-write",
        action="store_true",
        help="measure and gate without writing the artefact",
    )
    parser.add_argument("--manifest", type=pathlib.Path, default=None)
    parser.add_argument(
        "--verify-current",
        action="store_true",
        help="do not write; fail if a fresh build disagrees with the manifest",
    )
    parser.add_argument(
        "--verify-artefact",
        action="store_true",
        help="compare the artefact on disk to the committed manifest; no warehouse",
    )
    args = parser.parse_args(argv)

    database = args.database or warehouse.default_database()
    manifest_path = args.manifest or default_manifest()
    artefact_path = args.out or default_out()

    if args.verify_artefact:
        # Reads Parquet metadata and a JSON file. No warehouse, no scan - this
        # is the half that is cheap enough to hook. A machine with no artefact
        # skips out loud: the file is uncommitted by design, so failing here
        # would fail every contributor for a condition none of them can fix.
        if not artefact_path.exists():
            print(f"no artefact at {artefact_path} - artefact NOT verified")
            return 0
        if not manifest_path.exists():
            print(f"no manifest at {manifest_path}", file=sys.stderr)
            return 1
        committed: Any = json.loads(manifest_path.read_text(encoding="utf-8"))
        problems = verify_artefact(artefact_path, committed)
        if problems:
            print(f"{len(problems)} disagreement(s) with {manifest_path}:", file=sys.stderr)
            for problem in problems:
                print(f"  {problem}", file=sys.stderr)
            print(
                "run: python3 -m codrona_lens.responses.matrix --real-data",
                file=sys.stderr,
            )
            return 1
        print(f"artefact matches the committed manifest: {artefact_path}")
        return 0

    if args.verify_current and not database.exists():
        # A contributor without the warehouse genuinely cannot regenerate this,
        # so blocking them would be wrong. Said out loud rather than passing
        # quietly: this is a maintainer gate.
        print(f"no warehouse at {database} - matrix freshness NOT verified")
        return 0
    if not database.exists():
        print(f"no warehouse at {database}", file=sys.stderr)
        return 1
    write_artefact = not (args.no_write or args.verify_current)
    out_path = (args.out or default_out()) if write_artefact else None

    con = warehouse.connect(database)
    try:
        report = build(con, out_path, schema=args.schema)
    finally:
        con.close()

    rate = 100.0 * report.bank_accepted / report.bank_responses
    print(f"fact rows            {report.fact_rows:>12,}")
    print(f"attempts (evidence)  {report.attempts:>12,}")
    print(f"responses unmerged   {report.unmerged_responses:>12,}")
    print(f"responses merged     {report.merged_responses:>12,}")
    print(f"collapsed duplicates {report.collapsed:>12,}")
    print(f"twin map entries     {len(report.twin.mapping):>12,}")
    print(f"keys actually merged {report.merged_keys:>12,}")
    print(f"bank responses       {report.bank_responses:>12,}")
    print(f"bank base rate       {rate:>12.4f}%")
    print(f"bank baseline Brier  {rate / 100 * (1 - rate / 100):>12.6f}")
    print(f"artefact columns     {len(report.columns):>12,}")
    print("\nexcluded by the committed twin rule, counted not dropped:")
    for line in report.twin.audit.describe():
        print(f"  {line}")
    if out_path is not None:
        write_manifest(report, manifest_path)
        print(f"wrote {out_path}")
        print(f"wrote {manifest_path}")

    if args.verify_current:
        stale = compare_manifest(report, manifest_path)
        # The artefact is compared to the FRESH build, not to the committed
        # manifest, so this command answers the question a fit actually asks:
        # is the file I am about to read current with the warehouse? An absent
        # artefact fails here and skips under --verify-artefact, because this is
        # the command run before a fit and there is then nothing to fit.
        stale += verify_artefact(artefact_path, build_manifest(report))
        if stale:
            print(f"\n{len(stale)} disagreement(s) against the warehouse:", file=sys.stderr)
            for line in stale:
                print(f"  {line}", file=sys.stderr)
            print(
                "run: python3 -m codrona_lens.responses.matrix --real-data",
                file=sys.stderr,
            )
            return 1
        print(f"\nmanifest and artefact are current with the warehouse: {artefact_path}")
        return 0

    problems = check_invariants(report)
    if args.real_data:
        problems += check_real_data(report)
    if problems:
        print(f"\nG12: {len(problems)} problem(s):", file=sys.stderr)
        for problem in problems:
            print(f"  {problem}", file=sys.stderr)
        return 1
    scope = "invariants + pinned counts" if args.real_data else "invariants only"
    print(f"\nG12 passed ({scope})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
