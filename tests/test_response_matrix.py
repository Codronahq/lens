"""Tests for the response matrix builder and G12.

Two kinds of test, and the split matters. The first kind mutates a
``BuildReport`` and asserts each invariant fires - proving every G12 rule is
capable of going red, which is the only thing that separates a gate from a
claim. The second kind builds against a fixture warehouse and asserts the
counts the SQL actually produces, because a gate that can fail on a fabricated
report says nothing about whether the query feeding it is right.

THE FIXTURE EXERCISES THE MERGE ON PURPOSE. One user submits to BOTH sides of a
twin and a second user submits only to the absent side. Without the first, the
collapse branch is unreachable and every assertion passes vacuously; without the
second, the routing of orphaned responses into the bank is never tested. Both
paths are the ones that are wrong in practice.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

from __future__ import annotations

import dataclasses
import json
import pathlib
from typing import Any

import duckdb
import pytest

from codrona_lens.responses import matrix, twins

# problem_key, contest_id, name, rating, in_public, solved_count
PROBLEMS: list[tuple[str, int, str, int | None, bool, int | None]] = [
    ("100A", 100, "Alpha", 1500, True, 900),
    ("101A", 101, "Alpha", 1500, False, None),
    ("200B", 200, "Beta", 1800, True, 400),
    ("300C", 300, "Gamma", None, False, None),
]

# submission_key, user, problem, accepted, participant, evidence, person_level
SUBMISSIONS: list[tuple[int, str, str, bool, str, bool, bool]] = [
    (1, "u1", "100A", False, "CONTESTANT", True, True),
    (2, "u1", "100A", True, "PRACTICE", True, True),
    (3, "u1", "101A", True, "CONTESTANT", True, True),
    (4, "u2", "101A", False, "CONTESTANT", True, True),
    (5, "u3", "200B", True, "VIRTUAL", True, True),
    (6, "u4", "300C", False, "PRACTICE", True, True),
    (7, "u5", "200B", False, "PRACTICE", False, True),
    (8, "u6", "200B", True, "CONTESTANT", True, False),
]


def _warehouse(
    path: pathlib.Path,
    submissions: list[tuple[int, str, str, bool, str, bool, bool]] | None = None,
) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(path))
    con.execute("create schema if not exists main_marts")
    con.execute(
        "create table main_marts.dim_problem ("
        "problem_key varchar, problem_contest_id integer, problem_name varchar, "
        "problem_rating integer, in_public_problemset boolean, "
        "solved_count bigint, is_current boolean)"
    )
    con.executemany(
        "insert into main_marts.dim_problem values (?, ?, ?, ?, ?, ?, true)",
        PROBLEMS,
    )
    con.execute(
        "create table main_marts.fct_submission ("
        "submission_key bigint, user_key varchar, problem_key varchar, "
        "is_accepted boolean, participant_type varchar, is_evidence boolean, "
        "is_person_level boolean, submitted_at timestamp)"
    )
    con.executemany(
        "insert into main_marts.fct_submission values "
        "(?, ?, ?, ?, ?, ?, ?, timestamp '2020-01-01')",
        submissions if submissions is not None else SUBMISSIONS,
    )
    return con


def _report(tmp_path: pathlib.Path, **kwargs: object) -> matrix.BuildReport:
    con = _warehouse(tmp_path / "w.duckdb", **kwargs)  # type: ignore[arg-type]
    try:
        return matrix.build(con, None)
    finally:
        con.close()


def test_fixture_actually_exercises_the_merge(tmp_path: pathlib.Path) -> None:
    """Guard against the vacuous-fixture trap before asserting anything else."""
    report = _report(tmp_path)
    assert report.collapsed == 1, "no pair collapsed - the merge path is untested"
    assert report.merged_keys == 1, "no response was routed across keys"


def test_counts_the_fixture_produces(tmp_path: pathlib.Path) -> None:
    report = _report(tmp_path)
    assert report.fact_rows == 8
    assert report.attempts == 6
    assert report.unmerged_responses == 5
    assert report.merged_responses == 4
    assert report.merged_attempts == 6
    assert report.twin.gap_matches == 1


def test_merge_preserves_attempts_and_reduces_responses(
    tmp_path: pathlib.Path,
) -> None:
    report = _report(tmp_path)
    assert report.merged_attempts == report.attempts
    assert report.merged_responses < report.unmerged_responses
    assert not matrix.check_invariants(report)


def test_the_surviving_row_is_the_lowest_submission_key(
    tmp_path: pathlib.Path,
) -> None:
    """The ordering rule, asserted from the artefact rather than from the code."""
    con = _warehouse(tmp_path / "w.duckdb")
    try:
        matrix.build(con, None)
        rows = con.execute(
            "select user_key, problem_key, submission_key, attempts "
            "from response_matrix order by user_key, problem_key"
        ).fetchall()
    finally:
        con.close()
    survivors = {(user, problem): key for user, problem, key, _ in rows}
    # u1 touched both twin sides; keys 1, 2 and 3 collapse to one response.
    assert survivors[("u1", "100A")] == 1
    attempts = {(user, problem): n for user, problem, _, n in rows}
    assert attempts[("u1", "100A")] == 3
    # u2 only ever touched the absent side and is routed into the bank.
    assert survivors[("u2", "100A")] == 4


def test_orphan_responses_reach_the_bank(tmp_path: pathlib.Path) -> None:
    con = _warehouse(tmp_path / "w.duckdb")
    try:
        report = matrix.build(con, None)
        source = con.execute(
            "select source_problem_key from response_matrix where user_key = 'u2'"
        ).fetchone()
    finally:
        con.close()
    assert source is not None and source[0] == "101A"
    # 100A x2 (u1, u2) plus 200B x1; 300C is outside the public problemset.
    assert report.bank_responses == 3


def test_non_evidence_and_team_rows_are_excluded(tmp_path: pathlib.Path) -> None:
    report = _report(tmp_path)
    assert report.fact_rows == 8
    assert report.attempts == 6, "is_evidence / is_person_level filters not applied"


def test_artefact_is_written_and_readable(tmp_path: pathlib.Path) -> None:
    out = tmp_path / "nested" / "responses.parquet"
    con = _warehouse(tmp_path / "w.duckdb")
    try:
        matrix.build(con, out)
    finally:
        con.close()
    assert out.exists()
    reader = duckdb.connect()
    count = reader.execute(f"select count(*) from read_parquet('{out}')").fetchone()
    assert count is not None and count[0] == 4


def test_duplicate_submission_key_fails_the_uniqueness_invariant(
    tmp_path: pathlib.Path,
) -> None:
    """End-to-end mutation: a repeated ordering key must go red."""
    mutated = [*SUBMISSIONS, (1, "u7", "200B", True, "PRACTICE", True, True)]
    report = _report(tmp_path, submissions=mutated)
    problems = matrix.check_invariants(report)
    assert any("ordering key not unique" in problem for problem in problems)


def _split(**changes: object) -> matrix.SplitReport:
    """Consistent against _base_report: 1 + 2 == 3 bank responses, parts == test.

    The numbers are small because the fixture bank is three responses. Tests
    that need an INCONSISTENT split pass it explicitly rather than relying on
    this one drifting.
    """
    base: dict[str, object] = {
        "cutoff": "2026-01-01",
        "train": 1,
        "test": 2,
        "g1_known_user_known_item": 1,
        "g3_new_item_only": 1,
        "g2_new_user_only": 0,
        "both_new": 0,
        "g1_accepted": 1,
    }
    base.update(changes)
    return matrix.SplitReport(**base)  # type: ignore[arg-type]


def _base_report() -> matrix.BuildReport:
    twin = twins.TwinMap(
        mapping={"101A": "100A"},
        gap_matches=1,
        qualifying=1,
        rating_agree=1,
        both_unrated=0,
        exactly_one_unrated=0,
        rating_differs=0,
    )
    return matrix.BuildReport(
        fact_rows=8,
        distinct_submission_keys=8,
        attempts=6,
        unmerged_responses=5,
        merged_responses=4,
        merged_attempts=6,
        merged_keys=1,
        twin=twin,
        bank_responses=3,
        bank_accepted=2,
        columns=(("user_key", "VARCHAR"), ("problem_key", "VARCHAR")),
        split=_split(),
    )


def test_a_good_report_raises_nothing() -> None:
    assert matrix.check_invariants(_base_report()) == []


@pytest.mark.parametrize(
    ("field", "value", "expected"),
    [
        ("distinct_submission_keys", 7, "ordering key not unique"),
        ("merged_attempts", 5, "merge changed the attempt count"),
        ("merged_attempts", 7, "merge changed the attempt count"),
        ("merged_responses", 6, "merge grew the response count"),
        ("merged_keys", 2, "twin map holds"),
    ],
)
def test_each_invariant_can_fail(field: str, value: int, expected: str) -> None:
    changes: dict[str, Any] = {field: value}
    mutated = dataclasses.replace(_base_report(), **changes)
    problems = matrix.check_invariants(mutated)
    assert any(expected in problem for problem in problems), problems


@pytest.mark.parametrize(
    ("field", "value", "expected"),
    [
        ("exactly_one_unrated", 1, "filters pairs it did not before"),
        ("rating_differs", 1, "filters pairs it did not before"),
        ("gap_matches", 2, "rating clause now excludes pairs"),
    ],
)
def test_each_twin_invariant_can_fail(field: str, value: int, expected: str) -> None:
    base = _base_report()
    changes: dict[str, Any] = {field: value}
    twin = dataclasses.replace(base.twin, **changes)
    problems = matrix.check_invariants(dataclasses.replace(base, twin=twin))
    assert any(expected in problem for problem in problems), problems


def _audit(**changes: int) -> twins.TwinAudit:
    base: dict[str, Any] = {
        "reversed_name_matches": 4,
        "reversed_rating_agree": 1,
        "reversed_both_unrated": 1,
        "reversed_exactly_one_unrated": 1,
        "reversed_rating_differs": 1,
        "reversed_pairs": (("500E", "501E"), ("600F", "601F"), ("700G", "701G")),
    }
    base.update(changes)
    return twins.TwinAudit(**base)


def test_a_consistent_reversed_audit_raises_nothing() -> None:
    report = dataclasses.replace(
        _base_report(),
        twin=dataclasses.replace(_base_report().twin, audit=_audit()),
    )
    assert matrix.check_invariants(report) == []


@pytest.mark.parametrize(
    ("changes", "expected"),
    [
        ({"reversed_rating_differs": 2}, "reversed rating classes sum to"),
        ({"reversed_name_matches": 5}, "reversed rating classes sum to"),
        ({"reversed_rating_agree": 0}, "admitting classes hold"),
        ({"reversed_pairs": (("500E", "501E"),)}, "admitting classes hold"),
    ],
)
def test_the_reversed_reconciliation_can_fail(changes: dict[str, Any], expected: str) -> None:
    """Neither invariant catches every mutation, which is why there are two.

    A miscounted class breaks the sum; a clause that stops matching its classes
    breaks the passing count. A single check on either would let the other
    through.
    """
    base = _base_report()
    report = dataclasses.replace(base, twin=dataclasses.replace(base.twin, audit=_audit(**changes)))
    problems = matrix.check_invariants(report)
    assert any(expected in problem for problem in problems), problems


def test_a_consistent_split_raises_nothing() -> None:
    assert matrix.check_invariants(_base_report()) == []


@pytest.mark.parametrize(
    ("changes", "expected"),
    [
        ({"train": 2}, "does not partition the bank"),
        ({"test": 3}, "does not partition the bank"),
        ({"g3_new_item_only": 2}, "partition parts sum to"),
        ({"both_new": 1}, "partition parts sum to"),
        ({"g1_accepted": 2}, "accepted inside a G1 partition"),
        ({"g1_known_user_known_item": 0, "g3_new_item_only": 2}, "G1 partition is empty"),
    ],
)
def test_the_split_reconciliations_can_fail(changes: dict[str, object], expected: str) -> None:
    """The arithmetic error that reached eval-gates.md, as a failing gate.

    Three of four published partition counts were multiplications of rounded
    shares and summed to twenty-eight more than the population they partition.
    Nothing could have gone red. These can.
    """
    base = _base_report()
    report = dataclasses.replace(base, split=_split(**changes))
    problems = matrix.check_invariants(report)
    assert any(expected in problem for problem in problems), problems


def test_the_split_is_measured_from_the_matrix_not_assumed(
    tmp_path: pathlib.Path,
) -> None:
    """Build against the fixture and check the partition reconciles for real.

    _split() is hand-written and could agree with a broken measure_split. This
    exercises the query.
    """
    con = _warehouse(tmp_path / "w.duckdb")
    try:
        report = matrix.build(con, None, split_cutoff="2000-01-01")
    finally:
        con.close()
    split = report.split
    assert split.cutoff == "2000-01-01"
    assert split.train + split.test == report.bank_responses
    assert split.parts == split.test
    assert split.new_item_responses == split.g3_new_item_only + split.both_new
    # Everything in the fixture postdates 2000, so nothing trains and every
    # held-out response has an unseen user and an unseen item.
    assert split.train == 0
    assert split.g1_known_user_known_item == 0
    assert split.both_new == split.test


def test_moving_the_cutoff_moves_the_partition(tmp_path: pathlib.Path) -> None:
    """A cutoff past every fixture row puts everything in train and nothing in test."""
    con = _warehouse(tmp_path / "w.duckdb")
    try:
        late = matrix.build(con, None, split_cutoff="2099-01-01")
    finally:
        con.close()
    assert late.split.test == 0
    assert late.split.train == late.bank_responses
    assert late.split.parts == 0
    assert matrix.check_invariants(late) == []


def test_the_manifest_carries_the_split(tmp_path: pathlib.Path) -> None:
    report = _report(tmp_path)
    payload = matrix.build_manifest(report)
    assert payload["split"]["cutoff"] == report.split.cutoff
    assert payload["split"]["test"] == report.split.test
    path = tmp_path / "responses.manifest.json"
    matrix.write_manifest(report, path)
    assert matrix.compare_manifest(report, path) == []
    committed = json.loads(path.read_text(encoding="utf-8"))
    committed["split"]["g1_accepted"] += 1
    path.write_text(json.dumps(committed, indent=2) + "\n", encoding="utf-8")
    problems = matrix.compare_manifest(report, path)
    assert any("split.g1_accepted" in line for line in problems), problems


def test_a_response_exactly_on_the_cutoff_is_not_lost(tmp_path: pathlib.Path) -> None:
    """The boundary belongs to test, and every row lands on exactly one side.

    Every fixture row carries `2020-01-01`, so a cutoff of `2020-01-01` puts all
    of them precisely on the boundary. Train is `< cutoff` and test is
    `>= cutoff`; narrowing test to `> cutoff` would drop every one of them from
    both sides, and nothing else in this file exercises that instant.
    """
    con = _warehouse(tmp_path / "w.duckdb")
    try:
        report = matrix.build(con, None, split_cutoff="2020-01-01")
    finally:
        con.close()
    split = report.split
    assert split.train == 0
    assert split.test == report.bank_responses
    assert split.test > 0
    assert split.train + split.test == report.bank_responses
    # Not `check_invariants(report) == []`: with nothing before the cutoff no
    # user or item is known, so the empty-G1 invariant fires and is right to.
    # This test is about the boundary instant, so it asserts only that the two
    # reconciliations stay silent.
    problems = matrix.check_invariants(report)
    assert not [p for p in problems if "does not partition" in p or "parts sum to" in p], problems
    assert any("G1 partition is empty" in p for p in problems), problems


def test_the_cli_survives_an_empty_held_out_period(
    tmp_path: pathlib.Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The default cutoff postdates every fixture row, so test is empty.

    This is the CLI path, which no other test covers, and it divided a share by
    an empty held-out period before 17 Aug 2026. A cutoff after all data is a
    legitimate configuration - it is what a fresh warehouse looks like.
    """
    database = tmp_path / "w.duckdb"
    _warehouse(database).close()
    code = matrix.main(
        [
            "--database",
            str(database),
            "--out",
            str(tmp_path / "r.parquet"),
            "--manifest",
            str(tmp_path / "m.json"),
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "held-out period is empty" in out


def test_pinned_counts_reject_the_fixture() -> None:
    """The real-data half must NOT pass on synthetic data - that is the boundary."""
    problems = matrix.check_real_data(_base_report())
    assert len(problems) == len(matrix.REAL_DATA_COUNTS)


def test_two_writes_are_byte_identical(tmp_path: pathlib.Path) -> None:
    """Byte-reproducibility, and an honest statement of what this cannot prove.

    (user_key, problem_key) is unique in the response matrix, so the COPY's
    ORDER BY is a total order and the file is reproducible. This asserts the
    positive.

    IT CANNOT DETECT THE ORDER BY BEING REMOVED. Deleting it leaves this test
    green, because a fixture of a handful of rows never engages DuckDB's
    parallel writer - the same vacuity as the gym floor in the twin rule, and
    the reason the project treats a fixture that is merely valid as different
    from one that is representative. Sizing the fixture up until parallelism
    engages would make the negative case nondeterministic rather than detectable,
    which is not a test.

    NOTHING CURRENTLY GATES ON THIS. The artefact is 235 MB and is not
    committed; the committed half is the manifest, which compares counts, and
    counts do not depend on row order. The ORDER BY is kept because it costs one
    clause and makes the artefact md5-comparable if a later gate wants that -
    not because a gate reads it today. Said out loud so a reader does not
    mistake this for protection it is not providing.
    """
    outputs = []
    for tag in ("a", "b"):
        out = tmp_path / f"{tag}.parquet"
        con = _warehouse(tmp_path / f"{tag}.duckdb")
        try:
            matrix.build(con, out)
        finally:
            con.close()
        outputs.append(out)
    # Separate databases and separate connections, so this also catches a
    # dependence on connection state rather than only on writer ordering.
    assert outputs[0].read_bytes() == outputs[1].read_bytes()


def test_manifest_round_trips_and_detects_a_stale_count(
    tmp_path: pathlib.Path,
) -> None:
    report = _report(tmp_path)
    path = tmp_path / "nested" / "responses.manifest.json"
    matrix.write_manifest(report, path)
    assert matrix.compare_manifest(report, path) == []

    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["counts"]["merged_responses"] += 1
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    stale = matrix.compare_manifest(report, path)
    assert any("counts.merged_responses" in line for line in stale)


def test_a_missing_manifest_is_reported_not_silently_passed(
    tmp_path: pathlib.Path,
) -> None:
    report = _report(tmp_path)
    stale = matrix.compare_manifest(report, tmp_path / "absent.json")
    assert len(stale) == 1
    assert "no committed manifest" in stale[0]


def test_the_matrix_carries_the_contest_id_covariate(tmp_path: pathlib.Path) -> None:
    """Read from the artefact, not from the SQL string.

    Asserting the column name appears in ``matrix._RESPONSES`` would pass on a
    query that no longer runs. This reads the value back for a known fixture
    problem, so deleting the projection goes red.
    """
    out = tmp_path / "responses.parquet"
    con = _warehouse(tmp_path / "w.duckdb")
    try:
        matrix.build(con, out)
    finally:
        con.close()
    reader = duckdb.connect()
    sql = "select user_key, problem_key, problem_contest_id from read_parquet(?)"
    rows = reader.execute(sql, [str(out)]).fetchall()
    assert ("u3", "200B", 200) in rows


def test_the_manifest_records_the_schema_the_build_emitted(
    tmp_path: pathlib.Path,
) -> None:
    report = _report(tmp_path)
    payload = matrix.build_manifest(report)
    recorded = [(entry["name"], entry["type"]) for entry in payload["schema"]]
    assert recorded == list(report.columns)
    assert ("problem_contest_id", "INTEGER") in recorded
    # The point of the block: it is the only thing that moves when a column does.
    assert payload["counts"]["merged_responses"] == report.merged_responses


@pytest.mark.parametrize(
    ("mutation", "expected"),
    [
        ("drop", "absent from measured"),
        ("add", "absent from committed"),
        ("retype", "type: committed"),
        ("reorder", "column ORDER differs"),
    ],
)
def test_compare_manifest_catches_every_shape_change(
    tmp_path: pathlib.Path, mutation: str, expected: str
) -> None:
    """Four mutations no count can see. Each must go red on its own.

    A column added, dropped, renamed, retyped or reordered leaves every count in
    the manifest identical, which is precisely why the schema block exists.
    """
    report = _report(tmp_path)
    path = tmp_path / "responses.manifest.json"
    matrix.write_manifest(report, path)
    assert matrix.compare_manifest(report, path) == []

    payload = json.loads(path.read_text(encoding="utf-8"))
    schema = payload["schema"]
    if mutation == "drop":
        schema.append({"name": "invented", "type": "VARCHAR"})
    elif mutation == "add":
        schema.pop()
    elif mutation == "retype":
        schema[0]["type"] = "BLOB"
    else:
        schema[0], schema[1] = schema[1], schema[0]
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    problems = matrix.compare_manifest(report, path)
    assert any(expected in line for line in problems), problems


def test_a_manifest_with_no_schema_block_is_reported(tmp_path: pathlib.Path) -> None:
    """An older manifest must fail loudly rather than compare vacuously."""
    report = _report(tmp_path)
    path = tmp_path / "responses.manifest.json"
    matrix.write_manifest(report, path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    del payload["schema"]
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    problems = matrix.compare_manifest(report, path)
    assert any("carries no schema block" in line for line in problems), problems


def _built(tmp_path: pathlib.Path) -> tuple[pathlib.Path, dict[str, Any]]:
    out = tmp_path / "responses.parquet"
    con = _warehouse(tmp_path / "w.duckdb")
    try:
        report = matrix.build(con, out)
    finally:
        con.close()
    return out, matrix.build_manifest(report)


def _rewrite(out: pathlib.Path, tmp_path: pathlib.Path, projection: str) -> None:
    """Replace the artefact through a sibling, never in place.

    A COPY that reads and writes one path is a race the engine is not obliged
    to lose predictably, and a flaky mutation test is worse than none.
    """
    staged = tmp_path / "staged.parquet"
    writer = duckdb.connect()
    query = projection.format(src=out)
    writer.execute(f"copy ({query}) to '{staged}' (format parquet)")
    writer.close()
    staged.replace(out)


def test_verify_artefact_passes_on_the_file_the_build_wrote(
    tmp_path: pathlib.Path,
) -> None:
    out, manifest = _built(tmp_path)
    assert matrix.verify_artefact(out, manifest) == []


def test_verify_artefact_reads_the_file_and_not_the_manifest(
    tmp_path: pathlib.Path,
) -> None:
    """The anti-vacuity test: the manifest is untouched and the FILE changes.

    Every other check here mutates the manifest, so all of them would still pass
    against a verify_artefact that returned an empty list without opening
    anything. This one rewrites the Parquet with a row removed and leaves the
    manifest exactly as the build wrote it, so it can only pass by reading the
    file.
    """
    out, manifest = _built(tmp_path)
    _rewrite(out, tmp_path, "select * from read_parquet('{src}') limit 2")
    problems = matrix.verify_artefact(out, manifest)
    assert any("artefact rows" in line for line in problems), problems


def test_verify_artefact_catches_a_column_missing_from_the_file(
    tmp_path: pathlib.Path,
) -> None:
    """Rewrite the FILE without a column. Row count is unchanged throughout."""
    out, manifest = _built(tmp_path)
    projection = "select * exclude (problem_contest_id) from read_parquet('{src}')"
    _rewrite(out, tmp_path, projection)
    problems = matrix.verify_artefact(out, manifest)
    assert any("problem_contest_id" in line for line in problems), problems
    assert not any("artefact rows" in line for line in problems), problems


def test_verify_artefact_reports_an_absent_file_rather_than_passing(
    tmp_path: pathlib.Path,
) -> None:
    _, manifest = _built(tmp_path)
    problems = matrix.verify_artefact(tmp_path / "gone.parquet", manifest)
    assert len(problems) == 1
    assert "artefact absent" in problems[0]


def test_the_derived_rates_come_from_the_counts_beside_them(
    tmp_path: pathlib.Path,
) -> None:
    """A manifest whose rate disagrees with its own counts would gate nothing."""
    report = _report(tmp_path)
    payload = matrix.build_manifest(report)
    responses = payload["counts"]["bank_responses"]
    accepted = payload["counts"]["bank_accepted"]
    rate = accepted / responses
    assert payload["derived"]["bank_base_rate_pct"] == round(100.0 * rate, 4)
    assert payload["derived"]["bank_baseline_brier"] == round(rate * (1 - rate), 6)
