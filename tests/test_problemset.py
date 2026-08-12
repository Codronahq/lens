"""Tests for the Codeforces problemset snapshot.

The parity test is the load-bearing one. ``build_problem_id`` is a second
implementation of a key that already exists in the Spark normalize job, and two
implementations of one key is exactly the shape that drifts silently: a
mismatch does not raise anywhere, it just makes every problemset row fail to
join and leaves dim_problem looking untouched. So the test runs both over the
same inputs and compares, rather than asserting the Python side against
hand-written expectations that could match a wrong rule.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

from __future__ import annotations

import json
import pathlib
from typing import Any

import pytest

from codrona_lens.codeforces.problemset import (
    ACMSGURU_PROBLEMSET,
    MAIN_PROBLEMSET,
    ProblemsetError,
    build_problem_id,
    build_records,
    fetch,
    index_statistics,
    summarize,
    write_snapshot,
)

Json = dict[str, Any]

FETCHED_AT = "2026-08-12T09:00:00+00:00"


def records_for(result: dict[str, Any], source: str = MAIN_PROBLEMSET) -> Any:
    """Wrapper so the constant kwargs do not push every call over the margin."""
    return build_records(result, problemset_source=source, fetched_at=FETCHED_AT)


def problem(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "contestId": 1234,
        "index": "A",
        "name": "Example",
        "type": "PROGRAMMING",
        "rating": 800,
        "tags": ["implementation"],
    }
    base.update(overrides)
    return base


def statistic(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {"contestId": 1234, "index": "A", "solvedCount": 42}
    base.update(overrides)
    return base


class FakeClient:
    """Records the problemset_name of every call so the two-call rule is gated."""

    def __init__(self, responses: dict[str | None, Json]) -> None:
        self._responses = responses
        self.calls: list[str | None] = []

    def problemset_problems(self, *, problemset_name: str | None = None) -> Json:
        self.calls.append(problemset_name)
        return self._responses[problemset_name]


# --- key construction ------------------------------------------------------


def test_contest_problem_keys_on_contest_id() -> None:
    assert build_problem_id(problem()) == "1234A"


def test_acmsguru_keys_on_problemset_name() -> None:
    archive = {"problemsetName": "acmsguru", "index": "95"}
    assert build_problem_id(archive) == "acmsguru95"


def test_contest_id_wins_when_both_are_present() -> None:
    both = {"contestId": 1234, "problemsetName": "acmsguru", "index": "A"}
    assert build_problem_id(both) == "1234A"


def test_missing_index_has_no_key() -> None:
    assert build_problem_id({"contestId": 1234}) is None


def test_no_contest_and_no_problemset_has_no_key() -> None:
    assert build_problem_id({"index": "A"}) is None


# --- record construction ---------------------------------------------------


def test_solved_count_joins_onto_the_problem() -> None:
    result = {"problems": [problem()], "problemStatistics": [statistic()]}
    records = records_for(result)
    assert len(records) == 1
    assert records[0]["problem_id"] == "1234A"
    assert records[0]["solved_count"] == 42
    assert records[0]["fetched_at"] == FETCHED_AT
    assert records[0]["problemset_source"] == MAIN_PROBLEMSET


def test_problem_without_statistics_keeps_a_null_solved_count() -> None:
    result: dict[str, Any] = {"problems": [problem()], "problemStatistics": []}
    records = records_for(result)
    assert records[0]["solved_count"] is None


def test_unrated_problem_is_kept_with_a_null_rating() -> None:
    result = {
        "problems": [problem(rating=None)],
        "problemStatistics": [statistic()],
    }
    records = records_for(result)
    assert records[0]["problem_rating"] is None


def test_fields_outside_the_allowlist_never_reach_the_record() -> None:
    noisy = problem(secretField="should not survive")
    result = {"problems": [noisy], "problemStatistics": []}
    records = records_for(result)
    assert "secretField" not in records[0]
    assert "secretField" not in json.dumps(records[0])


def test_missing_tags_become_an_empty_list_not_null() -> None:
    bare = problem()
    del bare["tags"]
    result = {"problems": [bare], "problemStatistics": []}
    records = records_for(result)
    assert records[0]["problem_tags"] == []


def test_duplicate_problem_id_raises() -> None:
    result = {"problems": [problem(), problem()], "problemStatistics": []}
    with pytest.raises(ProblemsetError, match="duplicate problem_id"):
        records_for(result)


def test_statistic_matching_no_problem_raises() -> None:
    """A count that should reconcile and does not is a lead, never a warning."""
    result = {
        "problems": [problem()],
        "problemStatistics": [statistic(), statistic(index="B")],
    }
    with pytest.raises(ProblemsetError, match="matched no problem"):
        records_for(result)


def test_problem_with_no_derivable_key_raises() -> None:
    result = {"problems": [{"name": "keyless"}], "problemStatistics": []}
    with pytest.raises(ProblemsetError, match="no derivable key"):
        records_for(result)


def test_non_list_payload_raises() -> None:
    with pytest.raises(ProblemsetError, match="not lists"):
        records_for({})


def test_statistic_without_solved_count_is_skipped_not_fatal() -> None:
    stats = [statistic(solvedCount=None)]
    assert index_statistics(stats) == {}


# --- the two-call rule -----------------------------------------------------


def test_fetch_requests_main_unnamed_and_acmsguru_by_name() -> None:
    responses: dict[str | None, dict[str, Any]] = {
        None: {"problems": [problem()], "problemStatistics": [statistic()]},
        ACMSGURU_PROBLEMSET: {
            "problems": [{"problemsetName": "acmsguru", "index": "95"}],
            "problemStatistics": [],
        },
    }
    client = FakeClient(responses)
    records = fetch(client, fetched_at=FETCHED_AT)  # type: ignore[arg-type]

    assert client.calls == [None, ACMSGURU_PROBLEMSET]
    assert {r["problem_id"] for r in records} == {"1234A", "acmsguru95"}
    sources = {r["problemset_source"] for r in records}
    assert sources == {MAIN_PROBLEMSET, ACMSGURU_PROBLEMSET}


def test_fetch_raises_when_ids_collide_across_problemsets() -> None:
    responses: dict[str | None, dict[str, Any]] = {
        None: {"problems": [problem()], "problemStatistics": []},
        ACMSGURU_PROBLEMSET: {"problems": [problem()], "problemStatistics": []},
    }
    client = FakeClient(responses)
    with pytest.raises(ProblemsetError, match="collides"):
        fetch(client, fetched_at=FETCHED_AT)  # type: ignore[arg-type]


# --- snapshot --------------------------------------------------------------


def test_snapshot_is_one_json_object_per_line(tmp_path: pathlib.Path) -> None:
    result = {"problems": [problem()], "problemStatistics": [statistic()]}
    records = records_for(result)
    path = tmp_path / "nested" / "problemset.jsonl"
    write_snapshot(records, path)

    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["problem_id"] == "1234A"


def test_summary_counts_rated_and_unrated(tmp_path: pathlib.Path) -> None:
    result = {
        "problems": [problem(), problem(index="B", rating=None, tags=[])],
        "problemStatistics": [statistic()],
    }
    records = records_for(result)
    summary = summarize(records)
    assert summary["problems"] == 2
    assert summary["rated"] == 1
    assert summary["unrated"] == 1
    assert summary["with_solved"] == 1
    assert summary["tagged"] == 1


# --- parity with the Spark key ---------------------------------------------

PARITY_CASES: list[dict[str, Any]] = [
    {"contestId": 1234, "problemsetName": None, "index": "A"},
    {"contestId": 4, "problemsetName": None, "index": "B2"},
    {"contestId": None, "problemsetName": "acmsguru", "index": "95"},
    {"contestId": 1234, "problemsetName": "acmsguru", "index": "A"},
    {"contestId": None, "problemsetName": None, "index": "A"},
    {"contestId": 1234, "problemsetName": None, "index": None},
]


def test_python_key_matches_the_spark_key() -> None:
    pyspark = pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    from codrona_lens.normalize.cf_submissions import _problem_id

    assert pyspark is not None

    inner = StructType(
        [
            StructField("contestId", IntegerType()),
            StructField("problemsetName", StringType()),
            StructField("index", StringType()),
        ]
    )
    schema = StructType([StructField("problem", inner)])

    spark = (
        SparkSession.builder.appName("codrona-parity")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        rows = [(dict(case),) for case in PARITY_CASES]
        frame = spark.createDataFrame(rows, schema=schema)
        collected = frame.select(_problem_id()).collect()
        spark_keys = [row[0] for row in collected]
    finally:
        spark.stop()

    python_keys = [build_problem_id(case) for case in PARITY_CASES]
    assert python_keys == spark_keys
