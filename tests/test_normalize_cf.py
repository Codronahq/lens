"""Tests for the Codeforces silver normalization.

The Spark-backed tests build tiny in-memory frames rather than reading the
landing zone, so they gate the transform logic without any data dependency.
They skip when no JVM is present, which keeps them honest in a CI job that has
not installed a JDK: the suite reports skips rather than passing vacuously.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

from __future__ import annotations

import pathlib
import shutil
from typing import Any

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.types import StringType, StructField  # noqa: E402

from codrona_lens.normalize.cf_submissions import (  # noqa: E402
    SUBMISSION_SCHEMA,
    HiddenLandingFilesError,
    audit_landing,
    deduplicate,
    hidden_landing_files,
    landing_glob,
    normalize,
    stage,
)

LANDING_SCHEMA = type(SUBMISSION_SCHEMA)(
    [*SUBMISSION_SCHEMA.fields, StructField("collected_via_handle", StringType())]
)


@pytest.fixture(scope="session")
def spark() -> Any:
    if shutil.which("java") is None:
        pytest.skip("no JVM available; Spark tests need a JDK")
    session = (
        SparkSession.builder.appName("codrona-tests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def make_row(**overrides: Any) -> dict[str, Any]:
    """A syntactically complete landing row; override only what a test is about."""
    row: dict[str, Any] = {
        "id": 1,
        "contestId": 1000,
        "creationTimeSeconds": 1_700_000_000,
        "relativeTimeSeconds": 3600,
        "passedTestCount": 10,
        "programmingLanguage": "GNU C++20",
        "testset": "TESTS",
        "timeConsumedMillis": 120,
        "points": None,
        "verdict": "OK",
        "author": {
            "ghost": False,
            "teamId": None,
            "participantType": "CONTESTANT",
            "startTimeSeconds": 1_699_990_000,
            "members": [{"handle": "alice"}],
        },
        "problem": {
            "contestId": 1000,
            "index": "A",
            "name": "Example",
            "rating": 800,
            "points": 500.0,
            "problemsetName": None,
            "tags": ["greedy", "math"],
            "type": "PROGRAMMING",
        },
        "collected_via_handle": "alice",
    }
    for key, value in overrides.items():
        if key in {"author", "problem"} and isinstance(value, dict):
            merged = dict(row[key])
            merged.update(value)
            row[key] = merged
        else:
            row[key] = value
    return row


def frame(spark: Any, rows: list[dict[str, Any]]) -> Any:
    return spark.createDataFrame(rows, schema=LANDING_SCHEMA)


def _write_landing(root: pathlib.Path, shard: str, name: str) -> pathlib.Path:
    """Create one landing file. Content is irrelevant; only the name is tested."""
    path = root / shard / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"")
    return path


def test_audit_landing_counts_visible_files(tmp_path: pathlib.Path) -> None:
    _write_landing(tmp_path, "00", "tourist.jsonl.gz")
    _write_landing(tmp_path, "a1", "Petr.jsonl.gz")
    assert audit_landing(tmp_path) == 2


def test_underscore_handles_are_detected(tmp_path: pathlib.Path) -> None:
    _write_landing(tmp_path, "00", "tourist.jsonl.gz")
    _write_landing(tmp_path, "d0", "_nobita.jsonl.gz")
    assert [p.name for p in hidden_landing_files(tmp_path)] == ["_nobita.jsonl.gz"]


def test_audit_landing_refuses_hidden_files(tmp_path: pathlib.Path) -> None:
    _write_landing(tmp_path, "00", "tourist.jsonl.gz")
    _write_landing(tmp_path, "d0", "__PACIFIC__.jsonl.gz")
    with pytest.raises(HiddenLandingFilesError, match="__PACIFIC__"):
        audit_landing(tmp_path)


def test_dot_prefixed_files_are_also_hidden(tmp_path: pathlib.Path) -> None:
    _write_landing(tmp_path, "e5", ".hidden.jsonl.gz")
    with pytest.raises(HiddenLandingFilesError):
        audit_landing(tmp_path)


def test_landing_glob_defaults_to_every_shard() -> None:
    root = pathlib.Path("/data/user_status")
    assert landing_glob(root) == ["/data/user_status/*/*.jsonl.gz"]


def test_landing_glob_narrows_to_named_shards() -> None:
    root = pathlib.Path("/data/user_status")
    assert landing_glob(root, ["00", "ff"]) == [
        "/data/user_status/00/*.jsonl.gz",
        "/data/user_status/ff/*.jsonl.gz",
    ]


def test_solo_submission_is_person_level(spark: Any) -> None:
    result = stage(frame(spark, [make_row()])).collect()[0]
    assert result["is_person_level"] is True
    assert result["handle"] == "alice"
    assert result["team_size"] == 1
    assert result["is_ghost"] is False


def test_team_submission_is_not_person_level(spark: Any) -> None:
    row = make_row(author={"members": [{"handle": "alice"}, {"handle": "bob"}]})
    result = stage(frame(spark, [row])).collect()[0]
    assert result["is_person_level"] is False
    assert result["team_size"] == 2
    assert result["handle"] is None
    assert result["author_handles"] == ["alice", "bob"]


def test_ghost_submission_is_not_person_level(spark: Any) -> None:
    row = make_row(author={"ghost": True})
    result = stage(frame(spark, [row])).collect()[0]
    assert result["is_person_level"] is False
    assert result["is_ghost"] is True


def test_null_ghost_flag_is_treated_as_false(spark: Any) -> None:
    row = make_row(author={"ghost": None})
    result = stage(frame(spark, [row])).collect()[0]
    assert result["is_ghost"] is False
    assert result["is_person_level"] is True


def test_problem_id_uses_the_problems_contest_not_the_submissions(spark: Any) -> None:
    # Submitted during contest 1999, but the problem belongs to contest 1000.
    row = make_row(contestId=1999, problem={"contestId": 1000, "index": "C2"})
    result = stage(frame(spark, [row])).collect()[0]
    assert result["problem_id"] == "1000C2"
    assert result["contest_id"] == 1999
    assert result["problem_contest_id"] == 1000


def test_problem_id_is_null_when_the_contest_is_missing(spark: Any) -> None:
    row = make_row(problem={"contestId": None})
    result = stage(frame(spark, [row])).collect()[0]
    assert result["problem_id"] is None


def test_acmsguru_rows_get_a_namespaced_problem_id(spark: Any) -> None:
    # The SGU archive has no contestId at all; index is numeric, not a letter.
    row = make_row(
        contestId=None,
        problem={"contestId": None, "index": "314", "problemsetName": "acmsguru"},
    )
    result = stage(frame(spark, [row])).collect()[0]
    assert result["problem_id"] == "acmsguru314"
    assert result["problemset_name"] == "acmsguru"


def test_problem_id_prefers_the_contest_over_the_problemset(spark: Any) -> None:
    problem = {"contestId": 1000, "index": "A", "problemsetName": "acmsguru"}
    row = make_row(problem=problem)
    result = stage(frame(spark, [row])).collect()[0]
    assert result["problem_id"] == "1000A"


def test_team_id_survives_and_one_member_teams_stay_person_level(spark: Any) -> None:
    # ~100 rows per 8,400 carry a teamId with a single member: a solo entry in
    # a team contest. Attributable to that person, so still person-level.
    row = make_row(author={"teamId": 4242, "members": [{"handle": "alice"}]})
    result = stage(frame(spark, [row])).collect()[0]
    assert result["team_id"] == 4242
    assert result["is_person_level"] is True
    assert result["team_size"] == 1


def test_the_two_points_fields_stay_distinct(spark: Any) -> None:
    # Top-level points is what this submission scored; problem.points is the
    # problem's maximum. Collapsing them would be silent data loss.
    row = make_row(points=110.0, problem={"points": 500.0})
    result = stage(frame(spark, [row])).collect()[0]
    assert result["points_scored"] == 110.0
    assert result["problem_points"] == 500.0


def test_accepted_and_contest_predicates(spark: Any) -> None:
    rows = [
        make_row(id=1, verdict="OK", author={"participantType": "CONTESTANT"}),
        make_row(id=2, verdict="WRONG_ANSWER", author={"participantType": "PRACTICE"}),
        make_row(id=3, verdict=None, author={"participantType": "VIRTUAL"}),
    ]
    result = {r["submission_id"]: r for r in stage(frame(spark, rows)).collect()}
    assert (result[1]["is_accepted"], result[1]["is_contest"]) == (True, True)
    assert (result[2]["is_accepted"], result[2]["is_contest"]) == (False, False)
    # A null verdict is a submission still in the queue: not accepted, and the
    # raw value survives so the distinction is not lost.
    assert (result[3]["is_accepted"], result[3]["is_contest"]) == (False, False)
    assert result[3]["verdict"] is None
    assert result[3]["participant_type"] == "VIRTUAL"


def test_tags_survive_as_an_array(spark: Any) -> None:
    result = stage(frame(spark, [make_row()])).collect()[0]
    assert result["problem_tags"] == ["greedy", "math"]


def test_submitted_year_is_derived_in_utc(spark: Any) -> None:
    # 2024-01-01T00:30:00Z -- a machine-local zone west of UTC would say 2023.
    row = make_row(creationTimeSeconds=1_704_069_000)
    result = stage(frame(spark, [row])).collect()[0]
    assert result["submitted_year"] == 2024


def test_deduplicate_keeps_one_row_per_submission_id(spark: Any) -> None:
    rows = [
        make_row(id=7, collected_via_handle="zoe"),
        make_row(id=7, collected_via_handle="alice"),
        make_row(id=8, collected_via_handle="bob"),
    ]
    result = deduplicate(stage(frame(spark, rows))).collect()
    assert len(result) == 2


def test_deduplicate_is_deterministic_on_the_collecting_handle(spark: Any) -> None:
    rows = [
        make_row(id=7, collected_via_handle="zoe"),
        make_row(id=7, collected_via_handle="alice"),
    ]
    kept = deduplicate(stage(frame(spark, rows))).collect()[0]
    assert kept["collected_via_handle"] == "alice"


def test_normalize_preserves_distinct_submissions(spark: Any) -> None:
    rows = [make_row(id=n) for n in range(1, 6)]
    assert normalize(frame(spark, rows)).count() == 5
