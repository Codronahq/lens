"""Tests for the CodeNet normalize job.

Every fixture writes real CSV files and reads them through the real code path.
Nothing derived is injected as a literal: a test that supplies a derived column
as fixture data cannot gate the derivation, which is how a filename decode bug
survived fifty green tests earlier in this phase.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import SparkSession

from codrona_lens.codenet import normalize as mod

HEADER = (
    "submission_id,problem_id,user_id,date,language,original_language,"
    "filename_ext,status,cpu_time,memory,code_size,accuracy"
)

# 2019-07-01T00:00:00Z and 2020-07-01T00:00:00Z, so year partitioning is
# exercised by two distinct years rather than asserted.
T2019 = 1561939200
T2020 = 1593561600


@pytest.fixture(scope="module")
def spark() -> Any:
    session = (
        SparkSession.builder.appName("codrona-codenet-tests")
        .master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def _row(
    sid: str,
    pid: str,
    uid: str,
    ts: int,
    lang: str,
    olang: str,
    ext: str,
    status: str,
    cpu: str,
    mem: str,
    size: str,
    acc: str,
) -> str:
    """Build one metadata CSV line, so no test needs string concatenation."""
    fields = [sid, pid, uid, str(ts), lang, olang, ext, status, cpu, mem, size, acc]
    return ",".join(fields)


def _write(directory: Path, problem_id: str, rows: list[str]) -> None:
    (directory / f"{problem_id}.csv").write_text(
        "\n".join([HEADER, *rows]) + "\n" if rows else HEADER + "\n"
    )


@pytest.fixture
def metadata_dir(tmp_path: Path) -> Path:
    directory = tmp_path / "metadata"
    directory.mkdir()
    (directory / "problem_list.csv").write_text(
        "id,name,dataset,time_limit,memory_limit,rating,tags,complexity\n"
        "p00001,Alpha,AIZU,1000,131072,,,\n"
        "p02534,Beta,AtCoder,2000,1048576,,,\n"
        "p00099,Empty,AIZU,1000,131072,,,\n"
    )
    a = "p00001"
    b = "p02534"
    _write(
        directory,
        a,
        [
            _row(
                "s000000001",
                a,
                "u000000001",
                T2019,
                "Python",
                "Python 3",
                "py",
                "Accepted",
                "10",
                "5000",
                "420",
                "4/4",
            ),
            _row(
                "s000000002",
                a,
                "u000000002",
                T2019,
                "C++",
                "C++14",
                "cpp",
                "Wrong Answer",
                "20",
                "6000",
                "500",
                "1/4",
            ),
            _row(
                "s000000003",
                a,
                "u000000003",
                T2019,
                "C++",
                "C++14",
                "cpp",
                "Runtime Error",
                "-1",
                "6000",
                "500",
                "0/4",
            ),
        ],
    )
    _write(
        directory,
        b,
        [
            _row(
                "s000000004",
                b,
                "u000000004",
                T2020,
                "Java",
                "Java8",
                "java",
                "Compile Error",
                "",
                "",
                "700",
                "",
            ),
            _row(
                "s000000005",
                b,
                "u000000005",
                T2020,
                "Rust",
                "Rust",
                "rs",
                "Judge System Error",
                "",
                "",
                "300",
                "",
            ),
            _row(
                "s000000006",
                b,
                "u000000006",
                T2020,
                "Go",
                "Go",
                "go",
                "Query Limit Exceeded",
                "5",
                "100",
                "250",
                "",
            ),
        ],
    )
    _write(directory, "p00099", [])
    return directory


def _by_id(frame: Any, *fields: str) -> dict[str, Any]:
    """Collect a frame into {submission_id: value-or-tuple}.

    Exists so no test needs a comprehension long enough to be formatted
    differently at different line lengths.
    """
    rows = frame.collect()
    if len(fields) == 1:
        return {r["submission_id"]: r[fields[0]] for r in rows}
    return {r["submission_id"]: tuple(r[f] for f in fields) for r in rows}


def _normalized(spark: SparkSession, directory: Path) -> Any:
    files = mod.metadata_files(directory)
    submissions = mod.read_metadata(spark, files)
    problems = mod.read_problem_index(spark, directory)
    return mod.normalize(submissions, problems)


def test_index_excluded(metadata_dir: Path) -> None:
    names = [path.name for path in mod.metadata_files(metadata_dir)]
    assert mod.PROBLEM_LIST not in names
    assert "p00099.csv" in names
    assert len(names) == 3


def test_rows_survive_join(spark: SparkSession, metadata_dir: Path) -> None:
    assert _normalized(spark, metadata_dir).count() == 6


def test_judge_from_index(spark: SparkSession, metadata_dir: Path) -> None:
    rows = _by_id(_normalized(spark, metadata_dir), "judge")
    assert rows["s000000001"] == "AIZU"
    assert rows["s000000004"] == "AtCoder"


def test_status_mapping() -> None:
    assert len(mod.STATUS_CLASS) == 12
    assert mod.STATUS_CLASS["Internal error"] == ("unjudged", False)
    assert mod.STATUS_CLASS["Query Limit Exceeded"] == ("rejected", True)
    assert mod.STATUS_CLASS["WA: Presentation Error"][0] == "rejected"


def test_unjudged_not_evidence(spark: SparkSession, metadata_dir: Path) -> None:
    frame = _normalized(spark, metadata_dir)
    rows = _by_id(frame, "verdict_class", "is_evidence")
    assert rows["s000000005"] == ("unjudged", False)
    assert rows["s000000002"] == ("rejected", True)
    assert rows["s000000001"] == ("accepted", True)


def test_unknown_status(spark: SparkSession, tmp_path: Path) -> None:
    """A status CodeNet adds later must never be guessed at.

    Defaulting an unrecognised status to 'rejected' would corrupt calibration
    with no visible symptom, so it lands in 'unknown' where a test can fire.
    """
    directory = tmp_path / "metadata"
    directory.mkdir()
    (directory / "problem_list.csv").write_text(
        "id,name,dataset,time_limit,memory_limit,rating,tags,complexity\n"
        "p00001,Alpha,AIZU,1000,131072,,,\n"
    )
    _write(
        directory,
        "p00001",
        [
            _row(
                "s000000007",
                "p00001",
                "u000000007",
                T2019,
                "Python",
                "Python 3",
                "py",
                "Cosmic Ray Interference",
                "1",
                "1",
                "1",
                "",
            ),
        ],
    )
    row = _normalized(spark, directory).collect()[0]
    assert row["verdict_class"] == "unknown"
    assert row["is_evidence"] is False


def test_negative_cpu_nulled(spark: SparkSession, metadata_dir: Path) -> None:
    """A negative reading is corrupt, not absent, and must not become a zero."""
    frame = _normalized(spark, metadata_dir)
    rows = _by_id(frame, "cpu_time_ms", "has_corrupt_timing")
    assert rows["s000000003"] == (None, True)
    assert rows["s000000001"] == (10, False)


def test_missing_cpu_ok(spark: SparkSession, metadata_dir: Path) -> None:
    """Absent and corrupt are different states and must not collapse."""
    frame = _normalized(spark, metadata_dir)
    rows = _by_id(frame, "cpu_time_ms", "has_corrupt_timing")
    assert rows["s000000004"] == (None, False)


def test_accuracy_split(spark: SparkSession, metadata_dir: Path) -> None:
    frame = _normalized(spark, metadata_dir)
    rows = _by_id(frame, "tests_passed", "tests_total")
    assert rows["s000000001"] == (4, 4)
    assert rows["s000000002"] == (1, 4)
    assert rows["s000000004"] == (None, None)


def test_year_partition_utc(spark: SparkSession, metadata_dir: Path) -> None:
    years = _by_id(_normalized(spark, metadata_dir), "submitted_year")
    assert years["s000000001"] == 2019
    assert years["s000000004"] == 2020


def test_run_writes_silver(metadata_dir: Path, tmp_path: Path) -> None:
    out = tmp_path / "silver"
    reports = tmp_path / "_reports"
    report = mod.run(metadata_dir, out, reports)

    assert report.files_on_disk == 3
    # Every file is ACCOUNTED FOR, not merely read: two produced rows and
    # p00099 is a legitimate header-only problem. Requiring files_read to equal
    # the number producing rows would fail on the real dataset, which ships
    # five empty problems.
    assert report.files_read == 3
    assert report.empty_problem_files == ["p00099.csv"]
    assert report.rows == 6
    assert report.distinct_submission_ids == 6
    assert report.problems_in_index == 3
    assert report.judge_counts == {"AIZU": 3, "AtCoder": 3}
    assert report.corrupt_timing_rows == 1
    assert report.rows_with_tests == 3
    assert report.unknown_status_rows == 0

    partitions = sorted(p.name for p in out.iterdir() if p.name.startswith("submitted"))
    assert partitions == ["submitted_year=2019", "submitted_year=2020"]

    written = sorted(reports.iterdir())
    assert len(written) == 1
    payload = json.loads(written[0].read_text())
    assert payload["rows"] == 6


def test_missing_dir_errors(tmp_path: Path) -> None:
    directory = tmp_path / "empty"
    directory.mkdir()
    with pytest.raises(FileNotFoundError):
        mod.run(directory, tmp_path / "out")
