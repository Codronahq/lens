"""Tests for the CodeNet metadata census.

The fixtures write real CSV files to a real directory and the census reads them
through its real code path. Nothing is injected as a literal: a test that supplies
a derived value directly cannot gate the derivation, which is how a filename decode
bug survived fifty green tests earlier in this phase.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from codrona_lens.codenet import census as mod

HEADER = (
    "submission_id,problem_id,user_id,date,language,original_language,"
    "filename_ext,status,cpu_time,memory,code_size,accuracy"
)


def _problem_csv(path: Path, problem_id: str, rows: list[tuple[str, str, str]]) -> None:
    """Write a per-problem metadata CSV.

    Each row supplies (submission_id, user_id, status); the rest is plausible
    filler so the declared column types actually have to parse.
    """
    lines = [HEADER]
    for i, (sid, uid, status) in enumerate(rows):
        accuracy = "4/4" if status == "Accepted" else ""
        lines.append(
            f"{sid},{problem_id},{uid},{1500000000 + i},Python,Python 3,py,"
            f"{status},100,5000,420,{accuracy}"
        )
    path.write_text("\n".join(lines) + "\n")


@pytest.fixture
def metadata_dir(tmp_path: Path) -> Path:
    directory = tmp_path / "metadata"
    directory.mkdir()
    (directory / "problem_list.csv").write_text(
        "id,name,dataset,time_limit,memory_limit,rating,tags,complexity\n"
        "p00001,First,AIZU,1000,131072,,,\n"
        "p00002,Second,AIZU,1000,131072,,,\n"
        "p02534,Third,AtCoder,2000,1048576,,,\n"
    )
    _problem_csv(
        directory / "p00001.csv",
        "p00001",
        [
            ("s000000001", "u000000001", "Accepted"),
            ("s000000002", "u000000002", "Wrong Answer"),
        ],
    )
    _problem_csv(
        directory / "p00002.csv",
        "p00002",
        [("s000000003", "u000000001", "Time Limit Exceeded")],
    )
    _problem_csv(
        directory / "p02534.csv",
        "p02534",
        [("s000000004", "u000000003", "Accepted")],
    )
    return directory


def test_counts_every_file_and_row(metadata_dir: Path) -> None:
    result = mod.run_census(metadata_dir)
    assert result.files_on_disk == 3
    assert result.files_read == 3
    assert result.submissions == 4
    assert result.distinct_problems == 3
    assert result.distinct_users == 3


def test_index_is_excluded_from_the_per_problem_files(metadata_dir: Path) -> None:
    names = [path.name for path in mod.metadata_files(metadata_dir)]
    assert mod.PROBLEM_LIST not in names
    assert len(names) == 3


def test_status_vocabulary_is_read_from_the_data(metadata_dir: Path) -> None:
    result = mod.run_census(metadata_dir)
    assert result.status_counts == {
        "Accepted": 2,
        "Wrong Answer": 1,
        "Time Limit Exceeded": 1,
    }


def test_dataset_split_comes_from_the_index(metadata_dir: Path) -> None:
    result = mod.run_census(metadata_dir)
    assert result.problems_by_dataset == {"AIZU": 2, "AtCoder": 1}
    assert result.problems_in_index == 3


def test_accuracy_is_counted_only_where_present(metadata_dir: Path) -> None:
    result = mod.run_census(metadata_dir)
    assert result.submissions_with_accuracy == 2


def test_expected_total_mismatch_is_reported_not_hidden(metadata_dir: Path) -> None:
    result = mod.run_census(metadata_dir)
    assert result.expected_submissions == mod.CODENET_PERSON_LEVEL
    assert result.matches_expected is False


def test_dates_round_trip_to_utc(metadata_dir: Path) -> None:
    result = mod.run_census(metadata_dir)
    assert result.earliest_submission.endswith("+00:00")
    assert result.latest_submission.endswith("+00:00")
    assert result.earliest_submission <= result.latest_submission


def test_missing_index_is_an_error(tmp_path: Path) -> None:
    directory = tmp_path / "empty"
    directory.mkdir()
    with pytest.raises(FileNotFoundError):
        mod.run_census(directory)


def test_index_present_but_no_problem_files_is_an_error(tmp_path: Path) -> None:
    directory = tmp_path / "indexonly"
    directory.mkdir()
    (directory / "problem_list.csv").write_text(
        "id,name,dataset,time_limit,memory_limit,rating,tags,complexity\n"
    )
    with pytest.raises(FileNotFoundError):
        mod.run_census(directory)


def test_empty_problem_file_is_accounted_not_flagged(metadata_dir: Path) -> None:
    """CodeNet ships five problems with no submissions; they are not a gap.

    A header-only CSV contributes no rows and so never appears in the reader's
    filename column. Treating that as a missing file would fail the census on
    the real dataset, which is exactly the false positive this distinction
    exists to prevent.
    """
    (metadata_dir / "p00003.csv").write_text(HEADER + "\n")
    result = mod.run_census(metadata_dir)
    assert result.files_on_disk == 4
    assert result.files_read == 4
    assert result.empty_problem_files == ["p00003.csv"]
    assert result.submissions == 4


def test_file_gap_guard_fires_on_a_non_empty_unread_file(tmp_path: Path) -> None:
    """The guard must be able to fail, not merely be present.

    A file holding real rows that never reached the reader is the silent-skip
    shape - the failure that cost this phase a full-zone rebuild. Proven here
    against a file with content, so the guard cannot pass vacuously.
    """
    populated = tmp_path / "p00009.csv"
    _problem_csv(populated, "p00009", [("s000000009", "u000000009", "Accepted")])
    with pytest.raises(RuntimeError, match="file gap"):
        mod.classify_unread([populated], seen=set())


def test_empty_file_is_classified_without_raising(tmp_path: Path) -> None:
    header_only = tmp_path / "p00010.csv"
    header_only.write_text(HEADER + "\n")
    assert mod.classify_unread([header_only], seen=set()) == ["p00010.csv"]


def test_report_is_written_and_parses(metadata_dir: Path, tmp_path: Path) -> None:
    result = mod.run_census(metadata_dir)
    path = mod.write_report(result, tmp_path / "_reports")
    assert path.is_file()
    payload = json.loads(path.read_text())
    assert payload["submissions"] == 4
    assert payload["status_counts"]["Accepted"] == 2


def test_format_census_mentions_the_headline_numbers(metadata_dir: Path) -> None:
    text = mod.format_census(mod.run_census(metadata_dir))
    assert "submissions" in text
    assert "status vocabulary" in text
    assert "AtCoder" in text
