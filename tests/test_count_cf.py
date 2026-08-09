"""Tests for the landing-zone corpus count.

The fixture zone deliberately contains a team submission written into two
members' files, a percent-encoded filename, and a legacy underscore filename,
because those are the three shapes that have already produced wrong counts.
"""

from __future__ import annotations

import gzip
import json
import pathlib
from typing import Any

import pytest

from codrona_lens.codeforces import count as count_mod


def _write(path: pathlib.Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")


def _solo(sub_id: int, handle: str) -> dict[str, Any]:
    return {"id": sub_id, "author": {"members": [{"handle": handle}]}}


def _team(sub_id: int, team_id: int, handles: list[str]) -> dict[str, Any]:
    return {
        "id": sub_id,
        "author": {
            "teamId": team_id,
            "members": [{"handle": h} for h in handles],
        },
    }


@pytest.fixture
def zone(tmp_path: pathlib.Path) -> pathlib.Path:
    root = tmp_path / "user_status"
    pair = _team(100, 7, ["tourist", "petr"])
    solo_rows = [_solo(1, "tourist"), _solo(2, "tourist"), pair]
    _write(root / "00/tourist.jsonl.gz", solo_rows)
    _write(root / "01/petr.jsonl.gz", [_solo(3, "petr"), pair])
    _write(root / "02/%5Ffabio.jsonl.gz", [_solo(4, "_fabio")])
    _write(root / "03/_legacy.jsonl.gz", [_solo(5, "_legacy")])
    _write(root / "04/lone.jsonl.gz", [_team(101, 8, ["lone"])])
    return root


def test_landing_files_sees_hidden_names(zone: pathlib.Path) -> None:
    names = {p.name for p in count_mod.landing_files(zone)}
    assert "_legacy.jsonl.gz" in names
    assert "%5Ffabio.jsonl.gz" in names
    assert len(names) == 5


def test_hidden_and_encoded_classification(zone: pathlib.Path) -> None:
    files = count_mod.landing_files(zone)
    assert [p.name for p in count_mod.hidden_files(files)] == ["_legacy.jsonl.gz"]
    assert [p.name for p in count_mod.encoded_files(files)] == ["%5Ffabio.jsonl.gz"]


def test_duckdb_reads_every_file_including_underscore(zone: pathlib.Path) -> None:
    report = count_mod.count_corpus(zone)
    assert report["files_on_disk"] == 5
    assert report["files_read"] == 5


def test_team_rows_dedupe_globally(zone: pathlib.Path) -> None:
    report = count_mod.count_corpus(zone)
    assert report["raw_rows"] == 8
    assert report["unique_submissions"] == 7
    assert report["duplicate_rows"] == 1


def test_person_level_counts_one_member_teams(zone: pathlib.Path) -> None:
    report = count_mod.count_corpus(zone)
    assert report["solo_submissions"] == 5
    assert report["team_submissions"] == 2
    assert report["one_member_team_submissions"] == 1
    assert report["person_level_submissions"] == 6


def test_combined_figure_uses_codenet_constant(zone: pathlib.Path) -> None:
    report = count_mod.count_corpus(zone)
    expected = report["person_level_submissions"] + count_mod.CODENET_PERSON_LEVEL
    assert report["combined_person_level"] == expected


def test_unread_file_is_a_hard_failure(zone: pathlib.Path) -> None:
    _write(zone / "05/empty.jsonl.gz", [])
    with pytest.raises(count_mod.CountIntegrityError) as excinfo:
        count_mod.count_corpus(zone)
    assert "empty.jsonl.gz" in str(excinfo.value)


def test_empty_zone_is_a_hard_failure(tmp_path: pathlib.Path) -> None:
    (tmp_path / "user_status/00").mkdir(parents=True)
    with pytest.raises(count_mod.CountIntegrityError):
        count_mod.count_corpus(tmp_path / "user_status")


def test_report_writes_dated_json(zone: pathlib.Path, tmp_path: pathlib.Path) -> None:
    report = count_mod.count_corpus(zone)
    path = count_mod.write_report(report, tmp_path / "_reports")
    assert path.name.startswith("cf_corpus_count_")
    assert json.loads(path.read_text())["raw_rows"] == 8
