"""Tests for the observatory export and its publication gate.

The load-bearing ones are the failure tests. A gate written after a fix, that
passes either way, documents the fix without gating it - so every rule here is
first shown to FAIL against a mutated export before it is trusted to pass
against a good one. The four mutations mirror the four ways this can go wrong in
practice: an identifying column reaches the file, a small cell keeps its
statistics, a table's column set drifts from the allowlist, and the two tables
that partition one population stop agreeing.

The fixture warehouse is built here rather than sampled, for the same reason the
CI dbt fixtures are invented: a public repository is publication under LEGAL.md,
and a hundred real rows would put Codeforces handles in it.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

from __future__ import annotations

import json
import pathlib
from typing import Any

import duckdb
import pytest

from codrona_lens.observatory import export

SCHEMA_YAML = """
version: 2
models:
  - name: obs_rating_distribution
    description: Cohort users per rating band. Stratified cohort, never platform totals.
  - name: obs_activity_by_year
    description: Per-year volume. Left-truncated by activeOnly collection; not a growth curve.
  - name: obs_tag_landscape
    description: Per-tag problem counts. A problem appears once per tag.
  - name: obs_country_participation
    description: Self-declared country. Declaring correlates with strength.
  - name: obs_organization_participation
    description: Declared organisation, free text and unnormalised.
"""

# Two countries above the threshold and one below, so both branches are exercised.
CountryRow = tuple[str, bool, int, float, float | None, float | None, int | None, int | None]

COUNTRIES: list[CountryRow] = [
    ("Fixtureland", False, 30, 60.0, 1400.0, 1350.0, 2600, 4),
    ("(undeclared)", True, 19, 38.0, 900.0, 880.0, 1500, 0),
    ("Smallcountry", False, 1, 2.0, None, None, None, None),
]

BANDS = [
    ("newbie", 1, 25, 50.0, -19, 1199, 900.0),
    ("expert", 4, 20, 40.0, 1600, 1899, 1750.0),
    ("grandmaster", 8, 5, 10.0, 2400, 2599, 2450.0),
]


def _build_warehouse(path: pathlib.Path) -> None:
    con = duckdb.connect(str(path))
    con.execute("create schema if not exists main_marts")
    con.execute(
        "create table main_marts.obs_rating_distribution ("
        "rating_band varchar, band_order integer, cohort_users bigint, "
        "cohort_share_pct double, min_rating integer, max_rating integer, mean_rating double)"
    )
    con.executemany(
        "insert into main_marts.obs_rating_distribution values (?, ?, ?, ?, ?, ?, ?)", BANDS
    )
    con.execute(
        "create table main_marts.obs_activity_by_year ("
        "submitted_year integer, submissions bigint, person_level_submissions bigint, "
        "active_users bigint, problems_attempted bigint, accepted bigint, accepted_pct double, "
        "in_contest bigint, is_partial_year boolean, registered_by_then bigint, "
        "active_share_pct double)"
    )
    con.execute(
        "insert into main_marts.obs_activity_by_year values "
        "(2024, 900, 880, 40, 120, 400, 44.4, 300, false, 45, 88.9), "
        "(2025, 500, 490, 30, 90, 220, 44.0, 150, true, 50, 60.0)"
    )
    con.execute(
        "create table main_marts.obs_tag_landscape ("
        "tag varchar, problems_with_tag bigint, rated_problems bigint, mean_rating double, "
        "median_rating double, min_rating integer, max_rating integer, "
        "median_solved_count double, from_problemset bigint, from_tiebreak bigint)"
    )
    con.execute(
        "insert into main_marts.obs_tag_landscape values "
        "('math', 12, 10, 1500.0, 1400.0, 800, 2400, 3000.0, 12, 0), "
        "('dp', 7, 7, 1900.0, 1900.0, 1200, 2600, 900.0, 7, 0)"
    )
    con.execute(
        "create table main_marts.obs_country_participation ("
        "country varchar, is_undeclared boolean, cohort_users bigint, cohort_share_pct double, "
        "mean_rating double, median_rating double, max_rating integer, "
        "candidate_master_plus bigint, is_reportable boolean)"
    )
    for name, undeclared, users, share, mean, median, top, cm in COUNTRIES:
        con.execute(
            "insert into main_marts.obs_country_participation values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [name, undeclared, users, share, mean, median, top, cm, users >= 5],
        )
    con.execute(
        "create table main_marts.obs_organization_participation ("
        "organization varchar, cohort_users bigint, mean_rating double, median_rating double, "
        "max_rating integer, candidate_master_plus bigint, most_common_country varchar, "
        "is_reportable boolean)"
    )
    con.execute(
        "insert into main_marts.obs_organization_participation values "
        "('Fixture University', 12, 1500.0, 1450.0, 2200, 2, 'Fixtureland', true), "
        "('(below reporting threshold)', 6, null, null, null, null, null, false)"
    )
    con.close()


@pytest.fixture
def written(tmp_path: pathlib.Path) -> pathlib.Path:
    database = tmp_path / "fixture.duckdb"
    _build_warehouse(database)
    schema = tmp_path / "_schema.yml"
    schema.write_text(SCHEMA_YAML, encoding="utf-8")
    out = tmp_path / "export"
    export.write_export(database, out, schema)
    return out


def _load(out: pathlib.Path, table: str) -> dict[str, Any]:
    payload: dict[str, Any] = json.loads((out / f"{table}.json").read_text(encoding="utf-8"))
    return payload


def _save(out: pathlib.Path, table: str, payload: dict[str, Any]) -> None:
    (out / f"{table}.json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def test_export_writes_every_table_and_a_manifest(written: pathlib.Path) -> None:
    for table in export.ALLOWLIST:
        assert (written / f"{table}.json").exists()
    manifest = json.loads((written / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["row_counts"]["obs_country_participation"] == len(COUNTRIES)
    assert manifest["min_cohort_users"] == export.MIN_COHORT_USERS


def test_a_good_export_verifies_clean(written: pathlib.Path) -> None:
    assert export.verify_export(written) == []


def test_output_is_deterministic(tmp_path: pathlib.Path, written: pathlib.Path) -> None:
    """No wall-clock field, so a regeneration that changes nothing changes no bytes."""
    database = tmp_path / "fixture.duckdb"
    schema = tmp_path / "_schema.yml"
    again = tmp_path / "again"
    export.write_export(database, again, schema)
    for name in [f"{table}.json" for table in export.ALLOWLIST] + ["manifest.json"]:
        assert (written / name).read_bytes() == (again / name).read_bytes()


def test_caveats_come_from_the_schema_file(written: pathlib.Path) -> None:
    payload = _load(written, "obs_activity_by_year")
    assert "not a growth curve" in payload["caveat"]


def test_missing_description_is_fatal(tmp_path: pathlib.Path) -> None:
    schema = tmp_path / "_schema.yml"
    schema.write_text("version: 2\nmodels:\n  - name: obs_tag_landscape\n", encoding="utf-8")
    with pytest.raises(SystemExit):
        export.load_descriptions(schema)


def test_unexpected_column_is_fatal(tmp_path: pathlib.Path) -> None:
    """A column added to a published table must stop the export, not ride along."""
    database = tmp_path / "extra.duckdb"
    _build_warehouse(database)
    con = duckdb.connect(str(database))
    con.execute("alter table main_marts.obs_country_participation add column handle varchar")
    con.close()
    schema = tmp_path / "_schema.yml"
    schema.write_text(SCHEMA_YAML, encoding="utf-8")
    with pytest.raises(SystemExit) as excinfo:
        export.write_export(database, tmp_path / "out", schema)
    assert "handle" in str(excinfo.value)


def test_gate_fails_on_an_identifying_key(written: pathlib.Path) -> None:
    payload = _load(written, "obs_country_participation")
    payload["rows"][0]["handle"] = "tourist"
    _save(written, "obs_country_participation", payload)
    problems = export.verify_export(written)
    assert any("identifying key" in problem for problem in problems)


def test_gate_fails_when_a_small_cell_keeps_its_statistics(written: pathlib.Path) -> None:
    payload = _load(written, "obs_country_participation")
    small = next(row for row in payload["rows"] if row["cohort_users"] < export.MIN_COHORT_USERS)
    small["max_rating"] = 1609
    _save(written, "obs_country_participation", payload)
    problems = export.verify_export(written)
    assert any("publishes" in problem for problem in problems)


def test_gate_fails_when_columns_drift_from_the_allowlist(written: pathlib.Path) -> None:
    payload = _load(written, "obs_tag_landscape")
    payload["columns"] = payload["columns"][:-1]
    _save(written, "obs_tag_landscape", payload)
    assert any("allowlist" in problem for problem in export.verify_export(written))


def test_gate_fails_when_the_two_populations_disagree(written: pathlib.Path) -> None:
    payload = _load(written, "obs_rating_distribution")
    payload["rows"][0]["cohort_users"] += 1
    _save(written, "obs_rating_distribution", payload)
    assert any("population disagreement" in problem for problem in export.verify_export(written))


def test_gate_reports_a_missing_export(tmp_path: pathlib.Path) -> None:
    assert export.verify_export(tmp_path / "nothing") != []


def test_committed_export_passes_the_gate() -> None:
    """The artefact actually in the repository, checked without touching a warehouse."""
    out = export.repo_root() / "exports" / "observatory"
    assert out.exists(), (
        "no export at exports/observatory - run "
        "python3 -m codrona_lens.observatory.export before committing. "
        "A skip here would let a deleted export pass CI."
    )
    assert export.verify_export(out) == []
