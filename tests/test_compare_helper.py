"""Gate the notebook's compare() helper, which nothing else gates.

compare() is the only check that the third engine reproduces the published
observatory aggregates, and it is defined inline in the notebook rather than
imported: the notebook runs on a Databricks Free workspace where this package
is not installed. A notebook-only commit also skips the mypy and pytest hooks,
so the helper shipped ungated. Reading the function back out of the committed
.ipynb gates the source that actually runs instead of a copy free to drift.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

from __future__ import annotations

import json
import pathlib
from typing import Any

import pytest

NOTEBOOK = (
    pathlib.Path(__file__).resolve().parents[1] / "notebooks" / "state_of_cp_verification.ipynb"
)

ROWS = [{"year": 2019, "submissions": 100}]


def load_compare() -> Any:
    """Extract compare() from the committed notebook and return it."""
    cells = json.loads(NOTEBOOK.read_text())["cells"]
    sources = [
        "".join(cell["source"])
        for cell in cells
        if cell["cell_type"] == "code" and "def compare(" in "".join(cell["source"])
    ]
    if len(sources) != 1:
        raise AssertionError(f"expected one compare() cell, found {len(sources)}")
    namespace: dict[str, Any] = {}
    exec(sources[0], namespace)
    return namespace["compare"]


@pytest.fixture(name="compare")
def compare_fixture() -> Any:
    return load_compare()


def test_agreeing_rows_agree(compare: Any) -> None:
    assert compare("agree", ROWS, ROWS, "year", ["submissions"], []) == []


def test_differing_values_disagree(compare: Any) -> None:
    other = [{"year": 2019, "submissions": 999}]
    assert len(compare("differ", ROWS, other, "year", ["submissions"], [])) == 1


def test_column_absent_from_one_side_disagrees(compare: Any) -> None:
    """Asserts that it fails, not how many times.

    A one-sided absence trips both the membership check and the value check, so
    it yields two failures. Pinning the count would make this test fail on a
    change that improved the diagnostics rather than broke them.
    """
    one = [{"year": 2019}]
    failures = compare("one-side", ROWS, one, "year", ["submissions"], [])
    assert any("submissions" in failure for failure in failures)


def test_column_absent_from_both_sides_disagrees(compare: Any) -> None:
    """The regression: a typo'd column name was reported as verified."""
    failures = compare("typo", ROWS, ROWS, "year", ["submissionz"], [])
    assert failures, "a column absent from both sides must fail, not pass"


def test_real_column_null_on_both_sides_still_agrees(compare: Any) -> None:
    """Guards the fix from over-reaching.

    A column that exists on both sides and holds NULL in some row is genuine
    agreement. Deleting the None/None branch would break real comparisons.
    """
    rows = [{"year": 2019, "submissions": None}]
    assert compare("null", rows, rows, "year", ["submissions"], []) == []


def test_key_present_on_only_one_side_is_reported(compare: Any) -> None:
    extra = [{"year": 2019, "submissions": 100}, {"year": 2020, "submissions": 5}]
    failures = compare("keys", extra, ROWS, "year", ["submissions"], [])
    assert any("2020" in failure for failure in failures)
