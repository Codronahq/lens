"""Tests for the Div. 1 / Div. 2 twin key map, as the committed rule defines it.

THE RULE IS DIRECTIONAL AND THE FIXTURES ENCODE THAT. The published side carries
the LOWER contest id and the absent side the higher, because that is what
``div1-div2-twins.md``'s reproduction query does. A symmetric fixture would pass
against a symmetric implementation and silently stop testing the rule that
produced the pinned yield.

THE RATING CLAUSE IS THE QUERY'S, NOT THE PROSE'S. The committed SQL admits a
pair where EITHER side is unrated; the prose says "neither side is rated". They
agree in the rule's own scope, where no gap-1 absent-versus-present pair has
exactly one side rated - and ``test_exactly_one_side_unrated_is_admitted`` pins
which of them the code implements, so the day that stops being vacuous it is a
visible decision. THEY DO NOT AGREE IN THE REVERSED POPULATION: six of the real
74 name matches have exactly one side rated and all six sit inside the pinned
43, so the audit carries the class split and
``test_a_reversed_pair_with_one_side_unrated_is_admitted_and_classed`` keeps that
path reachable. This docstring claimed the two rules agreed everywhere until
16 Aug 2026.

THE RAISING TEST NEEDS TWO PUBLISHED PROBLEMS IN ONE CONTEST. Under a directional
rule an absent key can only draw several partners when one contest publishes the
same name twice - the subtask-indexed shape that contest 206/207 exhibits in the
real dimension. An earlier version of this test used adjacent contests, which
cannot produce the collision, and passed vacuously.

The fixture is invented rather than sampled - a public repository is publication
under LEGAL.md, and real rows would put Codeforces problem ids in it.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

from __future__ import annotations

import duckdb
import pytest

from codrona_lens.responses import twins

# contest_id is nullable because the acmsguru archive carries none.
# problem_key, contest_id, name, rating, in_public_problemset
ProblemRow = tuple[str, int | None, str, int | None, bool]

BASE: list[ProblemRow] = [
    ("100A", 100, "Alpha", 1500, True),
    ("101A", 101, "Alpha", 1500, False),
    ("200B", 200, "Beta", 1800, True),
    ("300C", 300, "Gamma", None, False),
]


def _warehouse(rows: list[ProblemRow]) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("create schema if not exists main_marts")
    con.execute(
        "create table main_marts.dim_problem ("
        "problem_key varchar, problem_contest_id integer, problem_name varchar, "
        "problem_rating integer, in_public_problemset boolean, is_current boolean)"
    )
    con.executemany("insert into main_marts.dim_problem values (?, ?, ?, ?, ?, true)", rows)
    return con


def test_derives_the_absent_to_present_mapping() -> None:
    twin = twins.derive(_warehouse(BASE))
    assert twin.mapping == {"101A": "100A"}
    assert twin.gap_matches == 1
    assert twin.qualifying == 1
    assert twin.present_keys == frozenset({"100A"})


def test_both_sides_unrated_still_qualifies() -> None:
    rows: list[ProblemRow] = [
        *BASE,
        ("400D", 400, "Delta", None, True),
        ("401D", 401, "Delta", None, False),
    ]
    twin = twins.derive(_warehouse(rows))
    assert twin.mapping == {"101A": "100A", "401D": "400D"}
    assert twin.both_unrated == 1
    assert twin.rating_agree == 1


def test_rating_disagreement_is_excluded_and_counted() -> None:
    rows: list[ProblemRow] = [
        *BASE,
        ("400D", 400, "Delta", 1200, True),
        ("401D", 401, "Delta", 2600, False),
    ]
    twin = twins.derive(_warehouse(rows))
    assert "401D" not in twin.mapping
    assert twin.rating_differs == 1
    assert twin.gap_matches == 2
    assert twin.qualifying == 1


def test_exactly_one_side_unrated_is_admitted() -> None:
    """Pins the committed SQL's clause over the document's prose."""
    rows: list[ProblemRow] = [
        *BASE,
        ("400D", 400, "Delta", 1200, True),
        ("401D", 401, "Delta", None, False),
    ]
    twin = twins.derive(_warehouse(rows))
    assert twin.mapping["401D"] == "400D"
    assert twin.exactly_one_unrated == 1
    assert twin.qualifying == 2


def test_a_contest_gap_of_two_is_not_a_twin() -> None:
    rows: list[ProblemRow] = [
        *BASE,
        ("400D", 400, "Delta", 1700, True),
        ("402D", 402, "Delta", 1700, False),
    ]
    twin = twins.derive(_warehouse(rows))
    assert "402D" not in twin.mapping
    assert twin.gap_matches == 1


def test_equal_names_in_the_same_contest_are_not_twins() -> None:
    rows: list[ProblemRow] = [*BASE, ("200Z", 200, "Beta", 1800, False)]
    twin = twins.derive(_warehouse(rows))
    assert "200Z" not in twin.mapping


def test_a_problem_with_no_contest_id_is_ignored() -> None:
    rows: list[ProblemRow] = [*BASE, ("SGU1", None, "Alpha", 1500, False)]
    twin = twins.derive(_warehouse(rows))
    assert twin.mapping == {"101A": "100A"}


def test_reversed_direction_is_audited_never_mapped() -> None:
    """The published side carrying the higher id: 43 such pairs exist for real."""
    rows: list[ProblemRow] = [
        *BASE,
        ("500E", 500, "Epsilon", 1700, False),
        ("501E", 501, "Epsilon", 1700, True),
    ]
    twin = twins.derive(_warehouse(rows))
    assert "500E" not in twin.mapping
    assert twin.audit.reversed_pairs == (("500E", "501E"),)
    assert twin.audit.reversed_name_matches == 1
    assert twin.gap_matches == 1


def test_a_reversed_pair_failing_the_rating_clause_is_name_matched_only() -> None:
    """The 74-versus-43 split, asserted rather than inferred from a count."""
    rows: list[ProblemRow] = [
        *BASE,
        ("500E", 500, "Epsilon", 1200, False),
        ("501E", 501, "Epsilon", 2900, True),
    ]
    twin = twins.derive(_warehouse(rows))
    assert twin.audit.reversed_name_matches == 1
    assert twin.audit.reversed_pairs == ()


def test_a_reversed_pair_with_one_side_unrated_is_admitted_and_classed() -> None:
    """The case the real data has six of, and the two documented rules disagree on.

    Under the query's clause this pair passes; under the prose rule - "ratings
    agree, or neither side is rated" - it does not. In the canonical direction
    the distinction is vacuous, which is why it went unnoticed. Here it decides
    whether the pinned reversed count is 43 or 37.
    """
    rows: list[ProblemRow] = [
        *BASE,
        ("500E", 500, "Epsilon", None, False),
        ("501E", 501, "Epsilon", 1700, True),
    ]
    twin = twins.derive(_warehouse(rows))
    assert twin.audit.reversed_pairs == (("500E", "501E"),)
    assert twin.audit.reversed_exactly_one_unrated == 1
    assert twin.audit.reversed_rating_agree == 0
    assert twin.audit.reversed_both_unrated == 0


def test_reversed_rating_classes_partition_the_name_matches() -> None:
    """Every reversed pair lands in exactly one class, and the classes reconcile.

    Four pairs, one per class, so no class is exercised only by its absence -
    a fixture where three classes are empty would pass against an
    implementation that never populated them.
    """
    rows: list[ProblemRow] = [
        *BASE,
        ("500E", 500, "Epsilon", 1700, False),
        ("501E", 501, "Epsilon", 1700, True),
        ("600F", 600, "Zeta", None, False),
        ("601F", 601, "Zeta", None, True),
        ("700G", 700, "Eta", None, False),
        ("701G", 701, "Eta", 1500, True),
        ("800H", 800, "Theta", 1200, False),
        ("801H", 801, "Theta", 2900, True),
    ]
    audit = twins.derive(_warehouse(rows)).audit
    assert audit.reversed_name_matches == 4
    assert audit.reversed_rating_agree == 1
    assert audit.reversed_both_unrated == 1
    assert audit.reversed_exactly_one_unrated == 1
    assert audit.reversed_rating_differs == 1
    classes = (
        audit.reversed_rating_agree
        + audit.reversed_both_unrated
        + audit.reversed_exactly_one_unrated
        + audit.reversed_rating_differs
    )
    assert classes == audit.reversed_name_matches
    assert len(audit.reversed_pairs) == classes - audit.reversed_rating_differs


def test_describe_reports_the_class_split_not_only_the_total() -> None:
    """The CLI report is what a maintainer reads before a fit.

    Without this the split is carried on the dataclass and never surfaces, so
    43 still reconciles to neither documented rule for anyone reading output
    rather than JSON.

    EVERY COUNT IS DISTINCT ON PURPOSE. A first version of this test used a
    fixture where the agreeing count was zero, so replacing the field with a
    literal zero produced a byte-identical line and the mutation survived. Equal
    values would likewise hide a swapped pair of fields.
    """
    audit = twins.TwinAudit(
        reversed_name_matches=11,
        reversed_rating_agree=2,
        reversed_both_unrated=3,
        reversed_exactly_one_unrated=4,
        reversed_rating_differs=2,
        reversed_pairs=(("500E", "501E"),),
    )
    split = [line for line in audit.describe() if "of those passing" in line]
    assert len(split) == 1, audit.describe()
    assert "2 agreeing" in split[0]
    assert "3 neither rated" in split[0]
    assert "4 exactly one rated" in split[0]


def test_both_published_is_audited_and_split_by_rating() -> None:
    rows: list[ProblemRow] = [
        *BASE,
        ("600F", 600, "Zeta", 1900, True),
        ("601F", 601, "Zeta", 1900, True),
        ("700G", 700, "Eta", 1100, True),
        ("701G", 701, "Eta", 3000, True),
    ]
    twin = twins.derive(_warehouse(rows))
    assert len(twin.audit.both_published) == 2
    assert twin.audit.both_published_passing_rating == (("600F", "601F"),)
    assert "600F" not in twin.mapping


def test_gym_pairs_are_counted_but_never_mapped() -> None:
    rows: list[ProblemRow] = [
        *BASE,
        ("100289H", 100289, "Hydra", None, False),
        ("100290H", 100290, "Hydra", None, False),
    ]
    twin = twins.derive(_warehouse(rows))
    assert twin.audit.gym_pairs == 1
    assert twin.mapping == {"101A": "100A"}


def test_the_gym_floor_is_inert_and_that_is_recorded_not_assumed() -> None:
    """The committed query's `< 100000` predicate filters nothing. Measured.

    A gym problem is never in the public problemset, so it can never be the
    published side, and a gym absent problem can only match a published partner
    that is by definition not gym. Raising the floor to admit gym therefore
    leaves the mapping identical. This is asserted rather than left implicit
    because a mutation test on that predicate passes either way, and a reader
    would otherwise reasonably believe it is load-bearing.
    """
    rows: list[ProblemRow] = [
        *BASE,
        ("100289H", 100289, "Hydra", None, False),
        ("100290H", 100290, "Hydra", None, False),
    ]
    scoped = twins.derive(_warehouse(rows))
    original = twins.GYM_CONTEST_ID_FLOOR
    twins.GYM_CONTEST_ID_FLOOR = 99_999_999
    try:
        unscoped = twins.derive(_warehouse(rows))
    finally:
        twins.GYM_CONTEST_ID_FLOOR = original
    assert unscoped.mapping == scoped.mapping == {"101A": "100A"}
    assert unscoped.gap_matches == scoped.gap_matches


def test_raises_when_one_absent_key_draws_two_partners() -> None:
    """Only reachable when one contest publishes the same name twice."""
    rows: list[ProblemRow] = [
        ("100A", 100, "Alpha", 1500, True),
        ("100B", 100, "Alpha", 1500, True),
        ("101A", 101, "Alpha", 1500, False),
    ]
    with pytest.raises(SystemExit, match="more than one published partner"):
        twins.derive(_warehouse(rows))


def test_neither_side_published_simply_does_not_match() -> None:
    rows: list[ProblemRow] = [
        ("100A", 100, "Alpha", 1500, False),
        ("101A", 101, "Alpha", 1500, False),
    ]
    twin = twins.derive(_warehouse(rows))
    assert twin.mapping == {}
    assert twin.gap_matches == 0
