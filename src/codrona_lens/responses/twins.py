"""Derive the Div. 1 / Div. 2 twin key map, and count what the rule excludes.

WHY THIS EXISTS. One problem can occupy two problem keys: a Div. 1 and a Div. 2
round host the same problem under different contest ids, and the public
problemset publishes only one of the pair. Left alone, a user who attempted both
sides contributes two first-attempt responses to what is one item, which is a
direct local-independence violation - the thing the response definition exists to
prevent. ``docs/analysis/div1-div2-twins.md`` derives the rule and
``docs/architecture/phase-2-modelling.md`` section 4 fixes it as a model-input
mapping rather than a warehouse mutation, so it stays reversible.

THE RULE IMPLEMENTED HERE IS THE COMMITTED ONE, NOT THE PROSE. The document's
prose says the contest ids "differ by exactly one", which is symmetric. Its
reproduction query is directional - ``p.problem_contest_id =
a.problem_contest_id - 1``, with the unpublished side always the higher id - and
that query is what produced the pinned yield of 1,183. Widening the rule changes
the item bank, which needs evidence rather than a code edit, so this module
reproduces the query and the audit below counts what the query cannot see.

WHAT THE RULE EXCLUDES, MEASURED RATHER THAN ASSUMED. Three populations sit
outside it and each is counted and reported rather than dropped, because dropping
them silently is why nobody knew the first one existed:

- REVERSED pairs, where the published side carries the higher contest id. The
  directional query never looks there. Contest 206/207 dominates them and breaks an
  assumption the rule never states - that a name is unique within a contest - since
  one subtask-indexed name occupies several indices on both sides at once, so a name
  match resolves nothing.
- BOTH-PUBLISHED pairs at gap 1. Most are themed-round name reuse that the rating
  clause rejects outright. Any that PASS the rating clause are the interesting ones: a
  problem plausibly holding two keys that are both already in the item bank, with
  responses split across them, which is the harm this exercise exists to prevent.
- GYM pairs. Out of the archive's scope by the document's own boundary, and out of the
  item bank. They are still counted, because the response matrix is deliberately not
  bank-filtered and a mirrored gym contest would split one problem's responses inside
  it.

WHAT STILL RAISES. An absent key drawing more than one published partner inside
the rule's own scope, measured as zero. Picking a winner from two candidates is a
guess wearing the costume of a rule, and unlike the exclusions above there is no
defensible count to report instead.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

from __future__ import annotations

from dataclasses import dataclass, field

import duckdb

SCHEMA_NAME = "main_marts"

# Contest ids one apart. A constant because the measurement justifying it - every
# rating disagreement sits at some other gap - is about this number specifically.
CONTEST_ID_GAP = 1

# Gym contest ids start here. The committed query's `< 100000` predicate, named
# rather than inlined so the audit and the rule cannot drift apart.
GYM_CONTEST_ID_FLOOR = 100_000

_MAINLINE = """
select problem_key, problem_contest_id, problem_name, problem_rating,
       in_public_problemset
from {schema}.dim_problem
where is_current
  and problem_contest_id is not null
  and problem_contest_id < {floor}
  and problem_name is not null
"""

# The committed rule's population, WITHOUT its rating clause, so every rating
# class is counted here rather than being filtered away unseen.
_IN_SCOPE = """
with cur as ({mainline}),
absent as (select * from cur where not in_public_problemset),
present as (select * from cur where in_public_problemset)
select a.problem_key, p.problem_key, a.problem_rating, p.problem_rating
from absent a
join present p
  on p.problem_name = a.problem_name
 and p.problem_contest_id = a.problem_contest_id - {gap}
"""

_REVERSED = """
with cur as ({mainline}),
absent as (select * from cur where not in_public_problemset),
present as (select * from cur where in_public_problemset)
select a.problem_key, p.problem_key, a.problem_rating, p.problem_rating
from absent a
join present p
  on p.problem_name = a.problem_name
 and p.problem_contest_id = a.problem_contest_id + {gap}
"""

_BOTH_PUBLISHED = """
with cur as ({mainline})
select a.problem_key, b.problem_key, a.problem_rating, b.problem_rating
from cur a
join cur b
  on b.problem_contest_id = a.problem_contest_id + {gap}
 and b.problem_name = a.problem_name
where a.in_public_problemset and b.in_public_problemset
"""

_GYM = """
with cur as (
    select problem_key, problem_contest_id, problem_name
    from {schema}.dim_problem
    where is_current
      and problem_contest_id is not null
      and problem_contest_id >= {floor}
      and problem_name is not null
)
select count(*)
from cur a
join cur b
  on b.problem_contest_id = a.problem_contest_id + {gap}
 and b.problem_name = a.problem_name
"""

Pair = tuple[str, str]


@dataclass(frozen=True)
class TwinAudit:
    """Populations the committed rule cannot see, counted rather than dropped."""

    reversed_pairs: tuple[Pair, ...] = ()
    reversed_name_matches: int = 0
    both_published: tuple[Pair, ...] = ()
    both_published_passing_rating: tuple[Pair, ...] = ()
    gym_pairs: int = 0

    def describe(self) -> list[str]:
        return [
            f"reversed direction (published side higher): "
            f"{self.reversed_name_matches} name matches, "
            f"{len(self.reversed_pairs)} passing the rating clause",
            f"both sides published at gap 1: {len(self.both_published)}"
            f", of which {len(self.both_published_passing_rating)} pass the"
            " rating clause",
            f"gym gap-1 name pairs: {self.gym_pairs}",
        ]


@dataclass(frozen=True)
class TwinMap:
    """The derived mapping, with the counts that make it checkable."""

    mapping: dict[str, str]
    gap_matches: int
    qualifying: int
    rating_agree: int
    both_unrated: int
    exactly_one_unrated: int
    rating_differs: int
    audit: TwinAudit = field(default_factory=TwinAudit)

    @property
    def present_keys(self) -> frozenset[str]:
        """Keys that survive a merge - the published side of every pair."""
        return frozenset(self.mapping.values())


def _rating_class(left: int | None, right: int | None) -> str:
    if left is None and right is None:
        return "both_unrated"
    if left is None or right is None:
        return "exactly_one_unrated"
    if left == right:
        return "rating_agree"
    return "rating_differs"


def _passes_rating(left: int | None, right: int | None) -> bool:
    """The committed query's clause: equal, or either side unrated.

    Wider than the prose, which says "ratings agree, or neither side is rated".
    Measured, the difference is vacuous - no gap-1 pair has exactly one side
    rated - and ``exactly_one_unrated`` is carried so that stops being true
    loudly rather than silently.
    """
    return left is None or right is None or left == right


def _audit(con: duckdb.DuckDBPyConnection, mainline: str, *, schema: str) -> TwinAudit:
    reversed_rows = con.execute(_REVERSED.format(mainline=mainline, gap=CONTEST_ID_GAP)).fetchall()
    both_rows = con.execute(
        _BOTH_PUBLISHED.format(mainline=mainline, gap=CONTEST_ID_GAP)
    ).fetchall()
    gym_row = con.execute(
        _GYM.format(schema=schema, floor=GYM_CONTEST_ID_FLOOR, gap=CONTEST_ID_GAP)
    ).fetchone()
    return TwinAudit(
        reversed_pairs=tuple(
            (left, right)
            for left, right, left_rating, right_rating in reversed_rows
            if _passes_rating(left_rating, right_rating)
        ),
        reversed_name_matches=len(reversed_rows),
        both_published=tuple((left, right) for left, right, _, _ in both_rows),
        both_published_passing_rating=tuple(
            (left, right)
            for left, right, left_rating, right_rating in both_rows
            if _passes_rating(left_rating, right_rating)
        ),
        gym_pairs=int(gym_row[0]) if gym_row is not None else 0,
    )


def derive(con: duckdb.DuckDBPyConnection, *, schema: str = SCHEMA_NAME) -> TwinMap:
    """Build the absent-key to present-key map under the committed rule."""
    mainline = _MAINLINE.format(schema=schema, floor=GYM_CONTEST_ID_FLOOR)
    rows = con.execute(_IN_SCOPE.format(mainline=mainline, gap=CONTEST_ID_GAP)).fetchall()

    counts = {
        "rating_agree": 0,
        "both_unrated": 0,
        "exactly_one_unrated": 0,
        "rating_differs": 0,
    }
    contested: dict[str, list[str]] = {}
    for absent_key, present_key, absent_rating, present_rating in rows:
        counts[_rating_class(absent_rating, present_rating)] += 1
        if _passes_rating(absent_rating, present_rating):
            contested.setdefault(absent_key, []).append(present_key)

    multiple = {key: keys for key, keys in contested.items() if len(keys) > 1}
    if multiple:
        sample = next(iter(sorted(multiple.items())))
        raise SystemExit(
            f"twin map cannot be derived: {len(multiple)} absent key(s) draw more "
            f"than one published partner, e.g. {sample}. Measured as zero in the "
            "rule's own scope at the 2026-08-06 snapshot; picking a winner would "
            "be a guess, not a rule."
        )

    qualifying = counts["rating_agree"] + counts["both_unrated"] + counts["exactly_one_unrated"]
    return TwinMap(
        mapping={key: keys[0] for key, keys in contested.items()},
        gap_matches=len(rows),
        qualifying=qualifying,
        rating_agree=counts["rating_agree"],
        both_unrated=counts["both_unrated"],
        exactly_one_unrated=counts["exactly_one_unrated"],
        rating_differs=counts["rating_differs"],
        audit=_audit(con, mainline, schema=schema),
    )
