"""A single-instant read of the whole Codeforces problemset.

Why this exists. Every problem attribute the warehouse holds was, until now,
reconstructed from submission rows - the only place tags and ratings appeared.
That has two defects this module fixes at the source.

Tags vary between collected files for 7 problems, so dim_problem has to pick a
winner by a tiebreak rule. A problemset read is one response at one instant, so
there is nothing to break a tie between.

A problem nobody in the cohort attempted has no submission row at all, and so
no rating, however well known it is. Codeforces is the ONLY labelled-difficulty
source in the corpus - CodeNet and CodeContests supply none - so every rating
recovered here is a prior anchoring an IRT scale that otherwise has nothing to
anchor it. That is the larger half of the value and it is invisible from the
submission side.

``solvedCount`` arrives free in the same response, from ``problemStatistics``.
It is a population-level difficulty signal measured over every Codeforces user
rather than over our cohort, which makes it an independent check on cohort
sampling bias rather than another view of the same evidence.

THE PROBLEM KEY IS NOT REDEFINED HERE. ``build_problem_id`` reproduces the rule
in ``normalize.cf_submissions._problem_id`` exactly, including the acmsguru
fallback, because a key that differs by so much as a separator would orphan
every row against dim_problem silently rather than failing. A parity test
asserts the two implementations agree on the same inputs; that test, not this
docstring, is what keeps them together.

TWO CALLS, NOT ONE. The default problemset does not contain the acmsguru
archive - it is reachable only by naming it - so the archive that already needs
special handling in the key would otherwise be the one set of problems missing
from the read. Both calls pass through the same global limiter at the
documented 1 request / 2 seconds.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
from typing import Any

from .client import CodeforcesClient
from .config import load_config
from .limiter import RateLimiter
from .schema import PROBLEM_FIELDS, _pick

# The mainline problemset is requested by passing no problemsetName at all.
MAIN_PROBLEMSET = "main"
ACMSGURU_PROBLEMSET = "acmsguru"
PROBLEMSETS = (MAIN_PROBLEMSET, ACMSGURU_PROBLEMSET)

STATISTIC_FIELDS = frozenset({"contestId", "index", "solvedCount"})


class ProblemsetError(Exception):
    """A structural problem in the response that must not reach disk."""


def build_problem_id(
    problem: dict[str, Any],
    *,
    default_problemset_name: str | None = None,
) -> str | None:
    """Mirror of normalize.cf_submissions._problem_id, in Python.

    Contest problems key on contestId; the acmsguru archive has no contestId at
    all and keys on its problemset name. Returns None when neither rule applies,
    which the caller treats as fatal rather than writing an unjoinable row.

    ``default_problemset_name`` exists for one measured reason. When a custom
    problemset is requested BY NAME, Codeforces omits problemsetName from every
    row of problemStatistics - it is already known from the request - so those
    rows carry only index and solvedCount and cannot be keyed on their own.
    Measured on acmsguru: 453 of 453 statistics lack both contestId and
    problemsetName, while 453 of 453 problems carry problemsetName. Supplying
    the requested name restores exactly the key the submission corpus already
    uses, because submission rows do carry problemsetName. The default is None,
    so the mainline path keeps the strict rule and an unkeyable row still
    raises rather than silently inheriting a name.
    """
    index = problem.get("index")
    if index is None:
        return None
    contest_id = problem.get("contestId")
    if contest_id is not None:
        return f"{contest_id}{index}"
    problemset_name = problem.get("problemsetName")
    if problemset_name is None:
        problemset_name = default_problemset_name
    if problemset_name is not None:
        return f"{problemset_name}{index}"
    return None


def index_statistics(
    statistics: list[dict[str, Any]],
    *,
    default_name: str | None = None,
) -> dict[str, int]:
    """Map problem_id -> solvedCount, rejecting anything that cannot be keyed."""
    out: dict[str, int] = {}
    for raw in statistics:
        stat = _pick(raw, STATISTIC_FIELDS)
        problem_id = build_problem_id(stat, default_problemset_name=default_name)
        if problem_id is None:
            raise ProblemsetError(f"statistic with no derivable key: {stat}")
        solved = stat.get("solvedCount")
        if solved is None:
            continue
        out[problem_id] = int(solved)
    return out


def build_records(
    result: dict[str, Any],
    *,
    problemset_source: str,
    fetched_at: str,
) -> list[dict[str, Any]]:
    """Project one problemset.problems response onto the columns we retain."""
    problems = result.get("problems")
    statistics = result.get("problemStatistics")
    if not isinstance(problems, list) or not isinstance(statistics, list):
        msg = f"{problemset_source}: problems/problemStatistics not lists"
        raise ProblemsetError(msg)

    named = None if problemset_source == MAIN_PROBLEMSET else problemset_source
    solved_by_id = index_statistics(statistics, default_name=named)
    records: list[dict[str, Any]] = []
    seen: set[str] = set()

    for raw in problems:
        problem = _pick(raw, PROBLEM_FIELDS)
        problem_id = build_problem_id(problem)
        if problem_id is None:
            msg = f"{problemset_source}: no derivable key: {problem}"
            raise ProblemsetError(msg)
        if problem_id in seen:
            msg = f"{problemset_source}: duplicate problem_id {problem_id}"
            raise ProblemsetError(msg)
        seen.add(problem_id)

        tags = problem.get("tags")
        records.append(
            {
                "problem_id": problem_id,
                "contest_id": problem.get("contestId"),
                "problemset_name": problem.get("problemsetName"),
                "problemset_source": problemset_source,
                "problem_index": problem.get("index"),
                "problem_name": problem.get("name"),
                "problem_type": problem.get("type"),
                "problem_points": problem.get("points"),
                "problem_rating": problem.get("rating"),
                "problem_tags": list(tags) if isinstance(tags, list) else [],
                "solved_count": solved_by_id.get(problem_id),
                "fetched_at": fetched_at,
            }
        )

    unmatched = set(solved_by_id) - seen
    if unmatched:
        sample = sorted(unmatched)[:5]
        count = len(unmatched)
        msg = f"{problemset_source}: {count} statistics matched no problem"
        msg += f" (e.g. {sample}) - both lists should key identically"
        raise ProblemsetError(msg)

    return records


def fetch(
    client: CodeforcesClient,
    *,
    fetched_at: str,
    problemsets: tuple[str, ...] = PROBLEMSETS,
) -> list[dict[str, Any]]:
    """Read every problemset and return one flat record list."""
    records: list[dict[str, Any]] = []
    for source in problemsets:
        name = None if source == MAIN_PROBLEMSET else source
        result = client.problemset_problems(problemset_name=name)
        chunk = build_records(result, problemset_source=source, fetched_at=fetched_at)
        records.extend(chunk)

    ids = [record["problem_id"] for record in records]
    if len(ids) != len(set(ids)):
        raise ProblemsetError("problem_id collides across problemsets")
    return records


def write_snapshot(records: list[dict[str, Any]], path: pathlib.Path) -> None:
    """One JSON object per line, so the file streams into DuckDB unchanged."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True))
            handle.write("\n")


def summarize(records: list[dict[str, Any]]) -> dict[str, int]:
    """Counts that a caller can compare against the warehouse afterwards."""
    return {
        "problems": len(records),
        "rated": sum(1 for r in records if r["problem_rating"] is not None),
        "unrated": sum(1 for r in records if r["problem_rating"] is None),
        "with_solved": sum(1 for r in records if r["solved_count"] is not None),
        "tagged": sum(1 for r in records if r["problem_tags"]),
        "acmsguru": sum(1 for r in records if r["problemset_source"] == "acmsguru"),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Snapshot the CF problemset.")
    parser.add_argument("--config", type=pathlib.Path, default=None)
    args = parser.parse_args(argv)

    config = load_config(args.config)
    now = dt.datetime.now(dt.UTC)
    fetched_at = now.isoformat(timespec="seconds")
    stamp = now.strftime("%Y%m%dT%H%M%S")

    limiter = RateLimiter(config.api.min_interval_seconds)
    client = CodeforcesClient(
        config.api.base_url,
        limiter,
        timeout=config.api.timeout_seconds,
        max_attempts=config.api.max_attempts,
    )

    print(f"fetching problemset.problems at {fetched_at}")
    records = fetch(client, fetched_at=fetched_at)

    path = config.data_root / "problemset" / f"problemset_{stamp}.jsonl"
    write_snapshot(records, path)

    print(f"wrote {path}")
    for key, value in summarize(records).items():
        print(f"  {key:18} {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
