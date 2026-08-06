"""The legal envelope, expressed as code rather than as a note in a document.

Only enumerated fields reach disk. A test asserts that nothing outside the
whitelist survives projection, so widening what we retain is a deliberate edit
to this file and never an accident of an API response gaining a key.

Deliberately dropped: author.teamName (user-generated content we have no reason
to hold) and author.room (meaningless outside a live contest).

SPDX-License-Identifier: AGPL-3.0-or-later
"""

from __future__ import annotations

from typing import Any

SUBMISSION_FIELDS = frozenset(
    {
        "id",
        "contestId",
        "creationTimeSeconds",
        "relativeTimeSeconds",
        "programmingLanguage",
        "verdict",
        "testset",
        "passedTestCount",
        "timeConsumedMillis",
        "memoryConsumedMillis",
        "points",
    }
)

PROBLEM_FIELDS = frozenset(
    {
        "contestId",
        "problemsetName",
        "index",
        "name",
        "type",
        "points",
        "rating",
        "tags",
    }
)

PARTY_FIELDS = frozenset(
    {
        "participantType",
        "teamId",
        "ghost",
        "startTimeSeconds",
    }
)

MEMBER_FIELDS = frozenset({"handle"})


def _pick(source: dict[str, Any], allowed: frozenset[str]) -> dict[str, Any]:
    return {k: v for k, v in source.items() if k in allowed}


def project_submission(
    submission: dict[str, Any],
    *,
    keep_problem_name: bool = True,
) -> dict[str, Any]:
    """Reduce an API submission object to the fields we are willing to hold."""
    out = _pick(submission, SUBMISSION_FIELDS)

    problem = submission.get("problem")
    if isinstance(problem, dict):
        allowed = PROBLEM_FIELDS if keep_problem_name else PROBLEM_FIELDS - {"name"}
        out["problem"] = _pick(problem, allowed)

    author = submission.get("author")
    if isinstance(author, dict):
        party = _pick(author, PARTY_FIELDS)
        members = author.get("members")
        if isinstance(members, list):
            party["members"] = [_pick(m, MEMBER_FIELDS) for m in members if isinstance(m, dict)]
        out["author"] = party

    return out


def is_person_level(submission: dict[str, Any]) -> bool:
    """True when a row can honestly be attributed to one identified person.

    The corpus claim in codrona.md is person-level, so ghost submissions
    (imported from another judge, no Codeforces account behind them) and team
    submissions (several handles, one row) must not be counted as such. They are
    retained and tagged downstream, never silently folded into the headline.
    """
    author = submission.get("author")
    if not isinstance(author, dict):
        return False
    if author.get("ghost") is True:
        return False
    members = author.get("members")
    if not isinstance(members, list) or len(members) != 1:
        return False
    return isinstance(members[0], dict) and bool(members[0].get("handle"))
