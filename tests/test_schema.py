"""The whitelist is the legal envelope. If these fail, we are holding more than we said."""

from __future__ import annotations

from codrona_lens.codeforces.schema import (
    PARTY_FIELDS,
    PROBLEM_FIELDS,
    SUBMISSION_FIELDS,
    is_person_level,
    project_submission,
)

SAMPLE = {
    "id": 123456789,
    "contestId": 1234,
    "creationTimeSeconds": 1700000000,
    "relativeTimeSeconds": 3600,
    "problem": {
        "contestId": 1234,
        "index": "A",
        "name": "Some Problem",
        "type": "PROGRAMMING",
        "rating": 1200,
        "tags": ["greedy", "math"],
        "statement": "SHOULD NEVER BE RETAINED",
    },
    "author": {
        "contestId": 1234,
        "members": [{"handle": "someone"}],
        "participantType": "CONTESTANT",
        "ghost": False,
        "room": 42,
        "teamName": "Team Of Real People",
        "startTimeSeconds": 1699999000,
    },
    "programmingLanguage": "GNU C++20",
    "verdict": "OK",
    "testset": "TESTS",
    "passedTestCount": 30,
    "timeConsumedMillis": 62,
    "memoryConsumedMillis": 1024000,
    "unexpectedNewApiField": "must not survive",
}


def test_projection_drops_everything_outside_the_whitelist() -> None:
    out = project_submission(SAMPLE)

    assert "unexpectedNewApiField" not in out
    assert set(out) <= SUBMISSION_FIELDS | {"problem", "author"}
    assert set(out["problem"]) <= PROBLEM_FIELDS
    assert set(out["author"]) <= PARTY_FIELDS | {"members"}


def test_statement_text_never_survives_projection() -> None:
    out = project_submission(SAMPLE)
    assert "statement" not in out["problem"]


def test_team_name_and_room_are_dropped() -> None:
    out = project_submission(SAMPLE)
    assert "teamName" not in out["author"]
    assert "room" not in out["author"]


def test_problem_name_can_be_withheld() -> None:
    kept = project_submission(SAMPLE, keep_problem_name=True)
    dropped = project_submission(SAMPLE, keep_problem_name=False)
    assert kept["problem"]["name"] == "Some Problem"
    assert "name" not in dropped["problem"]


def test_facts_are_retained() -> None:
    out = project_submission(SAMPLE)
    assert out["verdict"] == "OK"
    assert out["problem"]["rating"] == 1200
    assert out["problem"]["tags"] == ["greedy", "math"]
    assert out["author"]["members"] == [{"handle": "someone"}]


def test_person_level_accepts_a_single_named_author() -> None:
    assert is_person_level(SAMPLE) is True


def test_person_level_rejects_ghosts_and_teams() -> None:
    ghost = {"author": {"ghost": True, "members": [{"handle": "x"}]}}
    team = {"author": {"ghost": False, "members": [{"handle": "a"}, {"handle": "b"}]}}
    empty = {"author": {"ghost": False, "members": []}}
    assert is_person_level(ghost) is False
    assert is_person_level(team) is False
    assert is_person_level(empty) is False
