"""Cohort selection decides whose submissions the models ever see."""

from __future__ import annotations

from typing import Any

from codrona_lens.codeforces.cohort import (
    band_counts,
    band_for,
    build_cohort,
)


def make_users(spec: list[tuple[int, int]]) -> list[dict[str, Any]]:
    users: list[dict[str, Any]] = []
    n = 0
    for rating, count in spec:
        for _ in range(count):
            n += 1
            users.append({"handle": f"user{n:05d}", "rating": rating})
    return users


def test_band_boundaries() -> None:
    assert band_for(-19) == "newbie"
    assert band_for(1199) == "newbie"
    assert band_for(1200) == "pupil"
    assert band_for(1399) == "pupil"
    assert band_for(1400) == "specialist"
    assert band_for(1900) == "candidate_master"
    assert band_for(2400) == "grandmaster"
    assert band_for(3000) == "legendary_grandmaster"
    assert band_for(3857) == "legendary_grandmaster"


def test_zero_target_takes_the_whole_band() -> None:
    users = make_users([(800, 50), (2500, 7)])
    cohort = build_cohort(users, {"newbie": 10, "grandmaster": 0}, seed=1)
    counts = band_counts(cohort)
    assert counts["newbie"] == 10
    assert counts["grandmaster"] == 7


def test_target_larger_than_band_takes_everyone() -> None:
    users = make_users([(3100, 4)])
    cohort = build_cohort(users, {"legendary_grandmaster": 999}, seed=1)
    assert len(cohort) == 4


def test_selection_is_reproducible_for_a_given_seed() -> None:
    users = make_users([(800, 200)])
    a = build_cohort(users, {"newbie": 25}, seed=20260806)
    b = build_cohort(users, {"newbie": 25}, seed=20260806)
    c = build_cohort(users, {"newbie": 25}, seed=1)
    assert [m.handle for m in a] == [m.handle for m in b]
    assert [m.handle for m in a] != [m.handle for m in c]


def test_users_without_a_rating_are_ignored() -> None:
    users: list[dict[str, Any]] = [
        {"handle": "a", "rating": 800},
        {"handle": "b"},
        {"rating": 900},
    ]
    cohort = build_cohort(users, {}, seed=1)
    assert [m.handle for m in cohort] == ["a"]


def test_upper_bands_are_never_thinned_by_a_low_newbie_target() -> None:
    users = make_users([(800, 5000), (1950, 40), (2700, 6)])
    cohort = build_cohort(users, {"newbie": 100}, seed=7)
    counts = band_counts(cohort)
    assert counts["newbie"] == 100
    assert counts["candidate_master"] == 40
    assert counts["international_grandmaster"] == 6
