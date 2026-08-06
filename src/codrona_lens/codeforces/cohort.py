"""Stratified cohort selection.

Sampling policy is the highest-leverage decision in this whole collector. Taking
the strongest users biases every difficulty estimate toward people who are not
our users; sampling uniformly leaves hard problems with too few responses for
their difficulty to be identifiable at all. Equal-ish counts per rating band,
with the sparse upper bands taken whole, is what gives 2PL IRT responses across
the ability range for every problem.

The seed lives in config and is committed, so the cohort is regenerable and any
number resting on it is reproducible from the repository.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

from __future__ import annotations

import json
import pathlib
import random
from dataclasses import dataclass
from typing import Any

# (name, inclusive lower bound, exclusive upper bound). Bounds are the
# Codeforces rating tiers, which are also the product's colour ladder.
BANDS: tuple[tuple[str, int, int], ...] = (
    ("newbie", -100_000, 1200),
    ("pupil", 1200, 1400),
    ("specialist", 1400, 1600),
    ("expert", 1600, 1900),
    ("candidate_master", 1900, 2100),
    ("master", 2100, 2300),
    ("international_master", 2300, 2400),
    ("grandmaster", 2400, 2600),
    ("international_grandmaster", 2600, 3000),
    ("legendary_grandmaster", 3000, 1_000_000),
)

BAND_NAMES: tuple[str, ...] = tuple(name for name, _, _ in BANDS)


def band_for(rating: int) -> str:
    """Rating to band name. Negative ratings exist on Codeforces and land in newbie."""
    for name, low, high in BANDS:
        if low <= rating < high:
            return name
    return BANDS[-1][0]


@dataclass(frozen=True)
class CohortMember:
    handle: str
    band: str
    rating: int


def load_rated_list(path: pathlib.Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8") as fh:
        payload = json.load(fh)
    if payload.get("status") != "OK":
        raise ValueError(f"{path}: rated list payload is not OK")
    return list(payload["result"])


def latest_rated_list(directory: pathlib.Path) -> pathlib.Path:
    candidates = sorted(directory.glob("ratedList_*.json"))
    if not candidates:
        raise FileNotFoundError(f"no ratedList_*.json under {directory}")
    return candidates[-1]


def build_cohort(
    users: list[dict[str, Any]],
    targets: dict[str, int],
    *,
    seed: int,
) -> list[CohortMember]:
    """Stratified sample. A target of 0 (or missing) means take the whole band."""
    by_band: dict[str, list[CohortMember]] = {name: [] for name in BAND_NAMES}

    for user in users:
        handle = user.get("handle")
        rating = user.get("rating")
        if not isinstance(handle, str) or not isinstance(rating, int):
            continue
        band = band_for(rating)
        by_band[band].append(CohortMember(handle=handle, band=band, rating=rating))

    rng = random.Random(seed)
    selected: list[CohortMember] = []

    for name in BAND_NAMES:
        members = sorted(by_band[name], key=lambda m: m.handle)
        target = int(targets.get(name, 0))
        if target <= 0 or target >= len(members):
            selected.extend(members)
            continue
        selected.extend(rng.sample(members, target))

    selected.sort(key=lambda m: (BAND_NAMES.index(m.band), m.handle))
    return selected


def band_counts(members: list[CohortMember]) -> dict[str, int]:
    counts = dict.fromkeys(BAND_NAMES, 0)
    for member in members:
        counts[member.band] += 1
    return counts


def write_cohort(
    members: list[CohortMember],
    path: pathlib.Path,
    *,
    seed: int,
    source: str,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "seed": seed,
        "source": source,
        "count": len(members),
        "band_counts": band_counts(members),
        "members": [{"handle": m.handle, "band": m.band, "rating": m.rating} for m in members],
    }
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)
        fh.write("\n")
    tmp.replace(path)
