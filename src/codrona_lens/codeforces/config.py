"""Typed view over config/codeforces.toml.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

from __future__ import annotations

import pathlib
import tomllib
from dataclasses import dataclass

DEFAULT_CONFIG_PATH = pathlib.Path("config/codeforces.toml")

# The documented Codeforces limit. Configuration may raise the interval above
# this but never below it.
DOCUMENTED_MIN_INTERVAL = 2.0


@dataclass(frozen=True)
class ApiConfig:
    base_url: str
    min_interval_seconds: float
    page_size: int
    timeout_seconds: float
    max_attempts: int


@dataclass(frozen=True)
class CohortConfig:
    seed: int
    targets: dict[str, int]


@dataclass(frozen=True)
class Config:
    api: ApiConfig
    cohort: CohortConfig
    data_root: pathlib.Path
    keep_problem_name: bool

    @property
    def rated_list_dir(self) -> pathlib.Path:
        return self.data_root / "ratedList"

    @property
    def cohort_dir(self) -> pathlib.Path:
        return self.data_root / "cohort"

    @property
    def user_status_dir(self) -> pathlib.Path:
        return self.data_root / "user_status"

    @property
    def checkpoint_path(self) -> pathlib.Path:
        return self.data_root / "checkpoint.db"


def load_config(path: pathlib.Path | None = None) -> Config:
    path = path or DEFAULT_CONFIG_PATH
    with path.open("rb") as fh:
        raw = tomllib.load(fh)

    api_raw = raw["api"]
    interval = float(api_raw["min_interval_seconds"])
    if interval < DOCUMENTED_MIN_INTERVAL:
        raise ValueError(
            f"min_interval_seconds={interval} is below the documented Codeforces "
            f"limit of {DOCUMENTED_MIN_INTERVAL}s per request"
        )

    api = ApiConfig(
        base_url=str(api_raw["base_url"]).rstrip("/"),
        min_interval_seconds=interval,
        page_size=int(api_raw["page_size"]),
        timeout_seconds=float(api_raw["timeout_seconds"]),
        max_attempts=int(api_raw["max_attempts"]),
    )

    cohort_raw = raw["cohort"]
    cohort = CohortConfig(
        seed=int(cohort_raw["seed"]),
        targets={str(k): int(v) for k, v in cohort_raw["targets"].items()},
    )

    data_root = pathlib.Path(str(raw["paths"]["data_root"])).expanduser()
    keep_name = bool(raw.get("collection", {}).get("keep_problem_name", True))

    return Config(
        api=api,
        cohort=cohort,
        data_root=data_root,
        keep_problem_name=keep_name,
    )
