"""Gates for host-dependent derived values: every engine's session zone is UTC.

``dim_user.registered_at`` is TIMESTAMP WITH TIME ZONE and ``submitted_year`` is
a physical partition column, so ``year()`` resolves against whatever zone the
session happens to carry. A laptop in Asia/Kolkata and a container in UTC
therefore produced different published figures from identical inputs, and every
row count stayed identical while they did - which is exactly why nothing caught
it. The pins that fix it live in three places and, before this module, nothing
asserted any of them existed.

Two properties make these gates real rather than decorative.

They assert the pin rather than the environment. A test that configures UTC in
its own fixture proves that Spark's ``year()`` works under UTC, which is a
property of Spark; it passes unchanged when the pin is deleted from the code it
claims to gate. Each test here first poisons the live session with a non-UTC
zone, then invokes the production constructor, then asserts the poison is gone.
Deleting the pin leaves the poison in place and fails the test on every host,
including a CI runner that is already UTC.

They assert behaviour, not a config string. The epoch below is 20:00 UTC on the
last day of 2019, which is 01:30 on the first day of 2020 in Asia/Kolkata, so
``year()`` returns a different number under each zone. That is the founding
defect reproduced in one row.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

from __future__ import annotations

import pathlib
import shutil
from collections.abc import Iterator

import pytest
import yaml

TZ_KEY = "spark.sql.session.timeZone"
POISON = "Asia/Kolkata"

# 2019-12-31T20:00:00Z, which is 2020-01-01T01:30:00+05:30.
BOUNDARY_EPOCH = 1_577_822_400
YEAR_IN_UTC = 2019
YEAR_IN_POISON = 2020

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
PROFILES = REPO_ROOT / "profiles.yml"

needs_jvm = pytest.mark.skipif(
    shutil.which("java") is None,
    reason="no JVM available; Spark tests need a JDK",
)


def test_boundary_epoch_actually_straddles_the_year() -> None:
    """The fixture value is load-bearing, so it is checked rather than trusted.

    A boundary that reads the same year under both zones would make every test
    below pass against unpinned code.
    """
    import datetime as dt
    import zoneinfo

    moment = dt.datetime.fromtimestamp(BOUNDARY_EPOCH, tz=dt.UTC)
    assert moment.year == YEAR_IN_UTC
    assert moment.astimezone(zoneinfo.ZoneInfo(POISON)).year == YEAR_IN_POISON


def test_every_dbt_target_pins_utc() -> None:
    """dbt reads its zone from profiles.yml, and a new target inherits nothing.

    Structural rather than a grep: a target added later without ``settings``
    fails here instead of publishing figures that depend on whoever ran it.
    """
    assert PROFILES.is_file(), f"no profiles.yml at {PROFILES}"
    document = yaml.safe_load(PROFILES.read_text(encoding="utf-8"))

    targets = {
        f"{profile_name}.{target_name}": target
        for profile_name, profile in document.items()
        if isinstance(profile, dict)
        for target_name, target in profile.get("outputs", {}).items()
    }
    assert targets, "profiles.yml declares no targets"

    for name, target in targets.items():
        settings = target.get("settings")
        assert settings is not None, f"{name} sets no session settings"
        assert settings.get("TimeZone") == "UTC", f"{name} does not pin TimeZone to UTC"


@pytest.fixture
def poisoned_session() -> Iterator[object]:
    """Yield a live Spark session whose zone is deliberately wrong.

    Restoring UTC afterwards matters: the session is shared across the whole
    pytest process, so a failure here must not leak a non-UTC zone into another
    module's year assertions.
    """
    import os

    from pyspark.sql import SparkSession

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    session = (
        SparkSession.builder.appName("codrona-timezone-gate")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config(TZ_KEY, POISON)
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    assert session.conf.get(TZ_KEY) == POISON, "the poison did not take"
    try:
        yield session
    finally:
        session.conf.set(TZ_KEY, "UTC")


def year_of_boundary(session: object) -> int:
    frame = session.sql(  # type: ignore[attr-defined]
        f"select year(timestamp_seconds({BOUNDARY_EPOCH})) as year_read"
    )
    return int(frame.collect()[0]["year_read"])


@needs_jvm
def test_cf_build_session_pins_utc(poisoned_session: object) -> None:
    """The Codeforces normalizer's constructor must reset a wrong zone."""
    from codrona_lens.normalize.spark import build_session

    assert year_of_boundary(poisoned_session) == YEAR_IN_POISON

    built = build_session("codrona-timezone-gate")

    assert built.conf.get(TZ_KEY) == "UTC"
    assert year_of_boundary(built) == YEAR_IN_UTC


@needs_jvm
def test_codenet_build_session_pins_utc(poisoned_session: object) -> None:
    """The CodeNet normalizer builds its own session and needs its own gate."""
    from codrona_lens.codenet.normalize import build_session

    assert year_of_boundary(poisoned_session) == YEAR_IN_POISON

    built = build_session()

    assert built.conf.get(TZ_KEY) == "UTC"
    assert year_of_boundary(built) == YEAR_IN_UTC
