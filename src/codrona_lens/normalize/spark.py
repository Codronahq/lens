"""Spark session construction for Codrona Lens normalization jobs.

Three settings here are deliberate and load-bearing; none of them are defaults.

SPARK_LOCAL_IP is pinned to loopback. WSL resolves the machine hostname to a
127.0.1.1 alias and Spark warns about it on every start; the warning is benign
under ``local[*]`` but precedes real bind failures once anything binds an
external port. Pinning removes the class rather than the symptom.

Driver memory is passed through PYSPARK_SUBMIT_ARGS, not SparkConf. In local
mode the driver JVM is the same process that reads the config, so it is already
running with its default heap by the time ``.config("spark.driver.memory", ...)``
is evaluated: that call is silently ignored and the job spills far earlier than
expected. This is the single most common local-mode misconfiguration.

The session time zone is pinned to UTC because ``submitted_year`` is a physical
partition column. Deriving it under a machine-local zone would put the same
submission in different partitions on different hosts, which makes the lake
non-reproducible for the sake of nothing.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

from __future__ import annotations

import os
import pathlib

from pyspark.sql import SparkSession

# Ubuntu's openjdk-17 layout. java-1.17.0-openjdk-amd64 is a symlink to this.
DEFAULT_JAVA_HOME = "/usr/lib/jvm/java-17-openjdk-amd64"


def _ensure_java_home() -> None:
    """Set JAVA_HOME only if the caller has not, and only if the path exists."""
    if os.environ.get("JAVA_HOME"):
        return
    candidate = pathlib.Path(DEFAULT_JAVA_HOME)
    if candidate.is_dir():
        os.environ["JAVA_HOME"] = str(candidate)


def build_session(
    app_name: str,
    *,
    driver_memory: str = "4g",
    shuffle_partitions: int = 64,
) -> SparkSession:
    """Build (or attach to) a local Spark session configured for this project."""
    _ensure_java_home()
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault(
        "PYSPARK_SUBMIT_ARGS",
        f"--driver-memory {driver_memory} pyspark-shell",
    )

    session = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    return session
