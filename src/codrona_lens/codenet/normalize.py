"""Normalize IBM Project CodeNet metadata into the silver lake.

Mirrors the Codeforces pipeline shape deliberately: raw -> normalize -> silver
Parquet partitioned by year -> dbt staging. CodeNet is small enough that DuckDB
could do this in one query, but codrona.md section 4 commits the feature store to
PySpark and the lake paths mirror the eventual S3 keys exactly, so a source that
skips silver becomes the exception that breaks the layout later.

CodeNet is a SEPARATE SPINE, not a second loader into the Codeforces tables. Its
users are anonymised and can never join to anything, its problems carry no
difficulty, and its status vocabulary shares no value with Codeforces'. Conforming
the two problem spaces waits for IRT to produce a cross-judge difficulty
equivalence; asserting one now would invent an equivalence we have not earned.

Facts measured over all 13,916,868 rows before this was written, each of which
shapes a column below:

* ``submission_id`` is globally unique, so it is the key with no compounding.
* ``problem_id`` agrees with the filename on every row.
* ``cpu_time`` and ``memory`` are NULL together on 415,826 rows and never
  separately - but that is a JUDGE CONVENTION, not a fact about the submission.
  Every one of AtCoder's 415,516 Compile Error rows is null while none of AIZU's
  140,743 are. A ``was_executed`` flag derived from nullness would therefore be
  wrong for half the corpus and is deliberately not built.
* 852 rows carry a NEGATIVE cpu_time on statuses that certainly did execute
  (Runtime Error, Memory Limit Exceeded, Compile Error). A negative is a corrupt
  measurement, not an absent one, so it is nulled and flagged rather than
  clamped to zero - which would fabricate a real reading of zero milliseconds.
* ``accuracy`` is AIZU-only: present on 1,868,584 of 1,955,837 AIZU rows and on
  2 of 11,961,031 AtCoder rows. It is a ``passed/total`` fraction, richer than a
  boolean, and any model using it must condition on judge or it will learn that
  AtCoder problems have no test signal.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

from codrona_lens.codenet.census import classify_unread

PROBLEM_LIST = "problem_list.csv"

# The complete status vocabulary, measured over every row rather than taken from
# IBM's README - which lists "Internal Error" where the data says "Internal
# error", and omits "Query Limit Exceeded" entirely. Three of these twelve never
# appeared in a 200-file sample; only the full scan found them.
#
# The mapping matches the Codeforces policy in codrona.md section 6: the raw
# status is never discarded, verdict_class is the coarse grouping, and
# is_evidence marks whether the row says anything about ability. CodeNet has no
# partial class - AIZU and AtCoder are all-or-nothing in this snapshot.
STATUS_CLASS: dict[str, tuple[str, bool]] = {
    "Accepted": ("accepted", True),
    "Wrong Answer": ("rejected", True),
    "Runtime Error": ("rejected", True),
    "Time Limit Exceeded": ("rejected", True),
    "Compile Error": ("rejected", True),
    "WA: Presentation Error": ("rejected", True),
    "Memory Limit Exceeded": ("rejected", True),
    "Output Limit Exceeded": ("rejected", True),
    "Query Limit Exceeded": ("rejected", True),
    "Judge Not Available": ("unjudged", False),
    "Internal error": ("unjudged", False),
    "Judge System Error": ("unjudged", False),
}

SUBMISSION_SCHEMA = StructType(
    [
        StructField("submission_id", StringType(), nullable=True),
        StructField("problem_id", StringType(), nullable=True),
        StructField("user_id", StringType(), nullable=True),
        StructField("date", LongType(), nullable=True),
        StructField("language", StringType(), nullable=True),
        StructField("original_language", StringType(), nullable=True),
        StructField("filename_ext", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("cpu_time", LongType(), nullable=True),
        StructField("memory", LongType(), nullable=True),
        StructField("code_size", LongType(), nullable=True),
        StructField("accuracy", StringType(), nullable=True),
    ]
)

PROBLEM_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("dataset", StringType(), nullable=True),
        StructField("time_limit", LongType(), nullable=True),
        StructField("memory_limit", LongType(), nullable=True),
        StructField("rating", StringType(), nullable=True),
        StructField("tags", StringType(), nullable=True),
        StructField("complexity", StringType(), nullable=True),
    ]
)


@dataclass
class NormalizeReport:
    """Counts written alongside every build.

    Several fields exist only to be compared against each other. Both provenance
    bugs in this phase were found by two numbers that should have matched being
    off by one, and by nothing else, so a report that cannot disagree with itself
    is not worth writing.
    """

    normalized_at: str
    metadata_dir: str
    output_path: str
    files_on_disk: int
    files_read: int
    empty_problem_files: list[str]
    rows: int
    distinct_submission_ids: int
    distinct_problems: int
    distinct_users: int
    problems_in_index: int
    accepted_rows: int
    evidence_rows: int
    unknown_status_rows: int
    corrupt_timing_rows: int
    rows_with_tests: int
    judge_counts: dict[str, int]
    verdict_class_counts: dict[str, int]
    first_year: int
    last_year: int
    elapsed_seconds: float


def build_session(driver_memory: str = "4g") -> SparkSession:
    """Create a local-mode session with the three settings that are not optional.

    ``spark.driver.memory`` is set by the caller through PYSPARK_SUBMIT_ARGS
    because in local mode the driver JVM is already running by the time SparkConf
    is read, so setting it here would be silently ignored. The timezone is pinned
    to UTC because a year partition derived under a machine-local zone puts the
    same row in different partitions on different hosts.
    """
    return (
        SparkSession.builder.appName("codrona-codenet-normalize")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def metadata_files(metadata_dir: Path) -> list[Path]:
    """Every per-problem CSV, excluding the index.

    ``iterdir`` rather than ``glob``, and an explicit list rather than a
    directory path: Spark's FileInputFormat silently skips basenames starting
    with ``_`` or ``.``, and the only defence that has ever worked is comparing
    files-on-disk against files-actually-read. CodeNet's ``p#####.csv`` names
    cannot trigger that filter, but the guard costs nothing and the assumption
    is exactly the kind that stops being true without warning.
    """
    return sorted(
        path
        for path in metadata_dir.iterdir()
        if path.is_file() and path.suffix == ".csv" and path.name != PROBLEM_LIST
    )


def _status_mapping_expr(column: Column) -> tuple[Column, Column]:
    """Map a raw status to (verdict_class, is_evidence).

    An unrecognised status falls to 'unknown' and is NOT evidence. Defaulting it
    to 'rejected' would let a status CodeNet adds later corrupt calibration with
    no visible symptom; a warehouse test fires on 'unknown' instead.
    """
    cls = F.lit("unknown")
    ev = F.lit(False)
    for status, (verdict_class, evidence) in STATUS_CLASS.items():
        hit = column == F.lit(status)
        cls = F.when(hit, F.lit(verdict_class)).otherwise(cls)
        ev = F.when(hit, F.lit(evidence)).otherwise(ev)
    return cls, ev


def _uri_to_path(uri: str) -> str:
    """Turn a Spark input_file_name() URI back into a filesystem path."""
    parsed = urlparse(uri)
    return unquote(parsed.path) if parsed.scheme else unquote(uri)


def _source_files(frame: DataFrame) -> list[str]:
    rows = frame.select("source_file").distinct().collect()
    return [str(row["source_file"]) for row in rows]


def read_metadata(spark: SparkSession, files: list[Path]) -> DataFrame:
    """Read every per-problem CSV with an explicit schema."""
    paths = [str(path) for path in files]
    return (
        spark.read.option("header", "true")
        .option("mode", "FAILFAST")
        .schema(SUBMISSION_SCHEMA)
        .csv(paths)
        .withColumn("source_file", F.input_file_name())
    )


def read_problem_index(spark: SparkSession, metadata_dir: Path) -> DataFrame:
    """Read problem_list.csv, which carries the judge each problem came from."""
    return (
        spark.read.option("header", "true")
        .schema(PROBLEM_SCHEMA)
        .csv(str(metadata_dir / PROBLEM_LIST))
        .select(
            F.col("id").alias("problem_id"),
            F.col("name").alias("problem_name"),
            F.col("dataset").alias("judge"),
            F.col("time_limit").alias("time_limit_ms"),
            F.col("memory_limit").alias("memory_limit_kb"),
        )
    )


def normalize(submissions: DataFrame, problems: DataFrame) -> DataFrame:
    """Derive every silver column from the raw metadata."""
    verdict_class, is_evidence = _status_mapping_expr(F.col("status"))

    accuracy = F.col("accuracy")
    has_accuracy = accuracy.isNotNull() & (accuracy != F.lit(""))
    parts = F.split(accuracy, "/")
    tests_passed = F.when(has_accuracy, parts.getItem(0).cast("int"))
    tests_total = F.when(has_accuracy, parts.getItem(1).cast("int"))

    # A negative cpu_time is a corrupt reading on a row that did execute, so it
    # is nulled and flagged. Clamping to zero would fabricate a real measurement
    # of zero milliseconds, which is a value the model would happily learn from.
    corrupt_timing = F.col("cpu_time").isNotNull() & (F.col("cpu_time") < F.lit(0))
    absent = F.lit(None).cast("long")
    cpu_time = F.when(corrupt_timing, absent).otherwise(F.col("cpu_time"))

    submitted_at = F.to_timestamp(F.col("date"))

    return submissions.join(problems, on="problem_id", how="left").select(
        F.col("submission_id"),
        F.col("problem_id"),
        F.col("user_id"),
        F.col("judge"),
        F.col("problem_name"),
        F.col("time_limit_ms"),
        F.col("memory_limit_kb"),
        F.col("status"),
        verdict_class.alias("verdict_class"),
        is_evidence.alias("is_evidence"),
        (F.col("status") == F.lit("Accepted")).alias("is_accepted"),
        F.col("language"),
        F.col("original_language"),
        F.col("filename_ext"),
        cpu_time.alias("cpu_time_ms"),
        corrupt_timing.alias("has_corrupt_timing"),
        F.col("memory").alias("memory_kb"),
        F.col("code_size").alias("code_size_bytes"),
        F.col("accuracy"),
        tests_passed.alias("tests_passed"),
        tests_total.alias("tests_total"),
        submitted_at.alias("submitted_at"),
        F.year(submitted_at).alias("submitted_year"),
        F.col("source_file"),
    )


def summarize(frame: DataFrame) -> dict[str, Any]:
    """One pass over the normalized frame for every reported count."""
    row = frame.agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct("submission_id").alias("distinct_submission_ids"),
        F.countDistinct("problem_id").alias("distinct_problems"),
        F.countDistinct("user_id").alias("distinct_users"),
        F.countDistinct("source_file").alias("files_read"),
        F.sum(F.col("is_accepted").cast("long")).alias("accepted_rows"),
        F.sum(F.col("is_evidence").cast("long")).alias("evidence_rows"),
        F.sum((F.col("verdict_class") == F.lit("unknown")).cast("long")).alias(
            "unknown_status_rows"
        ),
        F.sum(F.col("has_corrupt_timing").cast("long")).alias("corrupt_timing_rows"),
        F.sum(F.col("tests_total").isNotNull().cast("long")).alias("rows_with_tests"),
        F.min("submitted_year").alias("first_year"),
        F.max("submitted_year").alias("last_year"),
    ).collect()[0]
    return row.asDict()


def _counts_by(frame: DataFrame, column: str) -> dict[str, int]:
    rows = frame.groupBy(column).count().collect()
    return {str(row[column]): int(row["count"]) for row in rows}


def run(
    metadata_dir: Path,
    output_path: Path,
    reports_dir: Path | None = None,
    driver_memory: str = "4g",
) -> NormalizeReport:
    """Normalize the metadata layer into partitioned silver Parquet."""
    started = datetime.now(tz=UTC)
    files = metadata_files(metadata_dir)
    if not files:
        msg = f"no per-problem metadata CSVs found in {metadata_dir}"
        raise FileNotFoundError(msg)

    spark = build_session(driver_memory)
    try:
        submissions = read_metadata(spark, files)
        problems = read_problem_index(spark, metadata_dir)
        problems_in_index = int(problems.count())

        normalized = normalize(submissions, problems).cache()
        stats = summarize(normalized)

        # input_file_name() returns a URI, not a filesystem path: the scheme
        # prefix is always present and any literal % in a name comes back
        # percent-encoded. CodeNet's p#####.csv names cannot carry one, but
        # decoding unconditionally costs nothing and this exact assumption -
        # that a path is a path - already cost a full-zone rebuild once.
        seen = {_uri_to_path(uri) for uri in _source_files(normalized)}
        empty_files = classify_unread(files, seen)
        files_read = len(seen) + len(empty_files)
        if files_read != len(files):
            msg = (
                f"file gap: {len(files)} metadata CSVs on disk but Spark "
                f"accounted for {files_read}. Refusing to write silver from "
                "an incomplete read."
            )
            raise RuntimeError(msg)

        rows = int(stats["rows"])
        distinct_ids = int(stats["distinct_submission_ids"])
        if rows != distinct_ids:
            msg = (
                f"submission_id is not unique: {rows:,} rows but "
                f"{distinct_ids:,} distinct ids. The key design assumes uniqueness."
            )
            raise RuntimeError(msg)

        judge_counts = _counts_by(normalized, "judge")
        verdict_counts = _counts_by(normalized, "verdict_class")

        normalized.drop("source_file").write.mode("overwrite").partitionBy(
            "submitted_year"
        ).parquet(str(output_path))
        normalized.unpersist()
    finally:
        spark.stop()

    elapsed = (datetime.now(tz=UTC) - started).total_seconds()
    report = NormalizeReport(
        normalized_at=started.isoformat(),
        metadata_dir=str(metadata_dir),
        output_path=str(output_path),
        files_on_disk=len(files),
        files_read=files_read,
        empty_problem_files=empty_files,
        rows=rows,
        distinct_submission_ids=distinct_ids,
        distinct_problems=int(stats["distinct_problems"]),
        distinct_users=int(stats["distinct_users"]),
        problems_in_index=problems_in_index,
        accepted_rows=int(stats["accepted_rows"]),
        evidence_rows=int(stats["evidence_rows"]),
        unknown_status_rows=int(stats["unknown_status_rows"]),
        corrupt_timing_rows=int(stats["corrupt_timing_rows"]),
        rows_with_tests=int(stats["rows_with_tests"]),
        judge_counts=judge_counts,
        verdict_class_counts=verdict_counts,
        first_year=int(stats["first_year"]),
        last_year=int(stats["last_year"]),
        elapsed_seconds=elapsed,
    )

    if reports_dir is not None:
        reports_dir.mkdir(parents=True, exist_ok=True)
        stamp = started.strftime("%Y%m%d_%H%M%S")
        path = reports_dir / f"codenet_submissions_{stamp}.json"
        payload: dict[str, Any] = asdict(report)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")

    return report


def format_report(report: NormalizeReport) -> str:
    lines = [
        f"files on disk         {report.files_on_disk:,}",
        f"files read            {report.files_read:,}",
        f"rows                  {report.rows:,}",
        f"distinct ids          {report.distinct_submission_ids:,}",
        f"distinct problems     {report.distinct_problems:,}",
        f"problems in index     {report.problems_in_index:,}",
        f"distinct users        {report.distinct_users:,}",
        f"accepted              {report.accepted_rows:,}",
        f"evidence              {report.evidence_rows:,}",
        f"unknown status        {report.unknown_status_rows:,}",
        f"corrupt timing        {report.corrupt_timing_rows:,}",
        f"rows with tests       {report.rows_with_tests:,}",
        f"years                 {report.first_year} - {report.last_year}",
        f"elapsed               {report.elapsed_seconds:.0f}s",
        "",
        "by judge",
    ]
    for judge, n in sorted(report.judge_counts.items()):
        lines.append(f"  {judge:<20} {n:,}")
    lines.append("")
    lines.append("by verdict class")
    for name, n in sorted(report.verdict_class_counts.items()):
        lines.append(f"  {name:<20} {n:,}")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata-dir", type=Path, required=True)
    parser.add_argument("--output-path", type=Path, required=True)
    parser.add_argument("--reports-dir", type=Path, default=None)
    parser.add_argument("--driver-memory", default="4g")
    args = parser.parse_args()

    report = run(
        args.metadata_dir,
        args.output_path,
        args.reports_dir,
        args.driver_memory,
    )
    print(format_report(report))
    if report.unknown_status_rows:
        print(
            f"\nWARNING: {report.unknown_status_rows:,} rows carry a status "
            "outside the measured vocabulary. Classify it deliberately before "
            "the skill model reads this build."
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
