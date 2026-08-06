"""Normalize the Codeforces landing zone into the silver Parquet dataset.

Input is the collector's output: one gzipped JSONL file per handle, sharded by
the first two hex characters of a digest. Output is a single Parquet dataset
partitioned by submission year.

Six decisions are encoded here, and every one of them is a place where a later
query would otherwise re-derive the answer and drift from its neighbours.

1. Team submissions are KEPT, flagged, and deduplicated. A team submission is
   returned by ``user.status`` for every member, so a row whose team has two
   members in the cohort arrives twice. Measured on the 18,524-user cohort:
   157,917 duplicate rows over 562,977 distinct team submissions. They are
   useless for modelling an individual's ability and are perfectly good
   evidence of a problem's difficulty, so they are flagged rather than dropped
   -- dropping at staging is irreversible, filtering at query time is free.

2. The grain is one row per submission id, chosen deterministically: of the
   duplicates, the row from the alphabetically first collecting handle wins.
   ``dropDuplicates`` would be cheaper and would pick arbitrarily, which makes
   the dataset unreproducible for no gain at this scale.

3. ``collected_via_handle`` is preserved. For a team row it is the only record
   of which member's history the row was actually read from.

4. Tags stay an array. Exploding into a bridge table is the warehouse layer's
   job (dbt), not the lake's; the lake stays close to the source shape.

5. ``problem_id`` is built from ``problem.contestId``, NOT the top-level
   ``contestId``. They differ: the top-level field is the contest the
   submission was made in, the nested one is the contest the problem belongs
   to. A Div. 2 problem solved during a Div. 1 round would otherwise get two
   different ids for the same problem.

6. Booleans are derived once, here. ``is_accepted``, ``is_contest`` and
   ``is_person_level`` are the three predicates every downstream model needs,
   and re-expressing them per query is how definitions drift.

Note on ``participant_type``: PRACTICE submissions are made with unlimited time
and frequently after reading an editorial, so treating them as equivalent to
CONTESTANT responses inflates ability estimates. The raw value is kept
alongside ``is_contest`` so Phase 2 can refine the split (VIRTUAL is
time-boxed but not live) without a re-ingest.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import time
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from .spark import build_session

DEFAULT_DATA_ROOT = pathlib.Path("~/codrona-data").expanduser()
DEFAULT_INPUT = DEFAULT_DATA_ROOT / "raw/codeforces/user_status"
DEFAULT_OUTPUT = DEFAULT_DATA_ROOT / "lake/silver/cf_submissions"
DEFAULT_REPORT_DIR = DEFAULT_DATA_ROOT / "lake/_reports"

# Explicit, never inferred. Inference costs a full extra pass over every file
# and can type a column differently between runs when a shard happens to hold
# only nulls. Fields absent from the source land as null, which is why
# memory_consumed_bytes is declared even though the collector may not store it
# -- the report prints its null rate so an all-null column is visible rather
# than assumed.
SUBMISSION_SCHEMA = StructType(
    [
        StructField("id", LongType()),
        StructField("contestId", IntegerType()),
        StructField("creationTimeSeconds", LongType()),
        StructField("relativeTimeSeconds", LongType()),
        StructField("passedTestCount", IntegerType()),
        StructField("programmingLanguage", StringType()),
        StructField("testset", StringType()),
        StructField("timeConsumedMillis", LongType()),
        StructField("points", DoubleType()),
        StructField("verdict", StringType()),
        StructField(
            "author",
            StructType(
                [
                    StructField("ghost", BooleanType()),
                    StructField("teamId", IntegerType()),
                    StructField("participantType", StringType()),
                    StructField("startTimeSeconds", LongType()),
                    StructField(
                        "members",
                        ArrayType(StructType([StructField("handle", StringType())])),
                    ),
                ]
            ),
        ),
        StructField(
            "problem",
            StructType(
                [
                    StructField("contestId", IntegerType()),
                    StructField("index", StringType()),
                    StructField("name", StringType()),
                    StructField("rating", IntegerType()),
                    StructField("points", DoubleType()),
                    StructField("problemsetName", StringType()),
                    StructField("tags", ArrayType(StringType())),
                    StructField("type", StringType()),
                ]
            ),
        ),
    ]
)

# Captures the handle from .../user_status/<shard>/<handle>.jsonl.gz
HANDLE_FROM_PATH = r"([^/]+)\.jsonl\.gz$"


class HiddenLandingFilesError(RuntimeError):
    """Raised when the landing zone holds files Spark's reader cannot see.

    Hadoop's FileInputFormat skips any path whose basename starts with "_" or
    ".", which is how it ignores _SUCCESS and _temporary markers. Codeforces
    handles may legally start with an underscore, so such a file is read as
    zero rows with no warning, silently biasing the corpus against exactly
    those users. The rule is hardcoded in Hadoop and cannot be configured
    away, so the only fix is a filename that does not lead with those
    characters. This is a hard error with no override: a silent sampling bias
    must never be suppressible by a flag.
    """


def hidden_landing_files(input_dir: pathlib.Path) -> list[pathlib.Path]:
    """Landing files whose basename Spark's reader will silently skip."""
    return sorted(
        path for path in input_dir.glob("*/*.jsonl.gz") if path.name.startswith(("_", "."))
    )


def audit_landing(input_dir: pathlib.Path) -> int:
    """Fail before reading if any landing file is invisible to Spark.

    Returns the number of files on disk so the caller can compare it with the
    number Spark actually read.
    """
    hidden = hidden_landing_files(input_dir)
    if hidden:
        shown = ", ".join(path.name for path in hidden[:5])
        more = f" (+{len(hidden) - 5} more)" if len(hidden) > 5 else ""
        raise HiddenLandingFilesError(
            f"{len(hidden)} landing file(s) start with '_' or '.' and would be "
            f"skipped by Spark, dropping their rows silently: {shown}{more}. "
            f"Rename them before normalizing."
        )
    return len(list(input_dir.glob("*/*.jsonl.gz")))


def landing_glob(input_dir: pathlib.Path, shards: list[str] | None = None) -> list[str]:
    """Glob patterns for the landing zone, optionally narrowed to named shards."""
    if shards:
        return [str(input_dir / shard / "*.jsonl.gz") for shard in shards]
    return [str(input_dir / "*" / "*.jsonl.gz")]


def read_landing(
    spark: SparkSession,
    input_dir: pathlib.Path,
    *,
    shards: list[str] | None = None,
) -> DataFrame:
    """Read the gzipped JSONL landing zone, tagging each row with its source handle."""
    paths = landing_glob(input_dir, shards)
    frame = spark.read.schema(SUBMISSION_SCHEMA).json(paths)
    return frame.withColumn(
        "collected_via_handle",
        F.regexp_extract(F.input_file_name(), HANDLE_FROM_PATH, 1),
    )


def _is_ghost() -> Column:
    return F.coalesce(F.col("author").getField("ghost"), F.lit(False))


def _is_contest() -> Column:
    participant = F.col("author").getField("participantType")
    return F.coalesce(participant == F.lit("CONTESTANT"), F.lit(False))


def _null_count(column: str) -> Column:
    return F.sum(F.col(column).isNull().cast(LongType()))


def _not_null_count(column: str) -> Column:
    return F.sum(F.col(column).isNotNull().cast(LongType()))


def _problem_id() -> Column:
    """Contest problems key on contestId; the acmsguru archive keys on its name.

    Codeforces hosts the old SGU archive with no contestId at all: the problem
    carries problemsetName "acmsguru" and a numeric index. Those rows are real
    problems with real submissions, so they get a namespaced id rather than a
    null that would drop them out of every join.
    """
    problem = F.col("problem")
    contest = problem.getField("contestId")
    index = problem.getField("index")
    problemset = problem.getField("problemsetName")
    return F.when(
        contest.isNotNull() & index.isNotNull(),
        F.concat(contest.cast(StringType()), index),
    ).when(
        problemset.isNotNull() & index.isNotNull(),
        F.concat(problemset, index),
    )


def stage(frame: DataFrame) -> DataFrame:
    """Project the source shape onto the silver column set. No deduplication yet."""
    author = F.col("author")
    problem = F.col("problem")
    members = author.getField("members")
    team_size = F.coalesce(F.size(members), F.lit(0))
    is_person_level = (team_size == F.lit(1)) & (~_is_ghost())
    submitted_at = F.timestamp_seconds(F.col("creationTimeSeconds"))
    handles = F.transform(members, lambda member: member.getField("handle"))

    return frame.select(
        F.col("id").alias("submission_id"),
        _problem_id().alias("problem_id"),
        problem.getField("contestId").alias("problem_contest_id"),
        problem.getField("index").alias("problem_index"),
        problem.getField("name").alias("problem_name"),
        problem.getField("rating").alias("problem_rating"),
        problem.getField("points").alias("problem_points"),
        problem.getField("problemsetName").alias("problemset_name"),
        problem.getField("tags").alias("problem_tags"),
        problem.getField("type").alias("problem_type"),
        F.col("contestId").alias("contest_id"),
        F.when(is_person_level, members.getItem(0).getField("handle")).alias("handle"),
        handles.alias("author_handles"),
        F.col("collected_via_handle"),
        is_person_level.alias("is_person_level"),
        _is_ghost().alias("is_ghost"),
        team_size.alias("team_size"),
        author.getField("teamId").alias("team_id"),
        author.getField("participantType").alias("participant_type"),
        _is_contest().alias("is_contest"),
        F.col("verdict"),
        F.coalesce(F.col("verdict") == F.lit("OK"), F.lit(False)).alias("is_accepted"),
        F.col("programmingLanguage").alias("programming_language"),
        F.col("testset"),
        F.col("passedTestCount").alias("passed_test_count"),
        F.col("timeConsumedMillis").alias("time_consumed_millis"),
        F.col("points").alias("points_scored"),
        F.col("creationTimeSeconds").alias("creation_time_seconds"),
        submitted_at.alias("submitted_at"),
        F.col("relativeTimeSeconds").alias("relative_time_seconds"),
        author.getField("startTimeSeconds").alias("contest_start_time_seconds"),
        F.year(submitted_at).alias("submitted_year"),
    )


def deduplicate(staged: DataFrame) -> DataFrame:
    """One row per submission id, keeping the alphabetically first collecting handle."""
    by_handle = F.col("collected_via_handle").asc()
    order = Window.partitionBy("submission_id").orderBy(by_handle)
    return (
        staged.withColumn("_row_number", F.row_number().over(order))
        .filter(F.col("_row_number") == F.lit(1))
        .drop("_row_number")
    )


def normalize(frame: DataFrame) -> DataFrame:
    """Project to the silver shape, then deduplicate by submission id."""
    return deduplicate(stage(frame))


def summarize(staged: DataFrame, deduped: DataFrame) -> dict[str, Any]:
    """Aggregate counts for the run report. Two passes, one over each frame."""
    raw_rows = staged.count()
    row = deduped.agg(
        F.count(F.lit(1)).alias("unique_rows"),
        F.sum(F.col("is_person_level").cast(LongType())).alias("person_level_rows"),
        F.sum((~F.col("is_person_level")).cast(LongType())).alias("non_person_rows"),
        F.sum(F.col("is_ghost").cast(LongType())).alias("ghost_rows"),
        F.sum((F.col("team_size") > F.lit(1)).cast(LongType())).alias("team_rows"),
        F.sum(F.col("is_accepted").cast(LongType())).alias("accepted_rows"),
        F.sum(F.col("is_contest").cast(LongType())).alias("contest_rows"),
        _null_count("problem_id").alias("null_problem_id"),
        _null_count("problem_rating").alias("null_problem_rating"),
        _not_null_count("problemset_name").alias("acmsguru_rows"),
        F.countDistinct(F.col("team_id")).alias("distinct_teams"),
        _null_count("verdict").alias("null_verdict"),
        F.countDistinct(F.col("problem_id")).alias("distinct_problems"),
        F.countDistinct(F.col("handle")).alias("distinct_person_handles"),
        F.countDistinct(F.col("collected_via_handle")).alias("distinct_source_handles"),
        F.min(F.col("submitted_year")).alias("first_year"),
        F.max(F.col("submitted_year")).alias("last_year"),
    ).collect()[0]

    report = {key: row[key] for key in row.asDict()}
    report["raw_rows"] = raw_rows
    report["duplicate_rows"] = raw_rows - int(report["unique_rows"])
    return report


def write_report(report: dict[str, Any], report_dir: pathlib.Path) -> pathlib.Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%d_%H%M%S")
    path = report_dir / f"cf_submissions_{stamp}.json"
    path.write_text(json.dumps(report, indent=2, sort_keys=True, default=str) + "\n")
    return path


def print_report(report: dict[str, Any]) -> None:
    order = [
        "input_paths",
        "files_on_disk",
        "files_read",
        "raw_rows",
        "duplicate_rows",
        "unique_rows",
        "person_level_rows",
        "non_person_rows",
        "team_rows",
        "ghost_rows",
        "accepted_rows",
        "contest_rows",
        "distinct_problems",
        "distinct_person_handles",
        "distinct_source_handles",
        "null_problem_id",
        "null_problem_rating",
        "null_verdict",
        "acmsguru_rows",
        "distinct_teams",
        "first_year",
        "last_year",
        "output",
        "elapsed_seconds",
    ]
    print("=" * 56)
    for key in order:
        if key not in report:
            continue
        value = report[key]
        rendered = f"{value:,}" if isinstance(value, int) else str(value)
        print(f"{key:<28}{rendered:>28}")
    print("=" * 56)


def run(
    *,
    input_dir: pathlib.Path,
    output_dir: pathlib.Path,
    report_dir: pathlib.Path,
    shards: list[str] | None,
    shuffle_partitions: int,
    driver_memory: str,
) -> dict[str, Any]:
    started = time.time()
    spark = build_session(
        "codrona-normalize-cf",
        driver_memory=driver_memory,
        shuffle_partitions=shuffle_partitions,
    )
    try:
        files_on_disk = audit_landing(input_dir)
        source = read_landing(spark, input_dir, shards=shards)
        staged = stage(source).cache()
        deduped = deduplicate(staged).cache()

        output_dir.parent.mkdir(parents=True, exist_ok=True)
        writer = deduped.write.mode("overwrite").partitionBy("submitted_year")
        writer.parquet(str(output_dir))

        report = summarize(staged, deduped)
        report["input_paths"] = ",".join(landing_glob(input_dir, shards))
        report["files_on_disk"] = files_on_disk
        report["files_read"] = int(report["distinct_source_handles"])
        report["output"] = str(output_dir)
        report["spark_version"] = spark.version
        report["elapsed_seconds"] = round(time.time() - started, 1)
    finally:
        spark.stop()

    path = write_report(report, report_dir)
    report["report_file"] = str(path)
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m codrona_lens.normalize.cf_submissions",
        description="Normalize the Codeforces landing zone into silver Parquet.",
    )
    parser.add_argument("--input", type=pathlib.Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=pathlib.Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--report-dir", type=pathlib.Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument(
        "--shards",
        default=None,
        help="Comma-separated shard prefixes, e.g. 00,01. Omit for the whole zone.",
    )
    parser.add_argument("--shuffle-partitions", type=int, default=64)
    parser.add_argument("--driver-memory", default="4g")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    raw_shards = args.shards.split(",") if args.shards else []
    shards = [s.strip() for s in raw_shards if s.strip()] or None
    report = run(
        input_dir=args.input.expanduser(),
        output_dir=args.output.expanduser(),
        report_dir=args.report_dir.expanduser(),
        shards=shards,
        shuffle_partitions=args.shuffle_partitions,
        driver_memory=args.driver_memory,
    )
    print_report(report)
    print(f"report written to {report['report_file']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
