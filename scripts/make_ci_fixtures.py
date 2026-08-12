#!/usr/bin/env python3
"""Generate synthetic warehouse fixtures so CI can run a real ``dbt build``.

WHY THIS EXISTS. G8 targets "dbt tests at error severity: 100% pass" and runs
"every DAG execution, before publish_marts". Until now CI ran only ``dbt
parse``, which compiles models and validates YAML but executes nothing, so all
121 data tests ran on one laptop and a broken model reached ``main`` whenever
someone forgot. A documented gate with no mechanism reads as green because
nothing ever contradicts it.

THE FIXTURES ARE INVENTED, NEVER SAMPLED. Every handle, id and name here is
made up. Copying even a hundred real rows out of the lake would put Codeforces
handles in a public repository, and LEGAL.md's publication boundary is explicit
that a local warehouse is not publication while a release is - the corpus goes
out through an explicit column allowlist or not at all. A test fixture is not
an exception to that; it is the easiest way to breach it by accident.

WHAT THIS CAN AND CANNOT GATE. Structural tests run for real: SCD-2 invariants,
key ambiguity, referential integrity, verdict and status enums, the
CodeContests partition rule, dimension coverage. Four tests pin counts of the
real world (13,916,868 CodeNet rows, 23,607,105 Codeforces rows, 11,809
problemset rows, 2,549 zerotrac records) and cannot pass against invented data;
they carry the ``real_data`` tag and CI excludes them. That exclusion is the
honest boundary of what CI proves, and it is narrow: 4 tests of 121.

THE FIXTURES MUST SATISFY EVERY NON-EXCLUDED TEST. That is the point and also
the trap - a fixture that quietly violates a constraint turns a real gate into
a permanently red build that someone will eventually disable. Constraints
encoded below, each traced to the test that enforces it:

  problem_id equals contestId || index, with no two (contest, index) pairs
    rendering to one key            -> assert_problem_key_unambiguous
  every verdict is a known value    -> assert_no_unknown_verdict
  every CodeNet status is known     -> assert_codenet_no_unknown_status
  CodeContests ids match p#####     -> assert_codecontests_partition
  and resolve in the problem index  -> assert_codecontests_ids_resolve
  every collected handle is rated   -> relationships on dim_user
  ratings agree with the problemset -> assert_problemset_ratings_agree

RUNNING THIS LOCALLY NEEDS THE PARSE CACHE CLEARED FIRST. dbt's
``target/partial_parse.msgpack`` survives a ``--target`` switch, so models can
render with env_var values captured under the previous target while others pick
up the new ones - a single run reading fixture paths for one source and
``~/codrona-data`` for another, which surfaces as a test failure that looks like
a data bug and is not. Delete ``target/partial_parse.msgpack`` (or pass
``--no-partial-parse``) before reproducing CI locally. CI itself is immune: a
clean checkout has no cache, which is why CI can be green while the same command
fails on a laptop.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

from __future__ import annotations

import argparse
import json
import pathlib
from typing import Any

import duckdb

SNAPSHOT_DATE = "2026-08-06"
PROBLEMSET_STAMP = "20260812T004059"
ZEROTRAC_SHA = "881a239306ce7a339e32e7825cdb9c00fead00f1"
CODECONTESTS_SHA = "802411c3010cb00d1b05bad57ca77365a3c699d6"

# Invented handles. Deliberately unlike real Codeforces handles.
HANDLES = ["fixture_alpha", "fixture_beta", "fixture_gamma", "fixture_delta"]

# (contest_id, index, name, rating, tags). Chosen so no two rows collide when
# contest_id and index are concatenated without a separator.
PROBLEMS: list[tuple[int, str, str, int | None, list[str]]] = [
    (1001, "A", "Fixture Sum", 800, ["implementation", "math"]),
    (1001, "B", "Fixture Sort", 1200, ["sortings"]),
    (1002, "A", "Fixture Graph", 1600, ["dfs and similar", "graphs"]),
    (1002, "C", "Fixture Tree", 2100, ["trees", "dp"]),
    (1003, "A", "Fixture Unrated", None, ["brute force"]),
]

VERDICTS = ["OK", "WRONG_ANSWER", "TIME_LIMIT_EXCEEDED", "PARTIAL", "SKIPPED"]

CODENET_PROBLEMS = ["p00001", "p00002", "p00003"]
CODENET_STATUSES = ["Accepted", "Wrong Answer", "Time Limit Exceeded"]


def _rated_list() -> dict[str, Any]:
    """Mirror of user.ratedList, restricted to the struct stg_cf_users reads."""
    users = []
    for position, handle in enumerate(HANDLES):
        users.append(
            {
                "handle": handle,
                "rating": 1200 + position * 300,
                "maxRating": 1400 + position * 300,
                "rank": "fixture",
                "maxRank": "fixture",
                "country": "Fixtureland",
                "city": "Fixture City",
                "organization": "Fixture University",
                "firstName": "Fixture",
                "lastName": f"User {position}",
                "contribution": 0,
                "friendOfCount": position,
                "registrationTimeSeconds": 1300000000 + position,
                "lastOnlineTimeSeconds": 1700000000 + position,
                "avatar": "https://example.invalid/avatar.png",
                "titlePhoto": "https://example.invalid/photo.png",
            }
        )
    return {"status": "OK", "result": users}


def _submission_rows() -> list[tuple[Any, ...]]:
    """One row per (problem, handle) pair, cycling verdicts and years."""
    rows: list[tuple[Any, ...]] = []
    submission_id = 100000
    for problem_position, (contest, index, name, rating, tags) in enumerate(PROBLEMS):
        for handle_position, handle in enumerate(HANDLES):
            submission_id += 1
            verdict = VERDICTS[(problem_position + handle_position) % len(VERDICTS)]
            year = 2019 + ((problem_position + handle_position) % 3)
            epoch = 1546300800 + submission_id
            points = 100.0 if verdict == "PARTIAL" else None
            rows.append(
                (
                    submission_id,
                    f"{contest}{index}",
                    contest,
                    index,
                    name,
                    rating,
                    None,
                    None,
                    tags,
                    "PROGRAMMING",
                    contest,
                    handle,
                    [handle],
                    handle,
                    True,
                    False,
                    1,
                    None,
                    "CONTESTANT",
                    True,
                    verdict,
                    verdict == "OK",
                    "GNU C++17",
                    "TESTS",
                    3,
                    120,
                    points,
                    epoch,
                    year,
                )
            )
    return rows


CF_DDL = """
create table cf_submissions (
    submission_id BIGINT,
    problem_id VARCHAR,
    problem_contest_id INTEGER,
    problem_index VARCHAR,
    problem_name VARCHAR,
    problem_rating INTEGER,
    problem_points DOUBLE,
    problemset_name VARCHAR,
    problem_tags VARCHAR[],
    problem_type VARCHAR,
    contest_id INTEGER,
    handle VARCHAR,
    author_handles VARCHAR[],
    collected_via_handle VARCHAR,
    is_person_level BOOLEAN,
    is_ghost BOOLEAN,
    team_size INTEGER,
    team_id INTEGER,
    participant_type VARCHAR,
    is_contest BOOLEAN,
    verdict VARCHAR,
    is_accepted BOOLEAN,
    programming_language VARCHAR,
    testset VARCHAR,
    passed_test_count INTEGER,
    time_consumed_millis BIGINT,
    points_scored DOUBLE,
    creation_time_seconds BIGINT,
    submitted_year BIGINT
)
"""

CODENET_DDL = """
create table codenet_submissions (
    submission_id VARCHAR,
    problem_id VARCHAR,
    user_id VARCHAR,
    judge VARCHAR,
    problem_name VARCHAR,
    time_limit_ms BIGINT,
    memory_limit_kb BIGINT,
    status VARCHAR,
    verdict_class VARCHAR,
    is_evidence BOOLEAN,
    is_accepted BOOLEAN,
    language VARCHAR,
    original_language VARCHAR,
    filename_ext VARCHAR,
    cpu_time_ms BIGINT,
    has_corrupt_timing BOOLEAN,
    memory_kb BIGINT,
    code_size_bytes BIGINT,
    accuracy VARCHAR,
    tests_passed INTEGER,
    tests_total INTEGER,
    submitted_at TIMESTAMP,
    submitted_year BIGINT
)
"""


def write_cf_silver(con: duckdb.DuckDBPyConnection, root: pathlib.Path) -> int:
    con.execute(CF_DDL)
    con.executemany(
        "insert into cf_submissions values (" + ",".join(["?"] * 29) + ")",
        _submission_rows(),
    )
    con.execute(
        """create view cf_out as select *,
             to_timestamp(creation_time_seconds) as submitted_at,
             cast(null as BIGINT) as relative_time_seconds,
             cast(null as BIGINT) as contest_start_time_seconds
           from cf_submissions"""
    )
    target = root / "lake" / "silver" / "cf_submissions"
    con.execute(
        f"copy (select * from cf_out) to '{target}' "
        "(format parquet, partition_by (submitted_year), overwrite_or_ignore true)"
    )
    counted = con.execute("select count(*) from cf_out").fetchone()
    return 0 if counted is None else int(counted[0])


def write_codenet_silver(con: duckdb.DuckDBPyConnection, root: pathlib.Path) -> int:
    con.execute(CODENET_DDL)
    rows: list[tuple[Any, ...]] = []
    for problem_position, problem in enumerate(CODENET_PROBLEMS):
        for attempt in range(3):
            status = CODENET_STATUSES[(problem_position + attempt) % 3]
            year = 2015 + attempt
            rows.append(
                (
                    f"s{problem_position}{attempt}",
                    problem,
                    f"u{attempt}",
                    "AIZU" if problem_position % 2 == 0 else "AtCoder",
                    f"Fixture problem {problem_position}",
                    2000,
                    65536,
                    status,
                    "accepted" if status == "Accepted" else "rejected",
                    True,
                    status == "Accepted",
                    "C++",
                    "C++14",
                    "cpp",
                    50,
                    False,
                    1024,
                    400,
                    None,
                    None,
                    None,
                    f"{year}-03-04 05:06:07",
                    year,
                )
            )
    con.executemany(
        "insert into codenet_submissions values (" + ",".join(["?"] * 23) + ")",
        rows,
    )
    target = root / "lake" / "silver" / "codenet_submissions"
    con.execute(
        f"copy (select * from codenet_submissions) to '{target}' "
        "(format parquet, partition_by (submitted_year), overwrite_or_ignore true)"
    )
    return len(rows)


def write_codenet_metadata(con: duckdb.DuckDBPyConnection, root: pathlib.Path) -> None:
    target = root / "raw" / "codenet" / "Project_CodeNet" / "metadata"
    target.mkdir(parents=True, exist_ok=True)
    lines = ["id,name,dataset,time_limit,memory_limit,rating,tags,complexity"]
    for position, problem in enumerate(CODENET_PROBLEMS):
        judge = "AIZU" if position % 2 == 0 else "AtCoder"
        lines.append(f"{problem},Fixture problem {position},{judge},2000,65536,,,")
    (target / "problem_list.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_codecontests(con: duckdb.DuckDBPyConnection, root: pathlib.Path) -> None:
    target = root / "raw" / "codecontests"
    target.mkdir(parents=True, exist_ok=True)
    con.execute(
        """create table codecontests as select * from (values
        ('p00001', 'Fixture problem 0', 'AIZU', 'A statement.', 12, false,
         2.0, 65536, '', '', 2, 0, 4, 3, 1),
        ('p00002', 'Fixture problem 1', 'AtCoder', 'Another statement.', 18,
         true, 2.0, 65536, '', '', 1, 0, 5, 2, 1)
        ) as t(problem_id, full_name, judge, description, description_chars,
               is_description_translated, time_limit_seconds,
               memory_limit_bytes, input_file, output_file, public_test_count,
               private_test_count, generated_test_count, solution_count,
               incorrect_solution_count)"""
    )
    path = target / f"codecontests_codenet_{CODECONTESTS_SHA}.parquet"
    con.execute(f"copy codecontests to '{path}' (format parquet)")


def write_zerotrac(root: pathlib.Path) -> None:
    target = root / "raw" / "zerotrac"
    target.mkdir(parents=True, exist_ok=True)
    records = [
        {
            "Rating": 1500.5 + position * 100,
            "ID": 9000 + position,
            "Title": f"Fixture LeetCode {position}",
            "TitleZH": f"Fixture {position}",
            "TitleSlug": f"fixture-problem-{position}",
            "ContestSlug": f"weekly-contest-{300 + position}",
            "ProblemIndex": f"Q{1 + position % 4}",
            "ContestID_en": f"Weekly Contest {300 + position}",
            "ContestID_zh": f"Contest {300 + position}",
        }
        for position in range(4)
    ]
    path = target / f"data_{ZEROTRAC_SHA}.json"
    path.write_text(json.dumps(records), encoding="utf-8")


def write_problemset(root: pathlib.Path) -> None:
    target = root / "raw" / "codeforces" / "problemset"
    target.mkdir(parents=True, exist_ok=True)
    lines = []
    for contest, index, name, rating, tags in PROBLEMS:
        lines.append(
            json.dumps(
                {
                    "problem_id": f"{contest}{index}",
                    "contest_id": contest,
                    "problemset_name": None,
                    "problemset_source": "main",
                    "problem_index": index,
                    "problem_name": name,
                    "problem_type": "PROGRAMMING",
                    "problem_points": None,
                    "problem_rating": rating,
                    "problem_tags": tags,
                    "solved_count": 1000,
                    "fetched_at": "2026-08-12T00:40:59+00:00",
                },
                sort_keys=True,
            )
        )
    path = target / f"problemset_{PROBLEMSET_STAMP}.jsonl"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_rated_list(root: pathlib.Path) -> None:
    target = root / "raw" / "codeforces" / "ratedList"
    target.mkdir(parents=True, exist_ok=True)
    path = target / f"ratedList_activeOnly_{SNAPSHOT_DATE.replace('-', '')}.json"
    path.write_text(json.dumps(_rated_list()), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build synthetic CI fixtures.")
    default_root = pathlib.Path("target/fixtures")
    parser.add_argument("--root", type=pathlib.Path, default=default_root)
    args = parser.parse_args(argv)

    root = args.root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    (root / "lake" / "silver").mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    try:
        cf_rows = write_cf_silver(con, root)
        codenet_rows = write_codenet_silver(con, root)
        write_codenet_metadata(con, root)
        write_codecontests(con, root)
    finally:
        con.close()

    write_zerotrac(root)
    write_problemset(root)
    write_rated_list(root)

    print(f"fixtures written under {root}")
    print(f"  cf submissions      {cf_rows}")
    print(f"  codenet submissions {codenet_rows}")
    print(f"  codenet problems    {len(CODENET_PROBLEMS)}")
    print(f"  cf problems         {len(PROBLEMS)}")
    print(f"  rated users         {len(HANDLES)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
