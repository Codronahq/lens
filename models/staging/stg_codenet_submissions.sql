-- IBM Project CodeNet submissions, staged from silver Parquet.
--
-- A SEPARATE SPINE from the Codeforces staging model, deliberately. The two
-- sources share no user space (CodeNet ids are anonymised and can never join to
-- anything), no problem key space, and no verdict vocabulary - not one of
-- CodeNet's twelve statuses appears in Codeforces' sixteen. Conforming them
-- waits for IRT to produce a cross-judge difficulty equivalence; asserting one
-- here would invent an equivalence we have not earned.
--
-- The heavy lifting happens in the normalize job. This model is deliberately
-- thin: it renames nothing, derives nothing that Spark already derived, and
-- exists to give dbt a typed, tested entry point over the lake. Anything that
-- needed measuring was measured before the Parquet was written.
--
-- Facts carried forward, each measured over all 13,916,868 rows:
--
--   judge resolves on every row - 0 nulls, so no submission orphaned its problem
--   submission_id is unique - 13,916,868 rows, 13,916,868 distinct ids
--   4,048 problems appear in submissions against 4,053 in the index; the five
--     absent ones are AIZU problems with no submissions at all
--   problem_name is NULL for 54 AIZU problems (79,337 rows). That is a gap in
--     IBM's problem_list.csv, not a join failure, and is carried as NULL rather
--     than filled with a placeholder that would read as a real name.
--
-- NULL cpu_time_ms MEANS DIFFERENT THINGS BY JUDGE and must never be read as one
-- fact. All 852 AIZU nulls are corrupt negative readings this pipeline nulled,
-- flagged by has_corrupt_timing. All 415,826 AtCoder nulls are compile-error
-- rows where AtCoder simply records nothing, while AIZU records zeros for the
-- same status. Any feature over execution time has to condition on judge.

{{ config(materialized='view') }}

with source as (

    select * from {{ source('codenet', 'silver_submissions') }}

)

select
    submission_id,
    problem_id,
    user_id,
    judge,

    problem_name,
    time_limit_ms,
    memory_limit_kb,

    status,
    verdict_class,
    is_evidence,
    is_accepted,

    language,
    original_language,
    filename_ext,

    cpu_time_ms,
    has_corrupt_timing,
    memory_kb,
    code_size_bytes,

    accuracy,
    tests_passed,
    tests_total,

    submitted_at,
    submitted_year

from source
