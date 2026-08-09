-- One row per submission. 23,607,105 rows at build time.
--
-- DIMENSION KEYS RESOLVE AS-OF submitted_at, NEVER TO is_current. Joining to the
-- current version would leak future information into the past - a 2015 submission
-- would be scored against a 2026 difficulty - and is the single most damaging error
-- possible in this model. The join predicates below are written for the general
-- case even though both dimensions currently hold exactly one version each, so the
-- model stays correct the moment a second snapshot lands rather than needing a
-- rewrite at that point.
--
-- MULTI-MEMBER TEAM SUBMISSIONS ARE KEPT (625,529 rows, 2.6%). A team submission is
-- returned in every member's user.status and was deduped globally in silver to one
-- surviving row carrying one member's collected handle. That handle is a genuine
-- co-author, so user_sk asserts nothing false, but it is one of N - per-user
-- submission counts computed off this table without filtering will attribute a
-- team's work to whichever member survived dedupe. is_person_level is the filter:
-- the skill model uses it, and any per-user count published to the observatory must
-- apply it too.
--
-- is_evidence, not the raw verdict, is what the skill model filters on. The raw
-- verdict is carried verbatim beside it and is never discarded. See codrona.md
-- section 6, "Verdict evidence policy".
--
-- preserve_insertion_order is disabled for this build only. DuckDB otherwise holds
-- ordering state across a 23.6M-row write, which is the difference between a build
-- that fits in memory on an 11 GB host and one that spills. Row order in a fact
-- table carries no meaning.

{{ config(
    materialized='table',
    pre_hook="set preserve_insertion_order = false",
    post_hook="set preserve_insertion_order = true"
) }}

with submissions as (

    select * from {{ ref('stg_cf_submissions') }}

)

select
    submissions.submission_id as submission_key,
    dim_problem.problem_sk,
    dim_user.user_sk,

    submissions.problem_id as problem_key,
    submissions.collected_via_handle as user_key,

    submissions.contest_id,
    submissions.participant_type,
    submissions.is_contest,

    submissions.verdict,
    submissions.verdict_class,
    submissions.is_evidence,
    submissions.is_accepted,

    submissions.is_person_level,
    submissions.team_id,
    submissions.team_size,

    submissions.programming_language,
    submissions.testset,
    submissions.passed_test_count,
    submissions.time_consumed_millis as execution_time_ms,
    submissions.points_scored,

    submissions.submitted_at,
    submissions.submitted_year

from submissions

inner join {{ ref('dim_problem') }} as dim_problem
    on submissions.problem_id = dim_problem.problem_key
    and submissions.submitted_at >= dim_problem.valid_from
    and (
        dim_problem.valid_to is null
        or submissions.submitted_at < dim_problem.valid_to
    )

inner join {{ ref('dim_user') }} as dim_user
    on submissions.collected_via_handle = dim_user.user_key
    and submissions.submitted_at >= dim_user.valid_from
    and (
        dim_user.valid_to is null
        or submissions.submitted_at < dim_user.valid_to
    )
