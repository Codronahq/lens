-- SCD-2 problem dimension, one row per problem version.
--
-- DEGENERATE BY CONSTRUCTION TODAY. We hold a single collection snapshot, and
-- problem_rating and problem_name were measured invariant within it (0 of 35,566
-- problems carry two ratings or two names across 23,607,105 rows). So every
-- problem builds exactly one version. The SCD-2 shape is real and the versioning
-- logic is exercised by fixtures, not by data, until a second snapshot lands.
--
-- valid_from OPENS AT A SENTINEL, NOT AT THE SNAPSHOT DATE. Submissions run from
-- 2010 and the snapshot is 2026-08-06; opening version 1 at the snapshot date
-- would make every as-of join in fct_submission resolve to nothing. Version 1
-- therefore opens at the beginning of time and the attributes are understood as
-- current values applied retroactively - the same caveat class codrona.md already
-- records for problem.rating. Later versions get real snapshot dates.
--
-- TAGS ARE THE ONE ATTRIBUTE THAT VARIES. 7 of 35,566 problems carry more than one
-- distinct tag array, all from contests 2248/2252/2253 - within days of the
-- collection window. Tags are constant within any single collected file and vary
-- only between files, so each API response is internally consistent; the mechanism
-- behind the between-file variation is NOT settled (submission-time and
-- collection-time explanations each fit some problems and not others). The
-- tiebreak below is deliberate rather than arbitrary: prefer the richest tagging,
-- break ties lexicographically. It is reproducible regardless of which explanation
-- is right, and a problemset.problems pull would supersede it with a
-- single-instant read.
--
-- Unrated problems are KEPT with a NULL rating. 63.5% of problems carry no rating
-- but only 8.6% of submissions do; excluding them would silently drop those rows
-- from fct_submission through a failed join, which is the silent-sampling-bias
-- class codrona.md section 9 treats as a hard error. IRT excludes them from
-- difficulty priors while keeping their responses - a modelling decision, not a
-- warehouse one.

{{ config(materialized='table') }}

with attribute_sets as (

    select distinct
        problem_id,
        problem_contest_id,
        problem_index,
        problem_name,
        problem_rating,
        problem_points,
        problemset_name,
        problem_tags,
        problem_type
    from {{ ref('stg_cf_submissions') }}

),

ranked as (

    select
        attribute_sets.*,
        row_number() over (
            partition by problem_id
            order by
                len(problem_tags) desc,
                cast(problem_tags as varchar) asc
        ) as attribute_rank
    from attribute_sets

)

select
    md5(problem_id || '|1970-01-01') as problem_sk,
    problem_id as problem_key,
    problem_contest_id,
    problem_index,
    problem_name,
    problem_rating,
    problem_points,
    coalesce(problemset_name, 'codeforces') as problemset_name,
    problem_tags,
    len(problem_tags) as tag_count,
    problem_rating is null as is_unrated,
    problem_type,
    cast('1970-01-01' as timestamp) as valid_from,
    cast(null as timestamp) as valid_to,
    true as is_current
from ranked
where attribute_rank = 1
