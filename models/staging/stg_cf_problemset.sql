-- Codeforces problemset, staged from the pinned single-instant snapshot.
--
-- WHAT THIS IS NOT. It is not the problem spine. dim_problem holds 35,566
-- problems and this holds 11,809; the 23,802 it does not cover are real
-- problems with real submissions, and measured they are:
--
--   21,881  gym (contestId >= 100000), of which only 106 are rated
--    1,921  mainline, absent because Codeforces does not publish mirror,
--           practice and special-format contests into the problemset archive
--
-- All 487 contests behind that second group were resolved against contest.list
-- - none is unknown - and their names are unambiguous: Kotlin Heroes practice
-- sessions, unofficial mirrors, VK Cup wild-cards, ABBYY Cup finals. So this is
-- the archive's scope rather than a data gap, and enrichment from here is
-- always a LEFT join. An inner join would drop 23,802 problems from the
-- dimension and their submissions from the fact table through a failed join,
-- which is the silent-sampling-bias class codrona.md section 9 treats as a hard
-- error rather than a warning.
--
-- MEASURED AGAINST dim_problem BEFORE THIS WAS WRITTEN:
--
--   11,764 of 11,809 rows resolve to an existing problem
--       45 do not, all from contests 2255 and 2256 - rounds that ran AFTER the
--          collection window closed, so they have no submissions here yet. All
--          45 are unrated, because Codeforces assigns a rating some time after
--          a round. They are deliberately NOT added to the dimension: a row no
--          fact references and no attribute populates is not information.
--   11,051 ratings equal, 713 both null, ZERO disagreements
--
-- Types are cast explicitly rather than inferred. problem_points is null on
-- most rows, and read_json_auto infers JSON for an all-null column, which would
-- make the staged type depend on which problems happen to carry points.
--
-- SPDX-License-Identifier: AGPL-3.0-or-later

{{ config(materialized='view') }}

with source as (

    select * from {{ source('cf_problemset', 'problems') }}

)

select
    cast(problem_id as varchar) as problem_id,
    cast(contest_id as integer) as contest_id,
    cast(problemset_name as varchar) as problemset_name,
    cast(problemset_source as varchar) as problemset_source,
    cast(problem_index as varchar) as problem_index,
    cast(problem_name as varchar) as problem_name,
    cast(problem_type as varchar) as problem_type,
    cast(problem_points as double) as problem_points,
    cast(problem_rating as integer) as problem_rating,
    problem_tags,
    len(problem_tags) as tag_count,
    cast(solved_count as bigint) as solved_count,
    problem_rating is null as is_unrated,
    cast(fetched_at as timestamp) as fetched_at

from source
