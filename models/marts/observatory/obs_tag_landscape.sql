-- Topic landscape: how many problems carry each tag, how hard they are, and
-- how many people solve them.
--
-- TAGS COME FROM THE PROBLEMSET SNAPSHOT WHERE ONE EXISTS. dim_problem records
-- which rule produced each row in tags_source; the submission-derived tiebreak
-- was measured wrong on 4 of 11,764 overlapping problems, so this aggregate
-- reports coverage rather than pretending the distinction does not exist.
--
-- solved_count IS POPULATION-WIDE, ratings are Codeforces' own. Neither is
-- derived from our cohort, which makes the two difficulty columns here
-- independent of the sampling policy - unlike anything computed from
-- fct_submission. That independence is the point of carrying both.
--
-- A PROBLEM APPEARS ONCE PER TAG, so the tag counts sum to more than the
-- problem count. That is inherent to a multi-label field and is not a bug;
-- problems_with_tag is per-tag and never a share of a whole.
--
-- SPDX-License-Identifier: AGPL-3.0-or-later

with problems as (

    select
        problem_key,
        problem_rating,
        is_unrated,
        solved_count,
        tags_source,
        problem_tags
    from {{ ref('dim_problem') }}
    where is_current

),

exploded as (

    select
        unnest(problem_tags) as tag,
        problem_key,
        problem_rating,
        is_unrated,
        solved_count,
        tags_source
    from problems
    where len(problem_tags) > 0

)

select
    tag,
    count(*) as problems_with_tag,
    count(*) filter (where not is_unrated) as rated_problems,
    round(avg(problem_rating), 1) as mean_rating,
    median(problem_rating) as median_rating,
    min(problem_rating) as min_rating,
    max(problem_rating) as max_rating,
    median(solved_count) as median_solved_count,
    count(*) filter (where tags_source = 'problemset') as from_problemset,
    count(*) filter (where tags_source = 'submissions') as from_tiebreak
from exploded
group by tag
order by problems_with_tag desc
