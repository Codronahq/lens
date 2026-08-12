-- dim_problem must contain every problem the corpus references. Exactly.
--
-- This exists because dim_problem now LEFT joins the problemset snapshot, which
-- covers 11,809 of its 35,566 problems. Changing that join to an inner one is a
-- one-word edit that would silently drop 23,802 problems here and, through the
-- failed dimension lookup, their submissions from fct_submission - the
-- codrona.md section 9 silent-sampling-bias class, arriving with a green build
-- and no other symptom.
--
-- The bound is the corpus itself rather than a hard-coded 35,566, so it keeps
-- gating after the next collection widens the cohort instead of needing an edit
-- that someone would eventually make by just pasting the new number in.

with corpus as (

    select count(distinct problem_id) as problem_count
    from {{ ref('stg_cf_submissions') }}

),

dimension as (

    select count(*) as problem_count
    from {{ ref('dim_problem') }}

)

select
    'dim_problem does not cover the corpus problem space' as failure,
    corpus.problem_count as corpus_problems,
    dimension.problem_count as dimension_problems
from corpus, dimension
where corpus.problem_count <> dimension.problem_count
