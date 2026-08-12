-- Two independent routes to the same rating must not contradict each other.
--
-- The corpus route reads problem.rating denormalised onto 23.6M submission rows
-- collected over four days; this route reads the problemset in one response.
-- Measured at build time: 11,051 equal, 713 both null, ZERO different.
--
-- SEVERITY IS WARN, DELIBERATELY. Codeforces does re-rate problems, so a future
-- disagreement is real news about rating drift rather than a broken build - and
-- it is exactly the signal the SCD-2 dimension exists to record. Erroring would
-- turn the first legitimate re-rating into a red pipeline and invite someone to
-- delete the test; warning surfaces it while the build keeps running.
--
-- codrona.md records that problem.rating is served as TODAY's value on every
-- historical row. That is a statement about a single snapshot. This test is how
-- we would first learn it does not hold ACROSS snapshots.

{{ config(severity = 'warn') }}

select
    dim.problem_key,
    dim.problem_rating as corpus_rating,
    snapshot_problem.problem_rating as problemset_rating
from {{ ref('dim_problem') }} as dim
inner join {{ ref('stg_cf_problemset') }} as snapshot_problem
    on dim.problem_key = snapshot_problem.problem_id
where dim.problem_rating is distinct from snapshot_problem.problem_rating
