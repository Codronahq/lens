-- Every SCD-2 key has EXACTLY ONE current version. Not zero, not two.
--
-- G8's third metric names "no overlap, one current, as-of correctness" as a
-- target, and until now nothing tested any of the three. That is the worst
-- shape a gate can take: a documented target with no mechanism, which reads as
-- green because nothing ever says otherwise.
--
-- Today this is VACUOUSLY TRUE - one collection snapshot means one version per
-- key, so it cannot fail. That is precisely why it is written now rather than
-- when snapshot 2 lands: the version-building logic changes on that day, and a
-- test authored on the same day as the change it is meant to gate is a test
-- written to pass. Proven against injected defects before shipping: two
-- current versions and zero current versions both fire.
--
-- Zero is as much a defect as two. A key with no current version disappears
-- from every is_current lookup while remaining in the fact table's history,
-- which surfaces as missing rows rather than as an error.--
-- THE UNION IS NAMESPACED, AND THAT IS NOT COSMETIC. Problem ids and user
-- handles occupy the same string space: 14A and 32A are each simultaneously a
-- Codeforces problem id and a real registered handle. Pooling the two key
-- spaces made this test report two current versions for keys that have exactly
-- one in each dimension. Caught on the test's first run against real data.

with versions as (

    select 'problem:' || problem_key as entity_key, is_current
    from {{ ref('dim_problem') }}
    union all
    select 'user:' || user_key as entity_key, is_current
    from {{ ref('dim_user') }}

)

select
    entity_key,
    sum(case when is_current then 1 else 0 end) as current_versions
from versions
group by entity_key
having sum(case when is_current then 1 else 0 end) <> 1
