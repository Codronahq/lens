-- No two versions of one SCD-2 key may cover the same instant.
--
-- Overlap is the defect that makes an as-of join non-deterministic: two
-- versions match one timestamp, the join picks by whatever order the engine
-- happens to produce, and the same query returns different attributes on
-- different runs. Nothing raises. fct_submission resolves dimension keys as-of
-- submitted_at across 23.6M rows, so the blast radius is the whole fact table.
--
-- An open version (valid_to null) extends to the end of time and is compared as
-- such, rather than being skipped for being null - a null-blind version of this
-- check would pass on exactly the rows most likely to be wrong.
--
-- Vacuous today for the same reason as the single-current test, and written now
-- for the same reason. Proven to fire against an injected overlapping pair.--
-- THE UNION IS NAMESPACED, AND THAT IS NOT COSMETIC. Problem ids and user
-- handles occupy the same string space: 14A and 32A are each simultaneously a
-- Codeforces problem id and a real registered handle. Pooling the two key
-- spaces made this test report two current versions for keys that have exactly
-- one in each dimension. Caught on the test's first run against real data.

with versions as (

    select 'problem:' || problem_key as entity_key, valid_from, valid_to
    from {{ ref('dim_problem') }}
    union all
    select 'user:' || user_key as entity_key, valid_from, valid_to
    from {{ ref('dim_user') }}

)

select
    earlier.entity_key,
    earlier.valid_from as earlier_valid_from,
    earlier.valid_to as earlier_valid_to,
    later.valid_from as later_valid_from
from versions as earlier
inner join versions as later
    on earlier.entity_key = later.entity_key
    and earlier.valid_from < later.valid_from
where coalesce(earlier.valid_to, cast('9999-12-31' as timestamp)) > later.valid_from
