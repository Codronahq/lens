-- Every fact resolves to a dimension version whose window CONTAINS its event.
--
-- This is the invariant fct_submission's whole design rests on: dimension keys
-- are resolved as-of submitted_at, never to is_current, because joining to the
-- current version would leak an attribute's present value into a submission
-- made years earlier - a problem re-rated in 2026 would appear to have carried
-- that rating in 2012, and the skill model would fit difficulty against labels
-- that did not exist at the time.
--
-- The failure mode is silent by construction. A wrong-version join still
-- produces exactly one row per fact and every count downstream stays correct;
-- only the attribute values are wrong, and only for rows whose dimension
-- changed. Counts cannot detect it, which is why this test compares WINDOWS
-- rather than counts.
--
-- Today every version opens at the 1970 sentinel and none has closed, so this
-- cannot fail yet. It becomes the load-bearing test the moment a second
-- snapshot creates a closed version.

select
    fact.submission_key,
    fact.submitted_at,
    dim.problem_key,
    dim.valid_from,
    dim.valid_to
from {{ ref('fct_submission') }} as fact
inner join {{ ref('dim_problem') }} as dim
    on fact.problem_sk = dim.problem_sk
where fact.submitted_at < dim.valid_from
   or fact.submitted_at >= coalesce(dim.valid_to, cast('9999-12-31' as timestamp))
