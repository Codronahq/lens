-- TAGGED real_data. This pins a count of the REAL world, so it cannot pass
-- against CI's synthetic fixtures and CI excludes it by tag. That exclusion
-- is the honest boundary of what CI proves: 4 tests of 121. Everything
-- structural still runs there for real.
{{ config(tags = ['real_data']) }}

-- The fact table must carry exactly the canonical corpus count.
--
-- This is the pair-that-must-match discipline applied to the mart: both provenance
-- bugs this phase were found by two counts that should have been equal being off by
-- one, and by nothing else. An inner join to either dimension can silently drop rows
-- - a failed problem or user resolution costs submissions with no error - so the
-- count is asserted against the figure two independent engines agreed on rather than
-- against the staging model this table is built from.
--
-- 23,607,105 is canonical: DuckDB count.py and the PySpark normalize job agree to
-- the row through completely different code paths. See codrona.md section 6.
-- If this fails, the mart lost rows in a join; do not adjust the number to match.

select
    23607105 as expected_rows,
    count(*) as actual_rows
from {{ ref('fct_submission') }}
having count(*) <> 23607105
