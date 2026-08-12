-- TAGGED real_data. This pins a count of the REAL world, so it cannot pass
-- against CI's synthetic fixtures and CI excludes it by tag. That exclusion
-- is the honest boundary of what CI proves: 4 tests of 121. Everything
-- structural still runs there for real.
{{ config(tags = ['real_data']) }}

-- The staging model must carry exactly the published CodeNet corpus count.
--
-- 13,916,868 is IBM's own figure and was confirmed against the actual files by
-- two independent readers - the DuckDB census and the PySpark normalize job -
-- before either number was quoted anywhere. If this fails, rows were lost
-- between the archive and the warehouse; do not adjust the number to match.
--
-- The same pair-that-must-match discipline that found both provenance bugs in
-- the Codeforces half of this phase. A count nothing compares is a count
-- nothing can catch.

select
    13916868 as expected_rows,
    count(*) as actual_rows
from {{ ref('stg_codenet_submissions') }}
having count(*) <> 13916868
