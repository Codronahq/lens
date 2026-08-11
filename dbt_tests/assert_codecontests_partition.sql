-- The staged partition must contain ONLY CodeNet-sourced problems.
--
-- This is a LICENCE BOUNDARY test, not a data-quality one, and it is the most
-- consequential test in this source. LEGAL.md permits CodeContests only for
-- problems originating in CodeNet; the 7,819 Codeforces problems in the
-- upstream dataset are excluded because link-never-host forbids storing
-- Codeforces statements, and routing them through a third party does not
-- launder that restriction.
--
-- Every CodeNet problem id matches p followed by five digits. A row whose id
-- does not, or whose judge is neither AIZU nor AtCoder, means the ingest filter
-- admitted something it must not have - a compliance failure, not a bug to
-- shrug at. Fix the ingest, never this test.

select problem_id, judge
from {{ ref('stg_codecontests_problems') }}
where judge not in ('AIZU', 'AtCoder')
   or problem_id not similar to 'p[0-9]{5}'
