-- TAGGED real_data. This pins a count of the REAL world, so it cannot pass
-- against CI's synthetic fixtures and CI excludes it by tag. That exclusion
-- is the honest boundary of what CI proves; `dbt list --select tag:real_data`
-- names every test in it. Everything structural still runs there for real.
{{ config(tags = ['real_data']) }}

-- The third enforcement of the pin: filename, column, and this count.
--
-- The snapshot is a point-in-time read of a live archive. Codeforces adds
-- problems every week, so re-running the pull produces a different file with a
-- different count - which is correct and expected. What must never happen
-- silently is the WAREHOUSE moving to a new snapshot without anyone deciding
-- to: an env var pointed elsewhere, a stale file picked up, a second pull
-- overwriting the first. This fails in that case and says so.
--
-- Updating the snapshot is therefore a two-line edit - the default stamp in
-- _sources_problemset.yml and the count here - which is the point. A pin that
-- can drift without an edit is not a pin.

select
    'problemset snapshot size changed' as failure,
    11809 as expected,
    count(*) as actual
from {{ ref('stg_cf_problemset') }}
having count(*) <> 11809
