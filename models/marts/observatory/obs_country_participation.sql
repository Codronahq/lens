-- Participation by declared country - the India-vs-world chart's data.
--
-- THIS IS THE MOST MISREADABLE TABLE IN THE WAREHOUSE, and the columns are
-- shaped to make misreading hard.
--
-- Country is SELF-DECLARED and OPTIONAL. 18,900 of 55,484 collected users
-- state one; 36,584 do not. Worse, declaring correlates with strength:
-- declared users average 1212.5 rating against 937.0 for undeclared, a
-- 275-point gap measured over the whole cohort. So a country slice is not a
-- sample of that country's programmers - it is a sample of the stronger,
-- more engaged fraction who filled in a profile field.
--
-- The undeclared population is therefore emitted AS A ROW rather than filtered
-- away, so the denominator is visible in the data instead of living in a
-- footnote nobody carries into the chart. Any surface rendering this must show
-- that row or state the coverage; dropping it silently turns a 34% sample into
-- an apparent census.
--
-- Cohort counts are also stratified (codrona.md section 6), so these are counts
-- of our users, never of a country's Codeforces population.
--
-- SPDX-License-Identifier: AGPL-3.0-or-later

with users as (

    select
        coalesce(nullif(country, ''), '(undeclared)') as country,
        nullif(country, '') is null as is_undeclared,
        rating
    from {{ ref('dim_user') }}
    where is_current

)

select
    country,
    is_undeclared,
    count(*) as cohort_users,
    round(100.0 * count(*) / sum(count(*)) over (), 3) as cohort_share_pct,
    round(avg(rating), 1) as mean_rating,
    median(rating) as median_rating,
    max(rating) as max_rating,
    count(*) filter (where rating >= 1900) as candidate_master_plus
from users
group by country, is_undeclared
order by cohort_users desc
