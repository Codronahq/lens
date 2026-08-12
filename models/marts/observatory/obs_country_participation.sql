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
-- SMALL CELLS PUBLISH NO STATISTICS, AND THIS WAS MEASURED, NOT ASSUMED.
-- 64 of 158 country rows hold fewer than five users and 1 is the minimum.
-- At one user the mean equals the max and equals that person's current
-- rating, next to their declared country - and user.ratedList is
-- filterable by country, so the row resolves to a named account in one
-- query against the same source it came from. The row therefore keeps its
-- name and its count, so coverage stays visible, and drops every rating
-- statistic. This differs from obs_organization_participation, which
-- collapses the row entirely, and the asymmetry is deliberate: an
-- organisation name at two users identifies a specific cohort, a country
-- name at one user identifies a country.
--
-- SPDX-License-Identifier: AGPL-3.0-or-later

with users as (

    select
        coalesce(nullif(country, ''), '(undeclared)') as country,
        nullif(country, '') is null as is_undeclared,
        rating
    from {{ ref('dim_user') }}
    where is_current

),

grouped as (

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

)

select
    country,
    is_undeclared,
    cohort_users,
    cohort_share_pct,
    case when cohort_users >= {{ var('min_cohort_users') }} then mean_rating end as mean_rating,
    case when cohort_users >= {{ var('min_cohort_users') }} then median_rating end as median_rating,
    case when cohort_users >= {{ var('min_cohort_users') }} then max_rating end as max_rating,
    case
        when cohort_users >= {{ var('min_cohort_users') }} then candidate_master_plus
    end as candidate_master_plus,
    cohort_users >= {{ var('min_cohort_users') }} as is_reportable
from grouped
order by cohort_users desc
