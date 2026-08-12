-- Participation by declared organisation. The college-readiness layer's seed.
--
-- A MINIMUM CELL SIZE IS APPLIED IN THE MODEL, NOT AT THE CHART. Organisation
-- plus rating band plus country is close to identifying at small counts: an
-- organisation with two users and a mean rating is a statement about two named
-- people who never agreed to be characterised. Rows below MIN_COHORT_USERS = 5
-- are collapsed into a single '(below reporting threshold)' row rather than
-- dropped, so the population still sums and the suppression is visible.
--
-- Five is a judgement, not a derived number. It is the smallest cell where a
-- mean stops describing individuals, and it is applied here - in the warehouse
-- - because a threshold enforced at the rendering layer is one refactor away
-- from not being enforced at all.
--
-- ORGANISATION IS FREE TEXT AND IS NOT NORMALISED. India alone shows 1,207
-- distinct strings across 7,762 users, and the same institution appears under
-- several spellings. Only whitespace is cleaned - trimmed, internal runs
-- collapsed - because that is mechanical and reversible. Merging "IIT Bombay"
-- with "IITB" is a judgement about identity that would silently misattribute
-- users if wrong, and it is deferred until there is a reason to make it.
-- Counts here are therefore per-string, and a chart must say so.
--
-- EMPTY STRING IS NOT NULL. 8,887 users carry '' and 30,880 carry NULL; a
-- count(organization) treats the blanks as present and overstates coverage by
-- exactly those 8,887. nullif is not optional here.
--
-- The self-declaration bias recorded in obs_country_participation applies with
-- equal force: users who fill in an organisation are the ones who fill in
-- profile fields.
--
-- SPDX-License-Identifier: AGPL-3.0-or-later

{% set min_cohort_users = 5 %}

with users as (

    select
        trim(regexp_replace(organization, '\s+', ' ', 'g')) as organization,
        country,
        rating
    from {{ ref('dim_user') }}
    where is_current
      and nullif(trim(organization), '') is not null

),

grouped as (

    select
        organization,
        count(*) as cohort_users,
        round(avg(rating), 1) as mean_rating,
        median(rating) as median_rating,
        max(rating) as max_rating,
        count(*) filter (where rating >= 1900) as candidate_master_plus,
        mode(country) as most_common_country
    from users
    group by organization

),

reportable as (

    select * from grouped where cohort_users >= {{ min_cohort_users }}

),

suppressed as (

    select
        '(below reporting threshold)' as organization,
        sum(cohort_users) as cohort_users,
        cast(null as double) as mean_rating,
        cast(null as double) as median_rating,
        cast(null as integer) as max_rating,
        cast(null as bigint) as candidate_master_plus,
        cast(null as varchar) as most_common_country
    from grouped
    where cohort_users < {{ min_cohort_users }}
    having sum(cohort_users) > 0

)

select
    organization,
    cohort_users,
    mean_rating,
    median_rating,
    max_rating,
    candidate_master_plus,
    most_common_country,
    organization <> '(below reporting threshold)' as is_reportable
from reportable

union all

select
    organization,
    cohort_users,
    mean_rating,
    median_rating,
    max_rating,
    candidate_master_plus,
    most_common_country,
    false as is_reportable
from suppressed

order by is_reportable desc, cohort_users desc
