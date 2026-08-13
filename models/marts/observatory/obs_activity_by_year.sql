-- Submission volume, active users and problems attempted, per year.
--
-- COUNTS ARE COHORT COUNTS, not platform totals. Every figure here is bounded
-- by the 55,484 users we collected and by their full histories, so the shape
-- of the curve is meaningful while its height is an artefact of cohort size.
--
-- THE CURVE IS LEFT-TRUNCATED AND IS NOT A GROWTH CURVE. This is the single
-- most misreadable table here. The cohort came from user.ratedList with
-- activeOnly=true, so every one of the 55,484 users was active in the
-- collection window by construction - 2026 reads as 100% active because it
-- cannot read as anything else. Earlier years contain only the subset of
-- today's-active users who had already registered: 352 of them in 2019 against
-- 21,921 registering in 2025. Both figures are read under a pinned UTC session:
-- registered_at is TIMESTAMP WITH TIME ZONE, so year() resolves against the
-- session zone and this number was 21,916 on a machine in Asia/Kolkata. So submissions rising from 196,625 in 2019 to
-- 8,711,676 in 2026 measures our cohort's registration dates, not the growth of
-- competitive programming, and publishing it as a trend would be a false claim.
--
-- What the data DOES support is the participation rate, which is why
-- registered_by_then and active_share_pct are carried alongside every count.
-- Measured flat at 73-78% from 2018 through 2024 before the collection window
-- forces it to 100%, and a flat rate under a rising volume is the honest
-- reading: the cohort grew, the engagement per registered user did not change.
--
-- THE RIGHT EDGE IS ALSO TRUNCATED. Collection ran 2026-08-06 to 2026-08-09, so the
-- final year is a partial year and will always read as a collapse unless it is
-- labelled. is_partial_year carries that rather than leaving a chart to imply
-- competitive programming died in 2026.
--
-- SPDX-License-Identifier: AGPL-3.0-or-later

with facts as (

    select * from {{ ref('fct_submission') }}

),

latest as (

    select max(submitted_year) as final_year from facts

),

registered as (

    select year(registered_at) as registration_year, count(*) as users
    from {{ ref('dim_user') }}
    where is_current and registered_at is not null
    group by 1

),

cumulative as (

    select
        registration_year,
        sum(users) over (order by registration_year) as registered_by_then
    from registered

)

select
    facts.submitted_year,
    count(*) as submissions,
    count(*) filter (where facts.is_person_level) as person_level_submissions,
    count(distinct facts.user_key) as active_users,
    count(distinct facts.problem_key) as problems_attempted,
    count(*) filter (where facts.is_accepted) as accepted,
    round(
        100.0 * count(*) filter (where facts.is_accepted) / count(*), 2
    ) as accepted_pct,
    count(*) filter (where facts.is_contest) as in_contest,
    facts.submitted_year = latest.final_year as is_partial_year,
    max(cumulative.registered_by_then) as registered_by_then,
    round(
        100.0 * count(distinct facts.user_key)
        / nullif(max(cumulative.registered_by_then), 0),
        2
    ) as active_share_pct
from facts
cross join latest
left join cumulative
    on cumulative.registration_year = facts.submitted_year
group by facts.submitted_year, latest.final_year
order by facts.submitted_year
