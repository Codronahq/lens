-- Users per Codeforces rating band. The observatory's spine chart.
--
-- WHOLE COHORT, NO COUNTRY FILTER. This is the one distribution here that
-- describes every collected user, which makes it the honest denominator for
-- every country- or organisation-sliced chart that follows: those cover only
-- the 18,900 of 55,484 users who chose to state a country.
--
-- THE COHORT IS STRATIFIED, NOT A CENSUS. codrona.md records the sampling
-- policy: upper bands are taken whole because they are the only evidence
-- identifying 2400+ difficulty, lower bands are sampled. So band COUNTS here
-- are counts of our cohort and must never be rendered as "how many Codeforces
-- users are pupils". The share column is therefore share-of-cohort, named so.
--
-- Rating can be NEGATIVE - the observed minimum is -19 - so the lowest band is
-- bounded below by nothing rather than by zero.
--
-- SPDX-License-Identifier: AGPL-3.0-or-later

with users as (

    select rating from {{ ref('dim_user') }} where is_current

),

banded as (

    select
        case
            when rating < 1200 then 'newbie'
            when rating < 1400 then 'pupil'
            when rating < 1600 then 'specialist'
            when rating < 1900 then 'expert'
            when rating < 2100 then 'candidate master'
            when rating < 2300 then 'master'
            when rating < 2400 then 'international master'
            when rating < 2600 then 'grandmaster'
            when rating < 3000 then 'international grandmaster'
            else 'legendary grandmaster'
        end as rating_band,
        case
            when rating < 1200 then 1
            when rating < 1400 then 2
            when rating < 1600 then 3
            when rating < 1900 then 4
            when rating < 2100 then 5
            when rating < 2300 then 6
            when rating < 2400 then 7
            when rating < 2600 then 8
            when rating < 3000 then 9
            else 10
        end as band_order,
        rating
    from users

)

select
    rating_band,
    band_order,
    count(*) as cohort_users,
    round(100.0 * count(*) / sum(count(*)) over (), 3) as cohort_share_pct,
    min(rating) as min_rating,
    max(rating) as max_rating,
    round(avg(rating), 1) as mean_rating
from banded
group by rating_band, band_order
order by band_order
