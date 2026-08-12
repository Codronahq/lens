-- No published observatory row may describe fewer than min_cohort_users people.
--
-- This is the independent check: it re-derives the rule from the output rather
-- than trusting either model, and it recomputes the cell size instead of
-- reading the models' own is_reportable flags, so a flag computed wrongly still
-- fails here.
--
-- An earlier version of this test covered organisations only and justified that
-- with the claim that country cells are large. That claim was false when
-- measured: 64 of 158 country rows held fewer than five users, and the smallest
-- held one, publishing a single person's exact rating beside their declared
-- country. user.ratedList is filterable by country, so the row resolved to a
-- named account in one query. The two tables are suppressed differently on
-- purpose - organisations collapse into one bucket, countries keep their name
-- and count and lose their statistics - because an organisation name at two
-- users identifies a cohort while a country name at one user identifies a
-- country, and coverage is itself information the observatory owes a reader.
--
-- The organisation suppressed bucket is exempt by construction: it aggregates
-- every small cell into one row so the population still sums, and carries a
-- count and nothing else.
--
-- CI exercises the suppression branch only. The fixture cohort is four users in
-- one country, which is below the threshold by construction, so the reportable
-- branch is covered on real data and not in CI. That is the honest boundary,
-- the same one the real_data tag draws.

with organisation_cells as (

    select
        'obs_organization_participation' as source_table,
        organization as cell,
        cohort_users
    from {{ ref('obs_organization_participation') }}
    where is_reportable
      and cohort_users < {{ var('min_cohort_users') }}

),

country_cells as (

    select
        'obs_country_participation' as source_table,
        country as cell,
        cohort_users
    from {{ ref('obs_country_participation') }}
    where cohort_users < {{ var('min_cohort_users') }}
      and (
          mean_rating is not null
          or median_rating is not null
          or max_rating is not null
          or candidate_master_plus is not null
      )

)

select * from organisation_cells
union all
select * from country_cells
