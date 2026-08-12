-- No reportable observatory row may describe fewer than 5 people.
--
-- The threshold lives in obs_organization_participation, and a threshold that
-- lives only in the model it protects is one careless edit from being gone.
-- This is the independent check: it re-derives the rule from the output rather
-- than trusting the model that produced it.
--
-- Why organisation specifically. Country cells are large and a country is not
-- an identifying attribute at these counts. An organisation is: with two users
-- from one college, a mean rating and a max rating together characterise two
-- named individuals who declared a workplace, not a willingness to be
-- measured. codrona.md places the control at the publication boundary, and an
-- observatory table IS the publication boundary - it is the thing the charts
-- read.
--
-- The suppressed bucket is exempt by construction: it aggregates every small
-- cell into one row precisely so the population still sums, and that row
-- carries no statistics, only a count.

select
    organization,
    cohort_users
from {{ ref('obs_organization_participation') }}
where is_reportable
  and cohort_users < 5
