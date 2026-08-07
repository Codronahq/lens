-- Fails when Codeforces returns a verdict the staging model does not map.
--
-- Such a row is excluded from evidence rather than guessed at, so the failure
-- mode without this test is silent: the corpus quietly shrinks and calibration
-- drifts with nothing in the logs. Fix by classifying the verdict explicitly.
--
-- SPDX-License-Identifier: AGPL-3.0-or-later

select
    verdict,
    count(*) as n
from {{ ref('stg_cf_submissions') }}
where verdict_class = 'unknown'
group by verdict
