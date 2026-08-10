-- No CodeNet status may fall to 'unknown'.
--
-- An unrecognised status is excluded from evidence rather than guessed at, and
-- this test is what makes that visible. Defaulting a new status to 'rejected'
-- would let it corrupt calibration with no symptom at all - the same reasoning
-- that governs the Codeforces verdict policy in codrona.md section 6.
--
-- Zero rows today across all twelve measured statuses. A failure here means
-- CodeNet shipped something the normalize mapping has never seen, which is a
-- deliberate classification decision, not a bug to paper over.

select status, count(*) as n
from {{ ref('stg_codenet_submissions') }}
where verdict_class = 'unknown'
group by 1
