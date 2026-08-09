-- SCD-2 user dimension, one row per user version.
--
-- KEYED ON THE COLLECTED HANDLE, NEVER ON A SUBMISSION'S AUTHOR HANDLE. Codeforces
-- denormalises the author handle when a submission is written and never rewrites it
-- on rename, and a released handle can later be retaken by an unrelated account - so
-- a historical row can name a handle that today belongs to a different human being
-- (measured: exactly one occurrence in 23.8M raw rows). The handle we asked
-- Codeforces for is the only authoritative identity, because that is the account CF
-- answered for. stg_cf_users.handle IS that handle: the ratedList is the same list
-- the collector iterated. See codrona.md section 6, "Handle identity and rename
-- semantics".
--
-- Referential integrity measured clean in BOTH directions over the full corpus: 0
-- fact handles absent from this dimension, 0 users here with no submissions. A
-- 55,484 <-> 55,484 bijection, which is the expected consequence of the identity
-- rule rather than a coincidence - we can only hold facts for handles we asked for.
--
-- DEGENERATE BY CONSTRUCTION TODAY, for the same reason as dim_problem: one
-- ratedList snapshot means one version per user. valid_from opens at the same
-- sentinel so as-of joins resolve for submissions dating back to 2010; rating and
-- rank here are TODAY's values applied retroactively. user.rating (per-user rating
-- history) is the Phase 2 call that makes this dimension genuinely time-varying,
-- and it inherits the rename hazard above.
--
-- Real names and locations load here deliberately. codrona.md section 6 settles
-- this: dropping name columns would be theatre, since handle is identity on every
-- submission row and no column choice makes the corpus anonymous. The control that
-- matters is the PUBLICATION boundary - the Kaggle release and the public
-- observatory take an explicit column allowlist - not the warehouse load. email and
-- vkId are excluded upstream at the struct allowlist and never reach here.

{{ config(materialized='table') }}

select
    md5(handle || '|1970-01-01') as user_sk,
    handle as user_key,
    rating,
    max_rating,
    rank_name,
    max_rank_name,
    country,
    city,
    organization,
    first_name,
    last_name,
    contribution,
    friend_of_count,
    registered_at,
    last_online_at,
    snapshot_date,
    cast('1970-01-01' as timestamp) as valid_from,
    cast(null as timestamp) as valid_to,
    true as is_current
from {{ ref('stg_cf_users') }}
