-- Codeforces user profiles, staged from one user.ratedList snapshot.
--
-- The struct in the read_json call below is an ALLOWLIST, the same shape the
-- collector's schema.py uses: a field absent from it never reaches the
-- warehouse. Two of the eighteen fields the API returns are excluded.
--
--   email   1,469 of 55,484 rows. No Codrona feature needs it and an
--           aggregated address list has obvious abuse value.
--   vkId    17 rows. A social-network handle for a third party, no use here.
--
-- Everything else is loaded. These profiles are public on Codeforces, and
-- dropping name columns would be theatre in any case: handle IS identity here
-- and appears on every submission row, so the corpus is pseudonymous at best
-- and no column choice changes that. The control that matters sits at the
-- publication boundary - the Kaggle corpus and the public observatory get an
-- explicit column allowlist, so a column added later must be admitted
-- deliberately rather than shipped by accident.
--
-- Codeforces states no licence and its site Terms govern (LEGAL.md), the same
-- footing that produced link-never-host. A local warehouse is not publication.
--
-- Ratings go NEGATIVE: the observed minimum is -19. Any range check
-- downstream must allow it.
--
-- SPDX-License-Identifier: AGPL-3.0-or-later

{% set snapshot_date = "2026-08-06" %}

{% set default_rated_list %}{{ env_var("HOME") }}/codrona-data/raw/codeforces/ratedList/ratedList_activeOnly_20260806.json{% endset %}
{% set rated_list_path = env_var("CODRONA_CF_RATED_LIST", default_rated_list | trim) %}

with source as (

    select unnest(result) as u
    from read_json(
        '{{ rated_list_path }}',
        maximum_object_size = 268435456,
        columns = {
            status: 'VARCHAR',
            result: 'STRUCT(handle VARCHAR, rating INTEGER, maxRating INTEGER,
                            "rank" VARCHAR, maxRank VARCHAR, country VARCHAR,
                            city VARCHAR, organization VARCHAR,
                            firstName VARCHAR, lastName VARCHAR,
                            contribution INTEGER, friendOfCount INTEGER,
                            registrationTimeSeconds BIGINT,
                            lastOnlineTimeSeconds BIGINT,
                            avatar VARCHAR, titlePhoto VARCHAR)[]'
        }
    )

)

select
    u.handle as handle,
    u.rating as rating,
    u.maxRating as max_rating,
    u."rank" as rank_name,
    u.maxRank as max_rank_name,
    u.country as country,
    u.city as city,
    u.organization as organization,
    u.firstName as first_name,
    u.lastName as last_name,
    u.contribution as contribution,
    u.friendOfCount as friend_of_count,
    to_timestamp(u.registrationTimeSeconds) as registered_at,
    to_timestamp(u.lastOnlineTimeSeconds) as last_online_at,
    u.avatar as avatar_url,
    u.titlePhoto as title_photo_url,
    cast('{{ snapshot_date }}' as date) as snapshot_date
from source
