-- LeetCode problem dimension, 2,549 rows, one per rated contest problem.
--
-- BUILT AHEAD OF ITS FACTS, deliberately. No LeetCode submissions exist in the
-- warehouse yet - the adapter ladder in codrona.md section 6 is Phase 5 work -
-- so this dimension currently joins to nothing. It is built now because the
-- source is a pinned snapshot that will be rewritten weekly upstream, and
-- because the adapter work later should join to something that already exists
-- and is tested rather than building a dimension under deadline.
--
-- NOT SCD-2, matching dim_problem_codenet rather than dim_problem. A pinned
-- commit is a frozen artifact by construction: nothing can change within a SHA.
-- snapshot_version carries the pin, so a later refresh is a new load with a new
-- version value rather than SCD-2 machinery that no mechanism could ever fire.
--
-- community_rating IS AN ESTIMATE AND MUST TRAVEL AS ONE. LEGAL.md's attribution
-- obligation is not satisfied by a footer alone: anywhere this number reaches a
-- user it is labelled a community estimate, never an authoritative LeetCode
-- difficulty. The column name, the description here, and the rating_source
-- column all carry that, so a downstream surface cannot render it as official
-- by accident.
--
-- Coverage is partial by design and the dimension does NOT pretend otherwise.
-- Only contest problems are rated, and weekly contests 1-62 are absent upstream.
-- A LeetCode problem missing from this dimension is expected; Codrona's own IRT
-- estimate is the fallback, which is also why no placeholder row is invented for
-- unrated problems.

{{ config(materialized='table') }}

select
    md5('leetcode|' || '{{ env_var("CODRONA_ZEROTRAC_SHA", "881a239306ce7a339e32e7825cdb9c00fead00f1") }}' || '|' || title_slug) as problem_sk,
    title_slug as problem_key,
    leetcode_id,
    title,
    title_zh,

    community_rating,
    'zerotrac-elo-mle' as rating_source,

    contest_slug,
    contest_name,
    contest_type,
    problem_index,

    'zerotrac@{{ env_var("CODRONA_ZEROTRAC_SHA", "881a239306ce7a339e32e7825cdb9c00fead00f1") }}' as snapshot_version

from {{ ref('stg_zerotrac_ratings') }}
