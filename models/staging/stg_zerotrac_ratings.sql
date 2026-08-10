-- Community LeetCode difficulty estimates, staged from the pinned data.json.
--
-- THESE ARE NOT LEETCODE DIFFICULTIES. LeetCode publishes Easy/Medium/Hard;
-- these are one maintainer's Elo/MLE estimates computed from contest results,
-- and LEGAL.md obliges us to label them as community estimates everywhere they
-- are displayed. The column is named accordingly - a column called
-- "difficulty" would be a false claim about provenance every time it rendered.
--
-- COVERAGE IS CONTEST PROBLEMS ONLY, and deliberately partial. 2,549 problems
-- against LeetCode's several thousand, with weekly contests 1-62 absent because
-- the APIs of the era differed and the maintainer excluded them. A LeetCode
-- problem with no row here is the expected case, not a data-quality failure;
-- Codrona's own IRT estimate fills the gap rather than a null being emitted.
--
-- Measured over the pinned snapshot before this was written:
--
--   2,549 records, 2,549 distinct title_slug, 2,549 distinct leetcode_id
--   nine fields present on every row - zero ragged records
--   rating spans 1084.13 to 3773.76, continuous rather than integral
--   problem_index is Q1 through Q5
--   1,801 weekly plus 748 biweekly contests, summing exactly to 2,549
--
-- title_slug is the key rather than the numeric id: the slug is what appears in
-- a LeetCode URL and in every adapter route in codrona.md section 6, from the
-- GraphQL endpoint to LeetHub exports. The numeric id is platform-internal.

{{ config(materialized='view') }}

with source as (

    select * from {{ source('zerotrac', 'problem_ratings') }}

)

select
    "TitleSlug" as title_slug,
    "ID" as leetcode_id,
    "Title" as title,
    "TitleZH" as title_zh,
    "Rating" as community_rating,
    "ContestSlug" as contest_slug,
    "ContestID_en" as contest_name,
    "ProblemIndex" as problem_index,
    case
        when "ContestSlug" like 'biweekly-contest-%' then 'biweekly'
        when "ContestSlug" like 'weekly-contest-%' then 'weekly'
        else 'unknown'
    end as contest_type

from source
