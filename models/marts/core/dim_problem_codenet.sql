-- CodeNet problem dimension. One row per problem, 4,053 rows.
--
-- NOT SCD-2, unlike dim_problem, and that is a deliberate divergence rather than
-- an oversight. CodeNet 1.0.0 is a frozen archive published in May 2021 and will
-- never change: there is no collection window, no live judge to re-tag anything,
-- and no mechanism that could ever produce a second version of a row. SCD-2
-- columns here would be ceremony - a sentinel valid_from, a null valid_to and a
-- permanently true is_current, forever, with nothing able to fire them.
--
-- snapshot_version carries the honest versioning instead. If IBM ever ships a
-- 2.0.0 archive that is a new load with a new version value, which is what
-- versioning means for a static dataset.
--
-- SOURCED FROM THE INDEX, NOT FROM SUBMISSIONS. problem_list.csv holds all 4,053
-- problems while only 4,048 appear in any submission; the five absent ones are
-- AIZU problems nobody ever solved. Building this dimension from the fact side
-- would silently drop them, and a problem with zero attempts is a real fact
-- about the corpus - it is exactly the cold-start case codrona.md section 9
-- names as a failure mode, so it must survive into the dimension.
--
-- rating, tags and complexity are NOT selected. All three are empty on every one
-- of the 4,053 rows - verified across the whole file, not sampled - so CodeNet
-- contributes zero difficulty labels and zero topic tags. Its problems are
-- difficulty-unknown until IRT places them on a scale from response patterns
-- alone. Carrying three always-null columns would imply a signal that is not
-- there. Codeforces remains the only labelled-difficulty source.

{{ config(materialized='table') }}

with index_rows as (

    select * from {{ source('codenet', 'problem_index') }}

),

attempted as (

    select
        problem_id,
        count(*) as submission_count,
        count(distinct user_id) as user_count,
        min(submitted_at) as first_submission_at,
        max(submitted_at) as last_submission_at
    from {{ ref('stg_codenet_submissions') }}
    group by 1

)

select
    md5('codenet|1.0.0|' || index_rows.id) as problem_sk,
    index_rows.id as problem_key,
    index_rows.dataset as judge,

    -- NULL on 54 AIZU problems. A gap in IBM's index, carried honestly rather
    -- than filled with a placeholder that would read as a real problem name.
    nullif(trim(index_rows.name), '') as problem_name,

    index_rows.time_limit as time_limit_ms,
    index_rows.memory_limit as memory_limit_kb,

    coalesce(attempted.submission_count, 0) as submission_count,
    coalesce(attempted.user_count, 0) as user_count,
    attempted.first_submission_at,
    attempted.last_submission_at,
    attempted.problem_id is null as has_no_submissions,

    'codenet-1.0.0' as snapshot_version

from index_rows
left join attempted
    on index_rows.id = attempted.problem_id
