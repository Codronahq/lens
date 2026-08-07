-- Codeforces submissions, staged from silver Parquet.
--
-- The raw verdict is never discarded. Two derived columns sit beside it:
--
--   verdict_class  accepted / partial / rejected / unjudged / unknown
--   is_evidence    false where the submission says nothing about ability
--
-- Measured over 16.87M rows, the distinction is load-bearing. PARTIAL carries
-- points on 112,010 of 112,017 rows, so it is real ordinal evidence rather
-- than a rejection. CHALLENGED carries zero points on all 10,204 rows: the
-- code was accepted and then broken in the challenge phase, which is a
-- failure. SKIPPED is heterogeneous - 63% carry a problem rating and 60% are
-- in-contest, so some were judged before being invalidated - and is treated
-- as unjudged in full, because feeding contest-clustered pseudo-failures to
-- the skill model would teach it that strong users fail more.
--
-- An unrecognised verdict falls to 'unknown' and is NOT evidence. Defaulting
-- it to 'rejected' would let a new Codeforces verdict corrupt calibration
-- with no visible symptom; assert_no_unknown_verdict fires instead.
--
-- SPDX-License-Identifier: AGPL-3.0-or-later

with source as (

    select * from {{ source('silver', 'cf_submissions') }}

),

classified as (

    select
        *,
        case
            when verdict = 'OK' then 'accepted'
            when verdict = 'PARTIAL' then 'partial'
            when verdict in (
                'WRONG_ANSWER',
                'TIME_LIMIT_EXCEEDED',
                'RUNTIME_ERROR',
                'COMPILATION_ERROR',
                'MEMORY_LIMIT_EXCEEDED',
                'IDLENESS_LIMIT_EXCEEDED',
                'CHALLENGED',
                'PRESENTATION_ERROR',
                'REJECTED',
                -- documented by the API, unobserved in the corpus so far
                'SECURITY_VIOLATED'
            ) then 'rejected'
            when verdict is null or verdict in (
                'SKIPPED',
                'TESTING',
                'FAILED',
                'CRASHED',
                -- documented by the API, unobserved in the corpus so far
                'DENIAL_OF_JUDGEMENT',
                'INPUT_PREPARATION_CRASHED'
            ) then 'unjudged'
            else 'unknown'
        end as verdict_class
    from source

)

select
    *,
    verdict_class in ('accepted', 'partial', 'rejected') as is_evidence
from classified
