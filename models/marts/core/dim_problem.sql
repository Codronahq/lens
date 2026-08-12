-- SCD-2 problem dimension, one row per problem version.
--
-- DEGENERATE BY CONSTRUCTION TODAY. We hold a single collection snapshot, and
-- problem_rating and problem_name were measured invariant within it (0 of 35,566
-- problems carry two ratings or two names across 23,607,105 rows). So every
-- problem builds exactly one version. The SCD-2 shape is real and the versioning
-- logic is exercised by fixtures, not by data, until a second snapshot lands.
--
-- valid_from OPENS AT A SENTINEL, NOT AT THE SNAPSHOT DATE. Submissions run from
-- 2010 and the snapshot is 2026-08-06; opening version 1 at the snapshot date
-- would make every as-of join in fct_submission resolve to nothing. Version 1
-- therefore opens at the beginning of time and the attributes are understood as
-- current values applied retroactively - the same caveat class codrona.md already
-- records for problem.rating. Later versions get real snapshot dates.
--
-- TAGS NOW COME FROM THE PROBLEMSET SNAPSHOT WHERE ONE EXISTS. Until 2026-08-12
-- the only source of tags was the submission corpus, where 7 of 35,566 problems
-- carried more than one distinct tag array and were resolved by a deterministic
-- richest-then-lexicographic tiebreak. problemset.problems is one response at one
-- instant, so there is nothing to break a tie between, and it settles the
-- question the tiebreak could only paper over.
--
-- Measured across the 11,764 problems present in both, the tiebreak was wrong on
-- FOUR - and one of those is invisible to a count-based check: 2253E carries five
-- tags in each source with different members, so only comparing the sorted sets
-- finds it. Preferring the snapshot loses nothing: snapshot-empty-while-corpus-
-- has-tags is 0 of 11,764. Where a problem is absent from the snapshot the
-- tiebreak still applies, so tags_source records which rule produced the row and
-- no consumer has to infer it.
--
-- problem_rating DELIBERATELY STAYS CORPUS-DERIVED. The snapshot's rating lands
-- beside it in problemset_rating rather than replacing it, because
-- assert_problemset_ratings_agree compares the two and would become tautological
-- the moment one was computed from the other - it would pass forever while
-- testing nothing. Two independent routes are only worth having while they stay
-- independent. Measured today: 11,051 equal, 713 both null, zero different.
--
-- solved_count IS NEW EVIDENCE, NOT A RESTATEMENT. It counts accepted solutions
-- across every Codeforces user, while every other difficulty signal here is
-- computed over our own 55,484-user stratified cohort. That makes it an
-- independent check on cohort sampling bias - the codrona.md section 9 gate that
-- otherwise has only the committed band counts behind it - rather than another
-- view of the same evidence. It is NULL for the 23,802 problems the public
-- problemset does not carry, and a null here means "not published in the
-- archive", never "solved by nobody".
--
-- Unrated problems are KEPT with a NULL rating. 63.5% of problems carry no rating
-- but only 8.6% of submissions do; excluding them would silently drop those rows
-- from fct_submission through a failed join, which is the silent-sampling-bias
-- class codrona.md section 9 treats as a hard error. IRT excludes them from
-- difficulty priors while keeping their responses - a modelling decision, not a
-- warehouse one.
--
-- THE PROBLEMSET JOIN IS LEFT AND MUST STAY LEFT. It covers 11,809 problems
-- against this dimension's 35,566. The 23,802 it omits are 21,881 gym problems
-- plus 1,921 from mirror, practice and special-format contests - all 487 such
-- contests were resolved against contest.list, so this is the archive's own
-- scope rather than a gap. An inner join would drop them here and drop their
-- submissions from fct_submission through a failed dimension lookup.
--
-- SPDX-License-Identifier: AGPL-3.0-or-later

{{ config(materialized='table') }}

with attribute_sets as (

    select distinct
        problem_id,
        problem_contest_id,
        problem_index,
        problem_name,
        problem_rating,
        problem_points,
        problemset_name,
        problem_tags,
        problem_type
    from {{ ref('stg_cf_submissions') }}

),

ranked as (

    select
        attribute_sets.*,
        row_number() over (
            partition by problem_id
            order by
                len(problem_tags) desc,
                cast(problem_tags as varchar) asc
        ) as attribute_rank
    from attribute_sets

),

corpus as (

    select * from ranked where attribute_rank = 1

),

problemset as (

    select
        problem_id,
        problem_rating,
        problem_tags,
        solved_count,
        fetched_at
    from {{ ref('stg_cf_problemset') }}

),

joined as (

    select
        corpus.*,
        problemset.problem_id is not null as in_public_problemset,
        problemset.problem_rating as problemset_rating,
        problemset.problem_tags as problemset_tags,
        problemset.solved_count,
        problemset.fetched_at as problemset_fetched_at
    from corpus
    left join problemset
        on problemset.problem_id = corpus.problem_id

)

select
    md5(problem_id || '|1970-01-01') as problem_sk,
    problem_id as problem_key,
    problem_contest_id,
    problem_index,
    problem_name,
    problem_rating,
    problem_points,
    coalesce(problemset_name, 'codeforces') as problemset_name,
    coalesce(problemset_tags, problem_tags) as problem_tags,
    len(coalesce(problemset_tags, problem_tags)) as tag_count,
    case
        when in_public_problemset then 'problemset'
        else 'submissions'
    end as tags_source,
    problem_rating is null as is_unrated,
    problem_type,
    in_public_problemset,
    problemset_rating,
    solved_count,
    problemset_fetched_at,
    cast('1970-01-01' as timestamp) as valid_from,
    cast(null as timestamp) as valid_to,
    true as is_current
from joined
