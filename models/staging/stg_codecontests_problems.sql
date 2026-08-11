-- DeepMind CodeContests problem statements, CodeNet-sourced partition only.
--
-- WHAT THIS SOURCE DOES AND DOES NOT PROVIDE. It provides problem statements -
-- the licensed route to statement embeddings under codrona.md section 6, since
-- Codeforces text can never be stored - plus generated test cases the judge
-- service will want. It provides NO difficulty: the difficulty column is 0 on
-- all 3,474 rows of this partition, exactly as CodeNet's own rating column is
-- empty on all 4,053 of its problems.
--
-- That is worth stating plainly rather than burying: neither CodeNet nor
-- CodeContests contributes a single difficulty label. Codeforces is the only
-- labelled-difficulty source we hold, and every AIZU or AtCoder problem's
-- difficulty has to come from IRT over response patterns. It is a constraint on
-- Phase 2's cold start, not a footnote.
--
-- Measured over the pinned revision before this was written:
--
--   3,474 rows, 3,474 distinct problem_id - no dedupe needed
--   2,151 AIZU + 1,323 AtCoder, summing exactly
--   every problem_id resolves in dim_problem_codenet - zero orphans - and the
--     judge agrees on every row across two independent provenance chains
--   579 CodeNet problems have no row here, so enrichment is a LEFT join
--   descriptions run 29 to 12,976 characters, averaging 1,703
--   1,088 descriptions are machine-translated from Japanese, 2,386 are not
--   private_test_count averages 0 - private tests are absent in this partition
--
-- is_description_translated is carried because it is load-bearing for
-- embeddings rather than trivia: machine-translated Japanese embeds differently
-- from native English, and a retrieval model that cannot tell them apart will
-- cluster on translation artefacts instead of on problem content.

{{ config(materialized='view') }}

with source as (

    select * from {{ source('codecontests', 'codenet_problems') }}

)

select
    problem_id,
    full_name as problem_title,
    judge,

    description as statement,
    description_chars as statement_chars,
    is_description_translated,

    time_limit_seconds,
    memory_limit_bytes,
    nullif(input_file, '') as input_file,
    nullif(output_file, '') as output_file,

    public_test_count,
    private_test_count,
    generated_test_count,
    public_test_count + private_test_count + generated_test_count as test_count,

    -- Counts only. The solutions themselves stay upstream at the pinned
    -- revision until Phase 4 needs them; these columns record what is available
    -- to fetch rather than what we hold.
    solution_count,
    incorrect_solution_count

from source
