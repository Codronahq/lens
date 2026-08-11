-- Every CodeContests problem must resolve in the CodeNet problem dimension.
--
-- The two sources were assembled independently - IBM published an index, and
-- DeepMind scraped the judges - so agreement between them is real evidence
-- rather than an echo. Measured at zero orphans across all 3,474 rows.
--
-- EXCEPT rather than NOT IN, deliberately: NOT IN against a subquery holding a
-- single NULL returns zero rows unconditionally, which reads as a clean result
-- while being structurally incapable of failing. That voided a real diagnostic
-- earlier in this phase.
--
-- The reverse direction is NOT tested, because 579 CodeNet problems legitimately
-- have no CodeContests row - DeepMind took a subset - which is why the
-- enrichment join downstream is a LEFT join.

select problem_id from {{ ref('stg_codecontests_problems') }}
except
select problem_key from {{ ref('dim_problem_codenet') }}
