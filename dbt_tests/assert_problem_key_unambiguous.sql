-- The problem key concatenates contestId and index with no separator, so two
-- different problems could in principle produce one key: contest 921 index '04'
-- and contest 9210 index '4' both render as '92104'. It is not hypothetical
-- that indices are numeric - 1,936 of 35,149 mainline problems have a
-- non-letter index, and 92104 really is contest 921's "Labyrinth-4".
--
-- Measured today the key is clean, confirmed independently: the corpus and the
-- problemset snapshot agree on problem_name for all 11,764 shared keys, which a
-- collision would almost certainly break. But nothing DETECTS a future
-- collision, and the symptom would be two problems' submissions silently merged
-- into one dimension row - a corrupted response matrix with a green build.
--
-- This fires if any key is ever produced by two different (contest, index)
-- pairs. The fix, if it ever fires, is a separator in the key, which is a lake
-- rebuild - hence catching it early matters.

select
    problem_key,
    count(*) as distinct_sources
from (
    select distinct
        problem_id as problem_key,
        problem_contest_id,
        problem_index
    from {{ ref('stg_cf_submissions') }}
)
group by problem_key
having count(*) > 1
