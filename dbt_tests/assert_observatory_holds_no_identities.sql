-- No observatory table may carry a handle, a name or a problem title.
--
-- These five tables are the only models in the warehouse built to be
-- published. Everything else is local, and codrona.md is explicit that a local
-- warehouse is not publication while a release is. So the constraint that
-- matters here is not correctness but exposure: an aggregate that carries
-- user_key stops being an aggregate and becomes a corpus release with extra
-- steps, and link-never-host means a problem NAME is Codeforces material even
-- when a problem id is not.
--
-- This reads the catalog rather than the data, so it fails the moment a column
-- is ADDED, not the moment someone notices what a chart is rendering. Adding
-- an identifying column to an observatory model should be impossible by
-- accident; making it deliberate means editing this test, which is a decision
-- with a reviewer.

-- This test reads the catalog, not a model, so dbt cannot infer what it
-- depends on and would leave it out of any observatory-scoped selection
-- while the run still reported success. The edge is declared explicitly.
-- depends_on: {{ ref('obs_country_participation') }}
-- depends_on: {{ ref('obs_organization_participation') }}
-- depends_on: {{ ref('obs_rating_distribution') }}
-- depends_on: {{ ref('obs_activity_by_year') }}
-- depends_on: {{ ref('obs_tag_landscape') }}

with observatory_columns as (

    select table_name, column_name
    from information_schema.columns
    where table_name like 'obs_%'

)

select
    table_name,
    column_name,
    'identifying column in a publication table' as failure
from observatory_columns
where column_name in (
    'user_key', 'user_sk', 'handle', 'collected_via_handle', 'author_handles',
    'first_name', 'last_name', 'city', 'avatar', 'title_photo',
    'problem_name', 'problem_title', 'statement'
)
