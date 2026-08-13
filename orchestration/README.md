# Orchestration

Airflow 3.3.0 on LocalExecutor, running the Lens pipeline: stage the Codeforces
lake, build the warehouse, publish the observatory export.

## This is a Tier 2 artefact

Nothing in the free-stack ledger hosts a scheduler. This runs on a laptop and in
a demo, and nothing user-facing depends on it — the sustainability architecture
puts the product on static hosting and on-device inference precisely so that a
scheduler being down is not a user-visible event.

## Collection is deliberately not orchestrated

The roadmap line reads "crawl, stage, dbt, publish". The crawl is absent, and
that is a decision rather than an omission.

Codeforces is documented at one request per two seconds. The cohort is 55,484
users and a full pass is a twelve-hour unattended run against a live judge. It
has completed once and is stopped. A DAG task that crawled on a schedule would
either be permanently disabled — implying a capability the pipeline does not
have — or would start a second caller against an API where the home IP is the
rate-limited unit, risking a ban that costs the entire run.

Collection is launched by a person, deliberately:

    setsid nohup python3 -u -m codrona_lens.codeforces.collect \
      > ~/codrona-data/collect.log 2>&1 < /dev/null &

## What the DAG does

| Task | What it does |
|---|---|
| `check_landing_zone` | Compares files in the landing zone against `files_on_disk` in the most recent normalise run report. Short-circuits when unchanged. |
| `normalize_codeforces` | Whole-corpus PySpark normalisation into partitioned silver Parquet. |
| `dbt_build` | Every model, every data test, excluding the `real_data` count pins. |
| `export_observatory` | Writes the committed JSON export and verifies it against the publication rules. |
| `report_export_drift` | Reports whether this run changed what the project publishes. Never fails. |

The short-circuit is the load-bearing piece. Normalising is a whole-corpus Spark
job with no year filter and no cheap subset, so running it on every trigger
would recompute a silver layer that is already correct and make every run cost
an hour. Guarding it re-uses the check the pipeline already performs one layer
down — files on disk against files actually read — and the skip is itself
evidence: it says the input has not moved, where an absent task says nothing.

`ignore_downstream_trigger_rules=False` is required. Airflow's default cascades
a skip through every downstream task, which would mean an unchanged landing zone
also skipped the build and the export. With it set, the skip stops at the
normalise task and `dbt_build` runs under `none_failed`.

## Running it

    cd orchestration
    echo "AIRFLOW_UID=$(id -u)" > .env
    docker compose build          # first build pulls Airflow and installs Java
    docker compose up -d
    docker compose ps

The API server is at <http://localhost:8081>, username `airflow`, password
`airflow`. **Port 8081 is deliberate.** Airflow defaults to 8080 and this host
runs a sentinelops kind cluster that publishes 8080; remapping ours rather than
theirs is the standing rule. 8080 was free when this was written, which is
exactly why the choice is recorded here rather than left to chance.

The DAG is paused on creation. Unpause it, then trigger it manually — the
schedule is `None` because there is no upstream that changes on a clock.

Tear down:

    docker compose down -v        # -v also drops the metadata database

## Mounts

The repository is bind-mounted at `/opt/codrona-lens` and `PYTHONPATH` points at
its `src`, so the code that runs is the code under version control rather than a
copy baked into an image that drifts the moment either is edited. The lake and
warehouse are mounted from `~/codrona-data` — 14 GB, never committed.

Override either with `CODRONA_REPO` and `CODRONA_DATA_DIR` in `.env`.

## Two upstream details

The compose file in the Airflow repository is a **docs template**: its image
line reads `apache/airflow:|version|` literally, and only the copy rendered onto
the documentation site has that substituted. Pulling it from GitHub and running
it fails on an invalid image name.

The path also moved in Airflow 3. It now lives under `airflow-core/docs/`, so
every 2.x-era instruction pointing at `docs/apache-airflow/howto/` returns a
404.

This stack is derived from that file rather than copied, because upstream ships
CeleryExecutor with redis, a worker and flower — eight services to run four
tasks on one machine — and roughly half its comments describe that architecture.
Deleting the services while keeping the prose would leave a file that lies about
itself.
