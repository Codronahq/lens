"""Codrona Lens pipeline: stage, build, publish.

WHAT IS DELIBERATELY ABSENT: COLLECTION. The ROADMAP line for this reads "crawl,
stage, dbt, publish", and the crawl is not here. Codeforces is documented at one
request per two seconds, the cohort is 55,484 users, and a full pass is a
twelve-hour unattended run against a live judge that has completed once and is
stopped. Nothing about that belongs on a schedule, and a task that existed only
to sit disabled would be worse than an honest omission - it would imply a
capability the pipeline does not have and nobody would notice it never ran.

WHY THE SCHEDULE IS None. There is no upstream that changes on a clock. The
lake changes when a collection runs, which is a deliberate multi-hour act, so
this DAG is triggered by the person who did that. A daily schedule would produce
a run history that is almost entirely no-ops, and a run history of no-ops trains
you to stop reading it.

THE SHORT-CIRCUIT IS THE INTERESTING PART. Normalising is a whole-corpus Spark
job over 23.6M rows; there is no year filter and no cheap subset. Running it on
every trigger would recompute a silver layer that is already correct and make
every run cost an hour. So it is guarded by the same check the pipeline already
uses one layer down: files on disk against files actually read, taken from the
normalise job's own run report. When the landing zone is unchanged the task
short-circuits, and the skip is itself evidence - it says the input has not
moved, where an absent task would say nothing at all.

`ignore_downstream_trigger_rules=False` matters. The default cascades a skip
through every downstream task, which would mean an unchanged landing zone also
skipped the dbt build and the export. Here the skip stops at the normalise task
and the rest of the pipeline runs on its own trigger rule.

TIER 2. Nothing in the free-stack ledger hosts a scheduler, so this runs on a
laptop and in a demo. It is not on the hot path and nothing user-facing depends
on it.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

from __future__ import annotations

import datetime as dt
import json
import os
import pathlib

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.sdk import DAG

REPO = pathlib.Path(os.environ.get("CODRONA_LENS_REPO", "/opt/codrona-lens"))
DATA = pathlib.Path(os.environ.get("CODRONA_DATA", "/opt/codrona-data"))
# Measured, not guessed: the collector writes one gzipped JSONL per handle
# under raw/codeforces/user_status, sharded by digest prefix. An earlier
# version of this line said "submissions" and produced a plausible wrong
# answer rather than an error - zero files found, so the guard reported the
# zone had changed and normalise ran against nothing.
LANDING = DATA / "raw" / "codeforces" / "user_status"
REPORTS = DATA / "lake" / "_reports"
SILVER = DATA / "lake" / "silver" / "cf_submissions"


def landing_zone_changed() -> bool:
    """True when the Codeforces landing zone differs from what silver last read.

    Reads `files_on_disk` from the most recent cf_submissions run report - a key
    the normalise job already writes - and compares it against a live count.
    Both numbers come from the pipeline's own artefacts rather than from a
    remembered figure, so this cannot drift from what actually happened.

    Absent report means normalise has never run here: return True and let it.
    """
    if not LANDING.is_dir():
        # A path that does not exist finds zero files, and zero files can only
        # ever read as "changed" - so a wrong path never skips, it re-runs a
        # whole-corpus Spark job forever while looking like a working guard.
        # This is the first version of that mistake, caught by a run that died
        # in five seconds on an empty input rather than by anything here.
        raise FileNotFoundError(f"landing zone {LANDING} does not exist")

    on_disk = sum(1 for _ in LANDING.rglob("*.jsonl.gz"))
    if on_disk == 0:
        raise FileNotFoundError(f"landing zone {LANDING} holds no .jsonl.gz files")

    reports = sorted(REPORTS.glob("cf_submissions_*.json"))
    if not reports:
        print(f"no run report in {REPORTS} - normalise has not run; proceeding")
        return True

    latest = reports[-1]
    report = json.loads(latest.read_text(encoding="utf-8"))
    recorded = report.get("files_on_disk")
    read = report.get("files_read")
    print(f"report        {latest.name}")
    print(f"files_on_disk {recorded}  files_read {read}")
    print(f"on disk now   {on_disk}")

    if recorded is None:
        print("report predates the files_on_disk key - proceeding rather than guessing")
        return True
    if not SILVER.exists():
        print(f"no silver at {SILVER} - proceeding")
        return True

    changed = int(recorded) != on_disk
    print("landing zone changed" if changed else "landing zone unchanged - skipping normalise")
    return changed


with DAG(
    dag_id="codrona_lens_pipeline",
    description="Stage the Codeforces lake, build the warehouse, publish the observatory export",
    schedule=None,
    start_date=dt.datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    tags=["codrona", "lens", "tier-2"],
    doc_md=__doc__,
) as dag:
    check_landing_zone = ShortCircuitOperator(
        task_id="check_landing_zone",
        python_callable=landing_zone_changed,
        # Without this, an unchanged landing zone would skip the whole pipeline.
        ignore_downstream_trigger_rules=False,
        doc_md="Compare the landing zone against what the last normalise run recorded reading.",
    )

    normalize_codeforces = BashOperator(
        task_id="normalize_codeforces",
        bash_command=(
            f"cd {REPO} && python3 -m codrona_lens.normalize.cf_submissions "
            f"--input {LANDING} --output {SILVER} --report-dir {REPORTS} --driver-memory 4g"
        ),
        doc_md=(
            "Whole-corpus PySpark normalisation. Driver memory is passed as an argument because "
            "in local mode the JVM is already running by the time a SparkConf value is read."
        ),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {REPO} && dbt build --exclude tag:real_data",
        # Runs whether or not normalise was skipped, but not if it failed.
        trigger_rule="none_failed",
        doc_md=(
            "Builds every model and runs every data test. The real_data-tagged count pins are "
            "excluded here for the same reason CI excludes them: they assert corpus totals."
        ),
    )

    export_observatory = BashOperator(
        task_id="export_observatory",
        bash_command=(f"cd {REPO} && python3 -m codrona_lens.observatory.export --check"),
        doc_md=(
            "Writes the committed JSON export and verifies it: allowlist honoured, no statistics "
            "on cells below the minimum size, and the two population tables still agreeing."
        ),
    )

    report_export_drift = BashOperator(
        task_id="report_export_drift",
        bash_command=(
            f"cd {REPO} && "
            "if git diff --quiet -- exports/observatory; then "
            "echo 'published figures unchanged by this run'; "
            "else echo 'PUBLISHED FIGURES CHANGED - review and commit:'; "
            "git diff --stat -- exports/observatory; fi"
        ),
        doc_md=(
            "Turns 'did this run change what we publish' into a visible answer. It never fails: "
            "a changed export is a fact to review, not an error, and the commit is a human act."
        ),
    )

    check_landing_zone >> normalize_codeforces >> dbt_build
    dbt_build >> export_observatory >> report_export_drift
