"""Collector entry point.

    python -m codrona_lens.codeforces.collect cohort
    python -m codrona_lens.codeforces.collect run --limit-per-band 20
    python -m codrona_lens.codeforces.collect run
    python -m codrona_lens.codeforces.collect status

Atomicity is per user, not per page. A user's pages are held in memory and
written once under an atomic rename, so a crash can never leave a partial file
and resuming costs at most that one user's handful of requests. This is a
deliberate simplification over per-page checkpointing: the largest users are
three pages, so there is nothing worth the extra state.

Interrupting with Ctrl+C is safe here, unlike most long-running jobs: the
current user finishes, the checkpoint commits, and the process exits.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import pathlib
import signal
import sys
import time
import urllib.parse
from types import FrameType
from typing import Any

from .checkpoint import Checkpoint
from .client import (
    CodeforcesClient,
    CodeforcesError,
    HandleNotFound,
)
from .cohort import (
    BAND_NAMES,
    band_counts,
    build_cohort,
    latest_rated_list,
    load_rated_list,
    write_cohort,
)
from .config import Config, load_config
from .limiter import RateLimiter
from .schema import is_person_level, project_submission

_STOP = False


def _handle_signal(signum: int, frame: FrameType | None) -> None:
    global _STOP
    _STOP = True
    print("\n[signal] finishing current user, then stopping.", file=sys.stderr)


def user_path(config: Config, handle: str) -> pathlib.Path:
    shard = hashlib.sha1(handle.encode("utf-8")).hexdigest()[:2]
    safe = urllib.parse.quote(handle, safe="")
    return config.user_status_dir / shard / f"{safe}.jsonl.gz"


def write_rows(path: pathlib.Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, separators=(",", ":"), sort_keys=True))
            fh.write("\n")
    tmp.replace(path)


def collect_user(
    client: CodeforcesClient,
    config: Config,
    handle: str,
) -> tuple[int, int, int]:
    """Fetch one user in full. Returns (rows, pages, person_level_rows)."""
    rows: list[dict[str, Any]] = []
    pages = 0
    person_rows = 0
    from_ = 1

    while True:
        page = client.user_status(handle, from_=from_, count=config.api.page_size)
        pages += 1
        for submission in page:
            if not isinstance(submission, dict):
                continue
            if is_person_level(submission):
                person_rows += 1
            rows.append(project_submission(submission, keep_problem_name=config.keep_problem_name))
        if len(page) < config.api.page_size:
            break
        from_ += len(page)

    write_rows(user_path(config, handle), rows)
    return len(rows), pages, person_rows


def cmd_cohort(config: Config) -> int:
    source = latest_rated_list(config.rated_list_dir)
    users = load_rated_list(source)
    members = build_cohort(users, config.cohort.targets, seed=config.cohort.seed)
    out = config.cohort_dir / f"cohort_{source.stem.split('_')[-1]}.json"
    write_cohort(members, out, seed=config.cohort.seed, source=source.name)

    counts = band_counts(members)
    print(f"source      {source}")
    print(f"population  {len(users)}")
    print(f"cohort      {len(members)}")
    print(f"written     {out}")
    for name in BAND_NAMES:
        print(f"  {name:<28} {counts[name]:>6}")

    with Checkpoint(config.checkpoint_path) as store:
        inserted = store.register_cohort([(m.handle, m.band, m.rating) for m in members])
        store.set_meta("cohort_file", out.name)
        store.set_meta("cohort_seed", str(config.cohort.seed))
    print(f"registered  {inserted} new handles in {config.checkpoint_path}")
    return 0


def cmd_run(config: Config, limit_per_band: int | None, retry_errors: bool) -> int:
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    limiter = RateLimiter(config.api.min_interval_seconds)
    client = CodeforcesClient(
        config.api.base_url,
        limiter,
        timeout=config.api.timeout_seconds,
        max_attempts=config.api.max_attempts,
    )

    with Checkpoint(config.checkpoint_path) as store:
        if retry_errors:
            requeued = store.reset_errors()
            print(f"requeued {requeued} previously errored handles")

        queue = list(store.pending(limit_per_band=limit_per_band))
        total = len(queue)
        print(f"{total} handles queued; interval {config.api.min_interval_seconds}s")
        started = time.monotonic()
        collected = 0

        for index, user in enumerate(queue, start=1):
            if _STOP:
                print("stopped cleanly by signal")
                break
            try:
                rows, pages, person_rows = collect_user(client, config, user.handle)
            except HandleNotFound as exc:
                store.mark_skipped(user.handle, str(exc))
                print(f"[{index}/{total}] {user.handle}: skipped ({exc})")
                continue
            except CodeforcesError as exc:
                store.mark_error(user.handle, str(exc))
                print(f"[{index}/{total}] {user.handle}: ERROR {exc}", file=sys.stderr)
                continue

            store.mark_done(user.handle, rows=rows, pages=pages, person_rows=person_rows)
            collected += rows
            elapsed = time.monotonic() - started
            rate = index / elapsed if elapsed > 0 else 0.0
            remaining = (total - index) / rate if rate > 0 else 0.0
            print(
                f"[{index}/{total}] {user.handle:<24} {user.band:<26} "
                f"rows={rows:<6} pages={pages} "
                f"total={collected} eta={remaining / 3600:.1f}h"
            )

    return 0


def cmd_status(config: Config) -> int:
    with Checkpoint(config.checkpoint_path) as store:
        summary = store.band_summary()

    header = (
        f"{'band':<28}{'total':>7}{'done':>7}{'skip':>6}{'err':>5}"
        f"{'rows':>12}{'person':>12}{'rows/user':>11}"
    )
    print(header)
    print("-" * len(header))

    grand_rows = 0
    grand_person = 0
    projected = 0.0

    for row in summary:
        done = int(row["done"] or 0)
        rows = int(row["rows"] or 0)
        person = int(row["person_rows"] or 0)
        total = int(row["total"] or 0)
        per_user = rows / done if done else 0.0
        grand_rows += rows
        grand_person += person
        projected += per_user * total
        print(
            f"{row['band']!s:<28}{total:>7}{done:>7}"
            f"{int(row['skipped'] or 0):>6}{int(row['errors'] or 0):>5}"
            f"{rows:>12}{person:>12}{per_user:>11.0f}"
        )

    print("-" * len(header))
    print(f"collected rows        {grand_rows}")
    print(f"person-level rows     {grand_person}")
    print(f"projected cohort rows {projected:,.0f}")
    print("\nProjection extrapolates measured rows/user to the full cohort.")
    print("It is an estimate. Nothing public may cite it until the run completes")
    print("and a live count over the landing directory confirms it.")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="codrona-cf-collect")
    parser.add_argument("--config", type=pathlib.Path, default=None)
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("cohort", help="build the stratified cohort from the rated list")

    run = sub.add_parser("run", help="collect submissions for pending handles")
    run.add_argument(
        "--limit-per-band",
        type=int,
        default=None,
        help="cap handles per band; use for a short pilot before the full run",
    )
    run.add_argument(
        "--retry-errors",
        action="store_true",
        help="requeue handles previously marked error",
    )

    sub.add_parser("status", help="progress and rows-per-user by band")

    args = parser.parse_args(argv)
    config = load_config(args.config)

    if args.command == "cohort":
        return cmd_cohort(config)
    if args.command == "run":
        return cmd_run(config, args.limit_per_band, args.retry_errors)
    return cmd_status(config)


if __name__ == "__main__":
    raise SystemExit(main())
