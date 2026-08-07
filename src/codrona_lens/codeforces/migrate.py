"""Rename landing files that Spark's reader cannot see.

Hadoop's FileInputFormat skips any basename beginning with "_" or ".", so a
Codeforces handle like "_WXZY" lands in a file that reads as zero rows with no
warning, silently biasing the corpus against exactly those users. The
collector now encodes the leading character; this brings files written before
that change into line.

    python -m codrona_lens.codeforces.migrate
    python -m codrona_lens.codeforces.migrate --apply

The rename is idempotent and resumable: only names beginning with "_" or "."
are touched, and contents are never read or rewritten - Path.rename is a
metadata operation on one filesystem. An interrupted run finishes by running
the same command again. It refuses to run while the collector looks active,
and refuses to overwrite an existing file.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

from __future__ import annotations

import argparse
import pathlib
import sys
import time

from .collect import encode_leading

SUFFIX = ".jsonl.gz"
ACTIVE_WINDOW_SECONDS = 60.0
DEFAULT_USER_STATUS_DIR = pathlib.Path.home() / "codrona-data/raw/codeforces/user_status"


def target_name(name: str) -> str:
    """The name a landing file should carry."""
    stem = name[: -len(SUFFIX)] if name.endswith(SUFFIX) else name
    return encode_leading(stem) + SUFFIX


def planned_renames(
    user_status_dir: pathlib.Path,
) -> list[tuple[pathlib.Path, pathlib.Path]]:
    """Every hidden landing file paired with the name it should carry."""
    return [
        (path, path.with_name(target_name(path.name)))
        for path in sorted(user_status_dir.glob("*/*" + SUFFIX))
        if path.name.startswith(("_", "."))
    ]


def conflicts(
    plan: list[tuple[pathlib.Path, pathlib.Path]],
) -> list[tuple[pathlib.Path, pathlib.Path]]:
    """Renames whose target already exists. Never clobber irreplaceable data."""
    return [(src, dst) for src, dst in plan if dst.exists()]


def recently_written(user_status_dir: pathlib.Path, window: float = ACTIVE_WINDOW_SECONDS) -> int:
    """Count files touched inside the window - a live collector's fingerprint."""
    now = time.time()
    recent = 0
    for path in user_status_dir.glob("*/*"):
        try:
            if now - path.stat().st_mtime < window:
                recent += 1
        except FileNotFoundError:
            continue
    return recent


def apply_renames(plan: list[tuple[pathlib.Path, pathlib.Path]]) -> int:
    """Rename each planned file, refusing to overwrite an existing target."""
    renamed = 0
    for src, dst in plan:
        if dst.exists():
            raise FileExistsError(f"refusing to overwrite {dst}")
        src.rename(dst)
        renamed += 1
    return renamed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Rename landing files invisible to Spark.")
    parser.add_argument("--dir", type=pathlib.Path, default=DEFAULT_USER_STATUS_DIR)
    parser.add_argument("--apply", action="store_true", help="perform the rename")
    parser.add_argument("--force", action="store_true", help="skip the active check")
    args = parser.parse_args(argv)

    root = args.dir.expanduser()
    if not root.is_dir():
        print(f"no such directory: {root}", file=sys.stderr)
        return 2

    plan = planned_renames(root)
    total = len(list(root.glob("*/*" + SUFFIX)))
    print(f"landing files      {total:>8,}")
    print(f"hidden from spark  {len(plan):>8,}")
    if not plan:
        print("nothing to migrate")
        return 0

    clash = conflicts(plan)
    if clash:
        print(f"ABORT: {len(clash)} target name(s) already exist", file=sys.stderr)
        for src, dst in clash[:5]:
            print(f"  {src.name} -> {dst.name}", file=sys.stderr)
        return 1

    recent = recently_written(root)
    if recent and not args.force:
        window = int(ACTIVE_WINDOW_SECONDS)
        print(
            f"ABORT: {recent} file(s) written in the last {window}s - "
            "collection looks active. Stop the collector, or pass --force.",
            file=sys.stderr,
        )
        return 1

    if not args.apply:
        for src, dst in plan[:10]:
            print(f"  {src.parent.name}/{src.name} -> {dst.name}")
        if len(plan) > 10:
            print(f"  ... and {len(plan) - 10:,} more")
        print("dry run - pass --apply to rename")
        return 0

    renamed = apply_renames(plan)
    left = planned_renames(root)
    print(f"renamed            {renamed:>8,}")
    print(f"still hidden       {len(left):>8,}")
    return 0 if not left else 1


if __name__ == "__main__":
    raise SystemExit(main())
