"""Census of the IBM Project CodeNet metadata layer.

CodeNet ships one CSV per problem plus a ``problem_list.csv`` index. This module
reads every one of them with DuckDB and reports the facts a staging model has to
be written against: the true submission count, the complete status vocabulary,
the AIZU/AtCoder split, and the language and date ranges.

Why a committed module rather than a shell one-liner. The status vocabulary is a
product decision - it determines the ``verdict_class`` and ``is_evidence``
mapping for half the corpus - and anything settling a claim of that kind belongs
in the repo where it can be re-run, not in ``/tmp`` where it is lost. The same
reasoning produced ``codeforces/count.py``.

Two guards carry over from the Codeforces work, both earned the hard way:

* Files are enumerated with :meth:`pathlib.Path.iterdir` and the count of files
  actually read is compared against the count on disk, failing hard on any gap.
  A pipeline that never compares those two numbers cannot detect a silent skip,
  which is exactly how 322 landing files went missing from a Spark read.
* The total is asserted against the published figure rather than merely
  reported. A census that agrees with IBM's own count is evidence the read was
  complete; one that quietly disagrees is a bug, not a discovery.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from codrona_lens.codeforces.count import CODENET_PERSON_LEVEL

PROBLEM_LIST = "problem_list.csv"

# Columns of a per-problem metadata CSV, in file order. Declared explicitly so a
# schema change upstream surfaces as a load error rather than a silent shift of
# every field one column to the left.
SUBMISSION_COLUMNS: dict[str, str] = {
    "submission_id": "VARCHAR",
    "problem_id": "VARCHAR",
    "user_id": "VARCHAR",
    "date": "BIGINT",
    "language": "VARCHAR",
    "original_language": "VARCHAR",
    "filename_ext": "VARCHAR",
    "status": "VARCHAR",
    "cpu_time": "BIGINT",
    "memory": "BIGINT",
    "code_size": "BIGINT",
    "accuracy": "VARCHAR",
}


@dataclass
class Census:
    """Everything measured in one pass over the metadata layer."""

    metadata_dir: str
    counted_at: str
    files_on_disk: int
    files_read: int
    submissions: int
    expected_submissions: int
    matches_expected: bool
    distinct_problems: int
    distinct_users: int
    problems_in_index: int
    problems_by_dataset: dict[str, int] = field(default_factory=dict)
    status_counts: dict[str, int] = field(default_factory=dict)
    language_counts: dict[str, int] = field(default_factory=dict)
    submissions_with_accuracy: int = 0
    empty_problem_files: list[str] = field(default_factory=list)
    earliest_submission: str = ""
    latest_submission: str = ""


def metadata_files(metadata_dir: Path) -> list[Path]:
    """Return every per-problem CSV, excluding the index.

    ``iterdir`` rather than ``glob``: the landing-zone lesson generalises. A
    census that cannot see a file cannot report it missing.
    """
    return sorted(
        path
        for path in metadata_dir.iterdir()
        if path.is_file() and path.suffix == ".csv" and path.name != PROBLEM_LIST
    )


def classify_unread(files: list[Path], seen: set[str]) -> list[str]:
    """Account for every file that produced no rows.

    A file contributing zero rows never appears in ``filename``, so comparing
    that count against files-on-disk alone raises a false alarm: CodeNet ships
    five problems with no submissions at all, and a header-only CSV is a
    legitimate empty file rather than a silent skip.

    Every unread file is therefore opened and checked. One line means a header
    and nothing else, which is accounted for and reported. More than one line
    means the file had rows that did not reach the reader - the silent-skip
    shape - and that raises rather than being absorbed.
    """
    unread = [path for path in files if str(path) not in seen]
    empty: list[str] = []
    for path in unread:
        with path.open(encoding="utf-8", errors="replace") as handle:
            lines = sum(1 for _ in handle)
        if lines <= 1:
            empty.append(path.name)
        else:
            msg = (
                f"file gap: {path} holds {lines} lines but contributed no rows "
                "to the read. Refusing to report a census over an incomplete set."
            )
            raise RuntimeError(msg)
    return sorted(empty)


def _problem_index(con: duckdb.DuckDBPyConnection, path: Path) -> Counter[str]:
    con.read_csv(str(path), header=True).create_view("problem_index")
    sql = "select dataset, count(*) from problem_index group by 1"
    rows = con.execute(sql).fetchall()
    return Counter({str(dataset): int(n) for dataset, n in rows})


def run_census(metadata_dir: Path) -> Census:
    """Read every metadata CSV and return the measured facts."""
    index_path = metadata_dir / PROBLEM_LIST
    if not index_path.is_file():
        msg = f"{PROBLEM_LIST} not found in {metadata_dir}"
        raise FileNotFoundError(msg)

    files = metadata_files(metadata_dir)
    if not files:
        msg = f"no per-problem metadata CSVs found in {metadata_dir}"
        raise FileNotFoundError(msg)

    con = duckdb.connect()
    paths = [str(path) for path in files]

    # The relation API rather than a parameterised CREATE VIEW: DuckDB cannot
    # prepare a CREATE VIEW, so passing the path list as a bind parameter raises
    # a binder error. Building the SQL by string interpolation instead would put
    # filesystem paths into a query, so the typed API is both the working route
    # and the safe one.
    # The published stubs type the first argument as a single path, but the
    # runtime API accepts a list and that is the whole point here: passing an
    # explicit file list, rather than a glob, is what lets the census compare
    # files-read against files-on-disk. Narrow ignore rather than a cast so the
    # mismatch stays visible if the stubs are widened later.
    relation = con.read_csv(
        paths,  # type: ignore[arg-type]
        header=True,
        dtype=SUBMISSION_COLUMNS,
        filename=True,
    )
    relation.create_view("submissions")

    seen_rows = con.execute("select distinct filename from submissions").fetchall()
    seen = {str(row[0]) for row in seen_rows}
    empty_files = classify_unread(files, seen)
    files_read = len(seen) + len(empty_files)

    totals = con.execute(
        "select count(*), count(distinct problem_id), count(distinct user_id), "
        "count(*) filter (where accuracy is not null and accuracy <> ''), "
        "min(date), max(date) from submissions"
    ).fetchone()
    if totals is None:
        msg = "aggregate query returned no row; the read produced nothing"
        raise RuntimeError(msg)
    submissions = int(totals[0])
    distinct_problems = int(totals[1])
    distinct_users = int(totals[2])
    with_accuracy = int(totals[3])
    earliest = datetime.fromtimestamp(int(totals[4]), tz=UTC).isoformat()
    latest = datetime.fromtimestamp(int(totals[5]), tz=UTC).isoformat()

    status_rows = con.execute(
        "select status, count(*) as n from submissions group by 1 order by n desc"
    ).fetchall()
    language_rows = con.execute(
        "select language, count(*) as n from submissions group by 1 order by n desc"
    ).fetchall()

    by_dataset = _problem_index(con, index_path)
    problems_in_index = sum(by_dataset.values())
    con.close()

    return Census(
        metadata_dir=str(metadata_dir),
        counted_at=datetime.now(tz=UTC).isoformat(),
        files_on_disk=len(files),
        files_read=files_read,
        submissions=submissions,
        expected_submissions=CODENET_PERSON_LEVEL,
        matches_expected=submissions == CODENET_PERSON_LEVEL,
        distinct_problems=distinct_problems,
        distinct_users=distinct_users,
        problems_in_index=problems_in_index,
        problems_by_dataset=dict(by_dataset),
        status_counts={str(s): int(n) for s, n in status_rows},
        language_counts={str(lang): int(n) for lang, n in language_rows},
        submissions_with_accuracy=with_accuracy,
        empty_problem_files=empty_files,
        earliest_submission=earliest,
        latest_submission=latest,
    )


def write_report(census: Census, reports_dir: Path) -> Path:
    """Write a dated report JSON so the census is reproducible provenance."""
    reports_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%S")
    path = reports_dir / f"codenet_census_{stamp}.json"
    payload: dict[str, Any] = asdict(census)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return path


def format_census(census: Census) -> str:
    """Human-readable summary for the terminal."""
    lines = [
        f"metadata dir          {census.metadata_dir}",
        f"files on disk         {census.files_on_disk:,}",
        f"files read            {census.files_read:,}",
        f"submissions           {census.submissions:,}",
        f"expected (published)  {census.expected_submissions:,}",
        f"matches expected      {census.matches_expected}",
        f"distinct problems     {census.distinct_problems:,}",
        f"problems in index     {census.problems_in_index:,}",
        f"distinct users        {census.distinct_users:,}",
        f"rows with accuracy    {census.submissions_with_accuracy:,}",
        f"empty problem files   {len(census.empty_problem_files)}",
        f"earliest submission   {census.earliest_submission}",
        f"latest submission     {census.latest_submission}",
        "",
        "problems by dataset",
    ]
    for dataset, n in sorted(census.problems_by_dataset.items()):
        lines.append(f"  {dataset:<24} {n:,}")
    lines.append("")
    lines.append("status vocabulary")
    for status, n in census.status_counts.items():
        lines.append(f"  {status:<24} {n:,}")
    lines.append("")
    lines.append("top languages")
    for lang, n in list(census.language_counts.items())[:12]:
        lines.append(f"  {lang:<24} {n:,}")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--metadata-dir",
        type=Path,
        required=True,
        help="Project_CodeNet/metadata directory",
    )
    parser.add_argument(
        "--reports-dir",
        type=Path,
        default=None,
        help="where to write the dated report JSON (skipped if omitted)",
    )
    args = parser.parse_args()

    census = run_census(args.metadata_dir)
    print(format_census(census))

    if args.reports_dir is not None:
        path = write_report(census, args.reports_dir)
        print(f"\nreport {path}")

    if not census.matches_expected:
        delta = census.submissions - census.expected_submissions
        print(
            f"\nWARNING: counted {census.submissions:,}, published figure is "
            f"{census.expected_submissions:,} (delta {delta:+,}). "
            "Do not quote either number until this is explained."
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
