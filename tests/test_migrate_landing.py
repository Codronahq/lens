"""Tests for the landing-zone filename migration.

Pure filesystem: no Spark, no JVM, no network. The bug these cover deleted
1.1% of collected users from every Spark read without a single warning.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

from __future__ import annotations

import pathlib

import pytest

from codrona_lens.codeforces.collect import encode_leading, safe_filename
from codrona_lens.codeforces.migrate import (
    apply_renames,
    conflicts,
    planned_renames,
    target_name,
)


def _write(root: pathlib.Path, shard: str, name: str, body: bytes = b"x") -> None:
    path = root / shard / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)


def test_ordinary_handle_is_untouched() -> None:
    assert safe_filename("tourist") == "tourist"


def test_leading_underscore_is_encoded() -> None:
    assert safe_filename("_WXZY") == "%5FWXZY"


def test_interior_underscore_survives() -> None:
    assert safe_filename("flying_raijin") == "flying_raijin"


def test_double_underscore_encodes_only_the_first() -> None:
    assert safe_filename("__PACIFIC__") == "%5F_PACIFIC__"


def test_encoding_is_idempotent() -> None:
    assert encode_leading(encode_leading("_a")) == encode_leading("_a")


def test_a_literal_percent_cannot_be_confused_for_an_encoding() -> None:
    assert safe_filename("%5Ffake") == "%255Ffake"


def test_target_name_keeps_the_suffix() -> None:
    assert target_name("_nobita.jsonl.gz") == "%5Fnobita.jsonl.gz"


def test_plan_covers_only_hidden_files(tmp_path: pathlib.Path) -> None:
    _write(tmp_path, "00", "tourist.jsonl.gz")
    _write(tmp_path, "d0", "_nobita.jsonl.gz")
    _write(tmp_path, "e5", ".dotted.jsonl.gz")
    names = sorted(src.name for src, _ in planned_renames(tmp_path))
    assert names == [".dotted.jsonl.gz", "_nobita.jsonl.gz"]


def test_apply_makes_the_zone_visible(tmp_path: pathlib.Path) -> None:
    _write(tmp_path, "00", "tourist.jsonl.gz")
    _write(tmp_path, "d0", "_nobita.jsonl.gz", b"payload")
    assert apply_renames(planned_renames(tmp_path)) == 1
    assert planned_renames(tmp_path) == []
    assert (tmp_path / "d0/%5Fnobita.jsonl.gz").read_bytes() == b"payload"


def test_apply_is_idempotent(tmp_path: pathlib.Path) -> None:
    _write(tmp_path, "d0", "_nobita.jsonl.gz")
    apply_renames(planned_renames(tmp_path))
    assert apply_renames(planned_renames(tmp_path)) == 0


def test_a_clash_is_detected_not_clobbered(tmp_path: pathlib.Path) -> None:
    _write(tmp_path, "d0", "_nobita.jsonl.gz", b"original")
    _write(tmp_path, "d0", "%5Fnobita.jsonl.gz", b"existing")
    plan = planned_renames(tmp_path)
    assert len(conflicts(plan)) == 1
    with pytest.raises(FileExistsError):
        apply_renames(plan)
    assert (tmp_path / "d0/%5Fnobita.jsonl.gz").read_bytes() == b"existing"
