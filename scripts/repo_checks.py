#!/usr/bin/env python3
"""Repository hygiene checks.

Two failures this catches that nothing else would, because both are silent:

  1. A malformed YAML file. GitHub issue forms, dbt schemas, and workflow
     files all fail quietly when their YAML is wrong -- the form simply
     stops rendering, the model stops being recognised.
  2. A relative markdown link pointing at a file that does not exist.
     In a public docs repository these rot with every rename.

Runs with no dependencies beyond PyYAML. Usable locally:
    python3 scripts/repo_checks.py

SPDX-License-Identifier: AGPL-3.0-or-later
"""

from __future__ import annotations

import pathlib
import re
import sys

import yaml

SKIP_DIRS = {".git", "node_modules", ".venv", "target", "__pycache__"}

# Matches [text](target) but not image embeds preceded by '!'.
LINK = re.compile(r"(?<!!)\[[^\]]*\]\(([^)]+)\)")

EXTERNAL_PREFIXES = ("http://", "https://", "mailto:", "tel:", "#")


def walk(root: pathlib.Path, pattern: str):
    for path in root.rglob(pattern):
        if SKIP_DIRS.isdisjoint(path.parts):
            yield path


def check_yaml(root: pathlib.Path) -> list[str]:
    problems: list[str] = []
    for pattern in ("*.yml", "*.yaml"):
        for path in walk(root, pattern):
            try:
                list(yaml.safe_load_all(path.read_text(encoding="utf-8")))
            except yaml.YAMLError as exc:
                problems.append(f"{path}: invalid YAML: {exc}")
            except UnicodeDecodeError as exc:
                problems.append(f"{path}: not valid UTF-8: {exc}")
    return problems


def check_links(root: pathlib.Path) -> list[str]:
    problems: list[str] = []
    for path in walk(root, "*.md"):
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError as exc:
            problems.append(f"{path}: not valid UTF-8: {exc}")
            continue
        for match in LINK.finditer(text):
            target = match.group(1).split("#")[0].strip()
            if not target or target.startswith(EXTERNAL_PREFIXES):
                continue
            if target.startswith("/"):
                resolved = root / target.lstrip("/")
            else:
                resolved = path.parent / target
            if not resolved.exists():
                problems.append(f"{path}: broken link -> {target}")
    return problems


def main() -> int:
    root = pathlib.Path(".").resolve()
    problems = check_yaml(root) + check_links(root)

    if problems:
        print(f"{len(problems)} problem(s) found:\n")
        for problem in problems:
            print(f"  {problem}")
        return 1

    print("YAML valid, internal links resolve.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
