"""Model-input construction: the IRT response matrix and the twin key map.

This subpackage turns warehouse submissions into the unit a model consumes. It
lives in ``lens`` rather than ``mind`` because it reads
``main_marts.fct_submission`` through ``codrona_lens.warehouse.connect``, which
G10 makes the only sanctioned route to DuckDB. In ``mind`` it would either open
DuckDB itself, violating that gate, or depend on ``lens`` regardless. The
boundary between the two repositories is the emitted artefact: ``lens`` writes
the matrix, ``mind`` reads a file and never sees the warehouse.

SPDX-License-Identifier: AGPL-3.0-or-later
"""
