"""This script has been retired.

The functionality once provided here (backfilling missing PDFs from a CSV
export) now lives directly inside `scripts/find_extra_pdfs.py`, which scans the
`cases` table, identifies extra documents, and inserts them in one pass with
cursor-based progress tracking.

Keeping this placeholder avoids import errors for any downstream tooling while
making the deprecation explicit.
"""

raise SystemExit(
    "scripts/backfill_missing_from_csv.py is retired. Use scripts/find_extra_pdfs.py "
    "with its built-in backfill workflow instead."
)
