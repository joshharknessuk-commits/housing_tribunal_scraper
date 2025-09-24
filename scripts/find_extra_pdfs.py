"""Scan tribunal cases for missing PDFs and optionally backfill them.

The script walks the `cases` table in batches, visits each GOV.UK decision
page, and compares the on-page PDF links with the rows already present in the
`documents` table. When it finds additional PDFs (and the page exposes two or
more documents), it can insert the missing rows directly into the database using
the same `case_id`.

Features
- Resume progress via the shared `cursors` table (row name configurable).
- Optional dry-run / report-only mode.
- Optional CSV export of the cases that still have missing documents.
- PDF download + SHA256 hashing so that inserts satisfy the `sha256` NOT NULL
  constraint in `dev.documents`.

Example:
    DATABASE_URL=... \
    python3 scripts/find_extra_pdfs.py --batch-size 100 --download-bytes 1

Use `--dry-run` to see what would be inserted without mutating the database.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import psycopg
from psycopg import sql

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scraper.session import build_http_session
from scripts.rescrape_cases import (  # type: ignore
    ensure_cursor_table,
    extract_pdfs_from_decision_page,
    download_pdf,
    sha256_bytes,
)

DEFAULT_DOCUMENTS_TABLE = "dev.documents"
DEFAULT_CASES_TABLE = "dev.cases"
DEFAULT_CURSOR_NAME = "extra_pdfs_progress"
DEFAULT_BATCH_SIZE = 200


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class CaseRow:
    case_id: str
    slug: str | None
    html_url: str | None


@dataclass
class CaseResult:
    case_id: str
    slug: str | None
    html_url: str | None
    db_count: int
    web_count: int
    missing_urls: list[str]
    inserted_urls: list[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--documents-table", default=os.getenv("DOCUMENTS_TABLE", DEFAULT_DOCUMENTS_TABLE))
    parser.add_argument("--cases-table", default=os.getenv("CASES_TABLE", DEFAULT_CASES_TABLE))
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--timeout", type=float, default=15.0, help="HTTP timeout for HTML fetches")
    parser.add_argument("--download-timeout", type=float, default=60.0, help="HTTP timeout when downloading PDFs")
    parser.add_argument("--limit", type=int, help="Maximum number of cases to process this run")
    parser.add_argument("--cursor-name", default=os.getenv("EXTRA_PDFS_CURSOR", DEFAULT_CURSOR_NAME))
    parser.add_argument("--reset-cursor", action="store_true", help="Start from the beginning (clears stored offset)")
    parser.add_argument("--dry-run", action="store_true", help="Report only; do not insert documents")
    parser.add_argument("--no-backfill", action="store_true", help="Disable inserts even if not in dry-run mode")
    parser.add_argument("--list-missing", action="store_true", help="Print missing PDF URLs for each hit")
    parser.add_argument("--csv", dest="csv_path", help="Export cases with missing PDFs to CSV")
    parser.add_argument("--delay-ms", type=int, default=0, help="Delay between batches to reduce load")
    parser.add_argument("--verbose", action="store_true", help="Log every case, not just hits")
    return parser.parse_args()


def quote_table(name: str) -> sql.Composed:
    parts = name.split(".")
    identifiers = [sql.Identifier(part) for part in parts]
    if len(identifiers) == 1:
        return identifiers[0]
    return sql.SQL(".").join(identifiers)


def ensure_progress(conn: psycopg.Connection, cursor_name: str):
    ensure_cursor_table(conn)
    conn.commit()
    if not cursor_name:
        raise ValueError("cursor_name must be non-empty")


def clear_progress(conn: psycopg.Connection, cursor_name: str):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM cursors WHERE name = %s", (cursor_name,))
    conn.commit()


def load_progress(conn: psycopg.Connection, cursor_name: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT last_seen_slug FROM cursors WHERE name=%s", (cursor_name,))
        row = cur.fetchone()
    if not row or not row[0]:
        return 0
    try:
        data = json.loads(row[0])
        return int(data.get("offset", 0))
    except Exception:
        return 0


def save_progress(conn: psycopg.Connection, cursor_name: str, offset: int):
    payload = json.dumps({"offset": offset, "timestamp": now_utc().isoformat()})
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO cursors (name, last_seen_slug, last_run_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (name)
            DO UPDATE SET last_seen_slug = EXCLUDED.last_seen_slug, last_run_at = NOW()
            """,
            (cursor_name, payload),
        )
    conn.commit()


def fetch_case_batch(
    conn: psycopg.Connection,
    *,
    cases_table: sql.Composed,
    limit: int,
    offset: int,
) -> list[CaseRow]:
    query = sql.SQL(
        """
        SELECT id::text, govuk_slug, html_url
        FROM {cases}
        WHERE html_url IS NOT NULL
        ORDER BY id
        LIMIT %s OFFSET %s
        """
    ).format(cases=cases_table)
    with conn.cursor() as cur:
        cur.execute(query, (limit, offset))
        rows = cur.fetchall()
    return [CaseRow(case_id=r[0], slug=r[1], html_url=r[2]) for r in rows]


def fetch_existing_pdf_urls(
    conn: psycopg.Connection,
    documents_table: sql.Composed,
    case_id: str,
) -> set[str]:
    query = sql.SQL("SELECT pdf_url FROM {docs} WHERE case_id = %s").format(docs=documents_table)
    with conn.cursor() as cur:
        cur.execute(query, (case_id,))
        return {row[0] for row in cur.fetchall() if row[0]}


def insert_document(
    conn: psycopg.Connection,
    documents_table: sql.Composed,
    *,
    case_id: str,
    pdf_url: str,
    sha256_hex: str,
    bytes_len: int,
    mime: str | None,
    filename: str | None,
    document_type: str | None,
    classification_method: str,
) -> bool:
    query = sql.SQL(
        """
        INSERT INTO {table} (
          case_id, pdf_url, sha256, bytes, mime, blob_url, filename,
          downloaded_at, processed, document_type, document_classification_method
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), FALSE, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING id
        """
    ).format(table=documents_table)
    with conn.cursor() as cur:
        cur.execute(
            query,
            (
                case_id,
                pdf_url,
                sha256_hex,
                bytes_len,
                mime,
                None,
                filename,
                document_type,
                classification_method,
            ),
        )
        row = cur.fetchone()
    conn.commit()
    return row is not None


def decide_doc_metadata(pdf: dict) -> tuple[str | None, str]:
    doc_type = pdf.get("document_type")
    if doc_type in {"reasons", "decision"}:
        return doc_type, "filename"
    return None, "default"


def derive_filename(url: str) -> str | None:
    return url.split("/")[-1] if url else None


def process_case(
    conn: psycopg.Connection,
    *,
    documents_table: sql.Composed,
    case: CaseRow,
    http_session,
    download_session,
    backfill_enabled: bool,
    list_missing: bool,
    dry_run: bool,
) -> CaseResult | None:
    html_url = case.html_url
    if not html_url:
        return None

    meta, pdfs = extract_pdfs_from_decision_page(html_url, http_session)
    web_urls = [p["url"] for p in pdfs]
    web_count = len(web_urls)

    existing_urls = fetch_existing_pdf_urls(conn, documents_table, case.case_id)
    db_count = len(existing_urls)

    missing_pdfs = [pdf for pdf in pdfs if pdf["url"] not in existing_urls]
    missing_urls = [pdf["url"] for pdf in missing_pdfs]

    inserted_urls: list[str] = []

    if backfill_enabled and missing_urls and web_count >= 2:
        for pdf in missing_pdfs:
            url = pdf["url"]
            if dry_run:
                continue
            buf, bytes_len, mime = download_pdf(url, download_session)
            sha_hex = sha256_bytes(buf)
            document_type, classification_method = decide_doc_metadata(pdf)
            filename = derive_filename(url)
            inserted = insert_document(
                conn,
                documents_table,
                case_id=case.case_id,
                pdf_url=url,
                sha256_hex=sha_hex,
                bytes_len=bytes_len,
                mime=mime,
                filename=filename,
                document_type=document_type,
                classification_method=classification_method,
            )
            if inserted:
                inserted_urls.append(url)
                existing_urls.add(url)

    if missing_urls and web_count >= 2:
        if list_missing:
            for url in missing_urls:
                print(f"    missing  : {url}")
        return CaseResult(
            case_id=case.case_id,
            slug=case.slug,
            html_url=html_url,
            db_count=db_count,
            web_count=web_count,
            missing_urls=missing_urls,
            inserted_urls=inserted_urls,
        )
    return None


def maybe_write_csv(path: str, rows: Iterable[CaseResult]):
    fieldnames = [
        "case_id",
        "govuk_slug",
        "html_url",
        "db_count",
        "web_count",
        "missing_count",
        "missing_urls",
        "inserted_count",
        "inserted_urls",
    ]
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "case_id": row.case_id,
                    "govuk_slug": row.slug or "",
                    "html_url": row.html_url or "",
                    "db_count": row.db_count,
                    "web_count": row.web_count,
                    "missing_count": len(row.missing_urls),
                    "missing_urls": "\n".join(row.missing_urls),
                    "inserted_count": len(row.inserted_urls),
                    "inserted_urls": "\n".join(row.inserted_urls),
                }
            )


def main() -> int:
    args = parse_args()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise SystemExit("DATABASE_URL is required")

    docs_table = quote_table(args.documents_table)
    cases_table = quote_table(args.cases_table)

    backfill_enabled = (not args.no_backfill) and (not args.dry_run)
    if backfill_enabled and args.dry_run:
        backfill_enabled = False

    if backfill_enabled and args.dry_run:
        raise SystemExit("Internal inconsistency: dry-run cannot backfill")

    if backfill_enabled:
        # We must download bytes to satisfy sha256 NOT NULL
        download_session = build_http_session(timeout=args.download_timeout)
    else:
        download_session = None

    http_session = build_http_session(timeout=args.timeout)

    total_cases = 0
    hits = 0
    inserted_docs = 0
    errors = 0
    exported: list[CaseResult] = []

    print("========================================")
    print("Starting EXTRA-PDF BACKFILL")
    print("========================================")
    print(f"Documents table : {args.documents_table}")
    print(f"Cases table     : {args.cases_table}")
    print(f"Batch size      : {args.batch_size}")
    print(f"Cursor name     : {args.cursor_name}")
    print(f"Dry run         : {args.dry_run}")
    print(f"Backfill        : {backfill_enabled}")
    print(f"CSV export      : {args.csv_path or 'none'}\n")

    start_ts = time.time()

    try:
        with psycopg.connect(db_url) as conn:
            conn.execute("SET SESSION statement_timeout = '5min';")
            ensure_progress(conn, args.cursor_name)

            if args.reset_cursor:
                clear_progress(conn, args.cursor_name)
                print("Cursor reset; starting from offset 0.")

            offset = load_progress(conn, args.cursor_name)
            if args.limit is not None:
                print(f"Processing up to {args.limit} case(s) this run starting at offset {offset}.")
            else:
                print(f"Resuming from offset {offset} (process all remaining cases).")

            # Count total cases once for summary.
            with conn.cursor() as cur:
                cur.execute(sql.SQL("SELECT COUNT(*) FROM {cases} WHERE html_url IS NOT NULL").format(cases=cases_table))
                total_available = int(cur.fetchone()[0])

            processed_this_run = 0

            while offset < total_available:
                if args.limit is not None and processed_this_run >= args.limit:
                    break

                batch_limit = min(args.batch_size, total_available - offset)
                if args.limit is not None:
                    batch_limit = min(batch_limit, args.limit - processed_this_run)

                cases = fetch_case_batch(conn, cases_table=cases_table, limit=batch_limit, offset=offset)
                if not cases:
                    break

                for case in cases:
                    total_cases += 1
                    processed_this_run += 1

                    if args.verbose:
                        print(f"Case {case.case_id} ({case.slug})")

                    try:
                        if download_session is None and backfill_enabled:
                            raise RuntimeError("Download session not initialised")
                        result = process_case(
                            conn,
                            documents_table=docs_table,
                            case=case,
                            http_session=http_session,
                            download_session=download_session,
                            backfill_enabled=backfill_enabled,
                            list_missing=args.list_missing,
                            dry_run=args.dry_run,
                        )
                        if result:
                            hits += 1
                            inserted_docs += len(result.inserted_urls)
                            exported.append(result)
                            print(f"  + {result.case_id} slug={result.slug}")
                            print(f"    page     : {result.html_url}")
                            print(f"    db_count : {result.db_count}")
                            print(f"    web_count: {result.web_count}")
                            if result.inserted_urls:
                                for url in result.inserted_urls:
                                    print(f"    inserted : {url}")
                        else:
                            if args.verbose:
                                print("    no extra PDFs")
                    except Exception as exc:  # noqa: BLE001
                        errors += 1
                        print(f"  ! Error processing case {case.case_id}: {exc}")

                offset += len(cases)
                save_progress(conn, args.cursor_name, offset)

                if args.delay_ms > 0:
                    time.sleep(args.delay_ms / 1000.0)

            print("\nProcessed cases this run: ", processed_this_run)
            print("Total cases with extra PDFs uncovered: ", hits)
            print("Documents inserted: ", inserted_docs)
            print("Errors: ", errors)

    finally:
        http_session.close()
        if download_session:
            download_session.close()

    duration = time.time() - start_ts
    print("\n========================================")
    print("✅ EXTRA-PDF BACKFILL COMPLETE")
    print("========================================")
    print(f"Time taken       : {duration / 60.0:.1f} minutes")
    print(f"Cases processed  : {total_cases}")
    print(f"Cases with extras: {hits}")
    print(f"Docs inserted    : {inserted_docs}")
    print(f"Errors           : {errors}")

    if args.csv_path and exported:
        try:
            maybe_write_csv(args.csv_path, exported)
            print(f"\nExported {len(exported)} case(s) to {args.csv_path}")
        except Exception as exc:  # noqa: BLE001
            print(f"\n⚠️  Failed to write CSV {args.csv_path}: {exc}")

    return 0 if errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
