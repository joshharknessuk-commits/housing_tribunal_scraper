"""Discover new GOV.UK housing tribunal decisions and ingest them."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Sequence

import psycopg
from psycopg import sql

from scraper.govuk_listing import ListingEntry, parse_listing_html
from scraper.session import build_http_session
from scripts.find_extra_pdfs import decide_doc_metadata, derive_filename, insert_document
from scripts.rescrape_cases import (
    download_pdf,
    ensure_cursor_table,
    extract_pdfs_from_decision_page,
)

DEFAULT_LISTING_URL = "https://www.gov.uk/residential-property-tribunal-decisions"
DEFAULT_DOCUMENTS_TABLE = "dev.documents"
DEFAULT_CASES_TABLE = "dev.cases"
DEFAULT_CURSOR_NAME = "discover_new_cases"
DEFAULT_MAX_PAGES_WITHOUT_NEW = 2


@dataclass
class CaseInsertResult:
    case_slug: str
    case_id: str
    published_at: datetime | None
    decision_date: datetime | None
    documents: list[str]


@dataclass
class DocumentPreview:
    case_slug: str
    pdf_url: str
    filename: str | None
    sha256: str | None
    bytes_len: int | None
    mime: str | None
    document_type: str | None
    classification_method: str


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def sha256_bytes(buffer: bytes) -> str:
    return hashlib.sha256(buffer).hexdigest()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--listing-url", default=os.getenv("LISTING_URL", DEFAULT_LISTING_URL))
    parser.add_argument("--documents-table", default=os.getenv("DOCUMENTS_TABLE", DEFAULT_DOCUMENTS_TABLE))
    parser.add_argument("--cases-table", default=os.getenv("CASES_TABLE", DEFAULT_CASES_TABLE))
    parser.add_argument("--cursor-name", default=os.getenv("DISCOVER_CURSOR", DEFAULT_CURSOR_NAME))
    parser.add_argument("--max-pages", type=int, help="Maximum listing pages to scan")
    parser.add_argument(
        "--max-stale-pages",
        type=int,
        default=DEFAULT_MAX_PAGES_WITHOUT_NEW,
        help="Stop after this many consecutive pages without inserts",
    )
    parser.add_argument("--timeout", type=float, default=15.0, help="HTTP timeout for listing/detail fetches")
    parser.add_argument("--download-timeout", type=float, default=60.0, help="HTTP timeout for PDF downloads")
    parser.add_argument("--delay-ms", type=int, default=0, help="Delay between listing page fetches")
    parser.add_argument("--dry-run", action="store_true", help="Report only; do not mutate the database")
    parser.add_argument("--csv", dest="csv_path", help="Optional CSV export of newly inserted cases")
    parser.add_argument("--docs-csv", dest="docs_csv_path", help="Optional CSV export of document rows that would be inserted")
    parser.add_argument("--verbose", action="store_true", help="Log each listing processed")
    parser.add_argument("--reset-cursor", action="store_true", help="Clear stored progress before running")
    parser.add_argument("--cutoff", help="Override cutoff datetime (ISO8601 or natural language)")
    return parser.parse_args()


def quote_table(name: str) -> sql.Composed:
    parts = name.split(".")
    identifiers = [sql.Identifier(part) for part in parts]
    if len(identifiers) == 1:
        return identifiers[0]
    return sql.SQL(".").join(identifiers)


def parse_table_reference(name: str) -> tuple[str, str]:
    parts = name.split(".")
    if len(parts) == 1:
        return "public", parts[0]
    if len(parts) == 2:
        return parts[0], parts[1]
    raise ValueError(f"Unexpected table reference: {name}")


def fetch_table_columns(conn: psycopg.Connection, table_name: str) -> set[str]:
    schema, table = parse_table_reference(table_name)
    query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema, table))
        return {row[0] for row in cur.fetchall()}


def fetch_max_column_value(conn: psycopg.Connection, table: sql.Composed, column: str) -> datetime | None:
    query = sql.SQL("SELECT MAX({column}) FROM {table}").format(column=sql.Identifier(column), table=table)
    with conn.cursor() as cur:
        cur.execute(query)
        value = cur.fetchone()[0]
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    return None


def load_existing_slugs(conn: psycopg.Connection, cases_table: sql.Composed, slugs: Sequence[str]) -> set[str]:
    if not slugs:
        return set()
    query = sql.SQL("SELECT govuk_slug FROM {table} WHERE govuk_slug = ANY(%s)").format(table=cases_table)
    with conn.cursor() as cur:
        cur.execute(query, (list(slugs),))
        return {row[0] for row in cur.fetchall() if row[0]}


def determine_cutoff(
    conn: psycopg.Connection,
    cases_table_name: str,
    cases_table: sql.Composed,
) -> datetime | None:
    columns = fetch_table_columns(conn, cases_table_name)
    candidates: list[datetime] = []
    for column in ("published_at", "decision_date", "created_at"):
        if column not in columns:
            continue
        value = fetch_max_column_value(conn, cases_table, column)
        if value:
            candidates.append(value)
    if not candidates:
        return None
    return max(candidates)


def fetch_listing_entries(session, listing_url: str, page: int) -> list[ListingEntry]:
    params = {"page": page} if page > 1 else {}
    target = f"{listing_url}.json"
    response = session.get(target, params=params)
    response.raise_for_status()
    payload = response.json()
    html = payload.get("search_results", "")
    return parse_listing_html(html, listing_url)


def parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
        return parsed.replace(tzinfo=None)
    except ValueError:
        from dateutil import parser as dateparser

        try:
            parsed_any = dateparser.parse(value, dayfirst=True, fuzzy=True)
            if parsed_any:
                return parsed_any.replace(tzinfo=None)
        except (ValueError, TypeError):
            return None
    return None


def maybe_sleep(delay_ms: int):
    if delay_ms > 0:
        time.sleep(delay_ms / 1000.0)

def save_cursor(conn: psycopg.Connection, cursor_name: str, payload: dict):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO cursors (name, last_seen_slug, last_run_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (name)
            DO UPDATE SET last_seen_slug = EXCLUDED.last_seen_slug, last_run_at = NOW()
            """,
            (cursor_name, json.dumps(payload)),
        )
    conn.commit()


def clear_cursor(conn: psycopg.Connection, cursor_name: str):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM cursors WHERE name = %s", (cursor_name,))
    conn.commit()


def write_csv(path: str, rows: Sequence[CaseInsertResult]):
    if not rows:
        return
    import csv

    fieldnames = [
        "case_slug",
        "case_id",
        "published_at",
        "decision_date",
        "documents",
    ]
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "case_slug": row.case_slug,
                    "case_id": row.case_id,
                    "published_at": row.published_at.isoformat() if row.published_at else "",
                    "decision_date": row.decision_date.isoformat() if row.decision_date else "",
                    "documents": "\n".join(row.documents),
                }
            )


def write_docs_csv(path: str, rows: Sequence[DocumentPreview]):
    if not rows:
        return
    import csv

    fieldnames = [
        "case_slug",
        "pdf_url",
        "filename",
        "sha256",
        "bytes",
        "mime",
        "document_type",
        "document_classification_method",
    ]

    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "case_slug": row.case_slug,
                    "pdf_url": row.pdf_url,
                    "filename": row.filename or "",
                    "sha256": row.sha256 or "",
                    "bytes": row.bytes_len or "",
                    "mime": row.mime or "",
                    "document_type": row.document_type or "",
                    "document_classification_method": row.classification_method,
                }
            )


def upsert_case(
    conn: psycopg.Connection,
    cases_table: sql.Composed,
    *,
    slug: str,
    html_url: str,
    meta: dict | None,
) -> tuple[int, bool]:
    payload = meta or {}
    query = sql.SQL(
        """
        INSERT INTO {table} (
            govuk_slug,
            html_url,
            title,
            category,
            subcategory,
            published_at,
            decision_date
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (govuk_slug)
        DO UPDATE SET
            html_url = EXCLUDED.html_url,
            title = EXCLUDED.title,
            category = EXCLUDED.category,
            subcategory = EXCLUDED.subcategory,
            published_at = EXCLUDED.published_at,
            decision_date = EXCLUDED.decision_date,
            updated_at = NOW()
        RETURNING id, (xmax = 0) AS inserted
        """
    ).format(table=cases_table)

    with conn.cursor() as cur:
        cur.execute(
            query,
            (
                slug,
                html_url,
                payload.get("title"),
                payload.get("category"),
                payload.get("subcategory"),
                payload.get("published"),
                payload.get("decisionDate"),
            ),
        )
        row = cur.fetchone()
    return int(row[0]), bool(row[1])


def process_listing_entry(
    conn: psycopg.Connection,
    *,
    entry: ListingEntry,
    cases_table: sql.Composed,
    documents_table: sql.Composed,
    http_session,
    download_session,
    dry_run: bool,
    export_docs: list[DocumentPreview],
    export_docs_enabled: bool,
) -> CaseInsertResult | None:
    meta, pdfs = extract_pdfs_from_decision_page(entry.url, http_session)

    published_str = meta.get("published") if meta else None
    published_at = parse_date(published_str)
    decision_str = meta.get("decisionDate") if meta else None
    decision_at = parse_date(decision_str)

    if dry_run:
        case_id = "dry-run"
    else:
        case_id_int, inserted = upsert_case(
            conn,
            cases_table,
            slug=entry.slug,
            html_url=entry.url,
            meta=meta,
        )
        if not inserted:
            return None
        case_id = str(case_id_int)

    inserted_urls: list[str] = []

    for pdf in pdfs:
        url = pdf.get("url")
        if not url:
            continue

        document_type, classification_method = decide_doc_metadata(pdf)
        filename = derive_filename(url)

        sha_hex: str | None = None
        bytes_len: int | None = None
        mime: str | None = None

        should_download = export_docs_enabled or (not dry_run)
        if should_download:
            try:
                buf, bytes_len, mime = download_pdf(url, download_session)
                sha_hex = sha256_bytes(buf)
            except Exception as exc:  # noqa: BLE001
                print(f"! Failed to download {url}: {exc}")
                if not dry_run:
                    conn.rollback()
                continue

        if not dry_run:
            if sha_hex is None or bytes_len is None:
                print(f"! Missing document metadata for {url}; skipping insert")
                continue
            try:
                inserted = insert_document(
                    conn,
                    documents_table,
                    case_id=case_id,
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
            except Exception as exc:  # noqa: BLE001
                print(f"! Failed to ingest document for {entry.slug}: {exc}")
                conn.rollback()
                continue
        else:
            inserted_urls.append(url)

        if export_docs_enabled:
            export_docs.append(
                DocumentPreview(
                    case_slug=entry.slug,
                    pdf_url=url,
                    filename=filename,
                    sha256=sha_hex,
                    bytes_len=bytes_len,
                    mime=mime,
                    document_type=document_type,
                    classification_method=classification_method,
                )
            )

    return CaseInsertResult(
        case_slug=entry.slug,
        case_id=case_id,
        published_at=published_at,
        decision_date=decision_at,
        documents=inserted_urls,
    )


def main() -> int:
    args = parse_args()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise SystemExit("DATABASE_URL is required")

    docs_table = quote_table(args.documents_table)
    cases_table = quote_table(args.cases_table)

    http_session = build_http_session(timeout=args.timeout)
    download_session = build_http_session(timeout=args.download_timeout)

    listing_url = args.listing_url.rstrip("/")
    if listing_url.endswith(".json"):
        listing_url = listing_url[:-5]

    override_cutoff = None
    if args.cutoff:
        override_cutoff = parse_date(args.cutoff)
        if not override_cutoff:
            raise SystemExit(f"Unable to parse cutoff value: {args.cutoff!r}")

    results: list[CaseInsertResult] = []
    doc_previews: list[DocumentPreview] = []
    export_docs_enabled = bool(args.docs_csv_path)
    pages_without_new = 0
    total_inserted = 0
    total_docs = 0

    start_ts = time.time()

    try:
        with psycopg.connect(db_url) as conn:
            conn.execute("SET SESSION statement_timeout = '5min';")
            ensure_cursor_table(conn)
            if args.reset_cursor:
                clear_cursor(conn, args.cursor_name)

            cutoff = override_cutoff or determine_cutoff(conn, args.cases_table, cases_table)
            if cutoff:
                label = "Cutoff timestamp (override)" if override_cutoff else "Cutoff timestamp"
                print(f"{label}: {cutoff.isoformat()}")
            else:
                print("No cutoff found (empty cases table).")

            page = 1
            while True:
                if args.max_pages is not None and page > args.max_pages:
                    break

                entries = fetch_listing_entries(http_session, listing_url, page)
                if not entries:
                    print(f"No entries returned for page {page}. Stopping.")
                    break

                slugs = [entry.slug for entry in entries]
                existing_slugs = load_existing_slugs(conn, cases_table, slugs)

                new_entries = [entry for entry in entries if entry.slug not in existing_slugs]

                if args.verbose:
                    for entry in entries:
                        marker = "*" if entry.slug not in existing_slugs else "-"
                        decided = entry.decided_at.isoformat() if entry.decided_at else "?"
                        print(f"{marker} {entry.slug} decided={decided}")

                page_inserted = 0

                for entry in new_entries:
                    try:
                        result = process_listing_entry(
                        conn,
                        entry=entry,
                        cases_table=cases_table,
                        documents_table=docs_table,
                        http_session=http_session,
                        download_session=download_session,
                        dry_run=args.dry_run,
                        export_docs=doc_previews,
                        export_docs_enabled=export_docs_enabled,
                    )
                    except Exception as exc:  # noqa: BLE001
                        print(f"! Error processing {entry.slug}: {exc}")
                        conn.rollback()
                        continue
                    if result:
                        page_inserted += 1
                        results.append(result)
                        doc_count = len(result.documents)
                        if args.dry_run:
                            print(f"[dry-run] {entry.slug} ({doc_count} document(s))")
                        else:
                            total_inserted += 1
                            total_docs += doc_count
                            print(f"+ {entry.slug} ({doc_count} document(s))")
                    existing_slugs.add(entry.slug)

                all_past_cutoff = False
                if cutoff is not None:
                    all_past_cutoff = all(
                        entry.decided_at and entry.decided_at <= cutoff for entry in entries
                    )

                if page_inserted == 0:
                    pages_without_new += 1
                else:
                    pages_without_new = 0

                if page_inserted == 0 and all_past_cutoff:
                    print("Reached cutoff; stopping.")
                    break

                if pages_without_new >= args.max_stale_pages:
                    print("No new cases found in recent pages; stopping.")
                    break

                page += 1
                maybe_sleep(args.delay_ms)

            cursor_payload = {
                "last_run": now_utc().isoformat(),
                "pages_processed": page - 1,
                "total_inserted": total_inserted,
            }
            if results:
                cursor_payload["last_case"] = results[-1].case_slug
                if results[-1].published_at:
                    cursor_payload["last_published"] = results[-1].published_at.isoformat()
            save_cursor(conn, args.cursor_name, cursor_payload)

            if args.csv_path and results:
                write_csv(args.csv_path, results)
                print(f"Exported {len(results)} case(s) to {args.csv_path}")

            if args.docs_csv_path and doc_previews:
                write_docs_csv(args.docs_csv_path, doc_previews)
                print(f"Exported {len(doc_previews)} document row(s) to {args.docs_csv_path}")

            duration = time.time() - start_ts
            print("\n========================================")
            print("✅ DISCOVER COMPLETE")
            print("========================================")
            print(f"Time taken        : {duration/60.0:.1f} minutes")
            print(f"Pages processed   : {page - 1}")
            print(f"New cases inserted: {total_inserted}")
            print(f"Documents inserted: {total_docs}")
            if args.dry_run:
                print("Dry run enabled; no database changes were committed.")

            return 0
    finally:
        http_session.close()
        download_session.close()


if __name__ == "__main__":
    sys.exit(main())
