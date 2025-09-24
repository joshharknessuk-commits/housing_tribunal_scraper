"""Rescrape tribunal decisions and backfill missing PDFs.

This script mirrors the progress output from the TypeScript `scrape-all.ts`
worker. It reads existing `cases` rows, revisits their GOV.UK pages to pick up
any newly linked PDFs, classifies each document heuristically, and inserts new
rows into a `documents` table.

Usage:
    python scripts/rescrape_cases.py

Environment variables (see `.env.example` for defaults):
    DATABASE_URL          Postgres connection string (required)
    BATCH_SIZE            Number of cases processed per page (default 200)
    DELAY_MS              Milliseconds sleep between page batches
    MAX_PAGES             Safety upper bound on pagination
    CURSOR_NAME           Name of progress cursor row (default rescrape_progress)
    STORE_PDF_BYTES       When "1" download PDF bytes and persist metadata
    ENABLE_BLOB_UPLOAD    When "1" call `upload_to_blob` for external storage

Tables expected in the target database:
  cases(govuk_slug PRIMARY KEY, html_url, title, category, subcategory,
        published_at, decision_date, updated_at)
  documents(case_id, pdf_url UNIQUE, sha256, bytes, mime, blob_url, filename,
            downloaded_at, processed, document_type, classification)
  cursors(name PRIMARY KEY, last_seen_slug, last_run_at)
"""

from __future__ import annotations

import hashlib
import json
import mimetypes
import os
import re
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import psycopg
from bs4 import BeautifulSoup

from scraper.session import build_http_session


# -----------------------------
# Config (env with sensible defaults)
# -----------------------------
DATABASE_URL = os.environ.get("DATABASE_URL")
BATCH_TABLE = os.environ.get("DOCUMENTS_TABLE", "documents")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "200"))
DELAY_MS = int(os.environ.get("DELAY_MS", "200"))
MAX_PAGES = int(os.environ.get("MAX_PAGES", "100000"))
CURSOR_NAME = os.environ.get("CURSOR_NAME", "rescrape_progress")

STORE_PDF_BYTES = os.environ.get("STORE_PDF_BYTES", "1") == "1"
ENABLE_BLOB_UPLOAD = os.environ.get("ENABLE_BLOB_UPLOAD", "0") == "1"


# -----------------------------
# Small utils
# -----------------------------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def sleep_ms(ms: int):
    if ms > 0:
        time.sleep(ms / 1000.0)


def sha256_bytes(buf: bytes) -> str:
    return hashlib.sha256(buf).hexdigest()


def normalized_pathname(url: str) -> str:
    try:
        parsed = urlparse(url)
        return re.sub(r"/+$", "", parsed.path)
    except Exception:  # pragma: no cover - defensive
        return url


def classify_document(link_text: str, surrounding_text: str, href: str) -> str:
    """Heuristically assign document type: reasons | decision | unknown."""

    text = f"{link_text} {surrounding_text} {href}".lower()

    reasons_patterns = (
        r"statement of reasons",
        r"\breasons\b",
        r"decision and reasons",
        r"reasons for decision",
        r"full reasons",
    )
    decision_patterns = (
        r"\bdecision\b",
        r"tribunal decision",
        r"determination",
        r"judgment",
        r"judgement",
    )

    if any(re.search(pattern, text) for pattern in reasons_patterns):
        return "reasons"
    if any(re.search(pattern, text) for pattern in decision_patterns):
        return "decision"

    fname = href.split("/")[-1].lower()
    if any(keyword in fname for keyword in ("reasons", "reason")):
        return "reasons"
    if any(keyword in fname for keyword in ("decision", "determination", "judgment", "judgement")):
        return "decision"

    return "unknown"


def extract_pdfs_from_decision_page(html_url: str, session):
    response = session.get(html_url)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    title_el = soup.find(["h1", "h2"])
    title = title_el.get_text(strip=True) if title_el else None

    def get_meta(name: str) -> Optional[str]:
        tag = soup.find("meta", attrs={"name": name})
        if tag and tag.get("content"):
            return tag["content"]
        return None

    category = get_meta("govuk:section")
    subcategory = get_meta("govuk:taxonomy")

    def find_date_like(label_keywords: List[str]) -> Optional[str]:
        time_tag = soup.find("time")
        if time_tag and (time_tag.get("datetime") or time_tag.get_text(strip=True)):
            return time_tag.get("datetime") or time_tag.get_text(strip=True)

        for dt in soup.find_all(["dt", "strong", "b", "span"]):
            text = dt.get_text(" ", strip=True).lower()
            if any(keyword in text for keyword in label_keywords):
                dd = dt.find_next(["dd", "span", "time", "p"])
                if dd:
                    return dd.get_text(" ", strip=True)
        return None

    published = find_date_like(["published", "date published"])
    decision_date = find_date_like(["decision", "date decision", "decided", "decision date"])

    pdfs = []
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        if not href.lower().endswith(".pdf"):
            continue
        pdf_url = urljoin(html_url, href)
        link_text = anchor.get_text(" ", strip=True)
        parent_text = anchor.parent.get_text(" ", strip=True) if anchor.parent else link_text
        document_type = classify_document(link_text, parent_text, href)
        pdfs.append(
            {
                "url": pdf_url,
                "link_text": link_text,
                "context_text": parent_text,
                "document_type": document_type,
            }
        )

    meta = {
        "title": title,
        "category": category,
        "subcategory": subcategory,
        "published": published,
        "decisionDate": decision_date,
    }
    return meta, pdfs


def download_pdf(url: str, session):
    response = session.get(url)
    response.raise_for_status()
    content = response.content
    mime = response.headers.get("Content-Type") or mimetypes.guess_type(url)[0] or "application/pdf"
    return content, len(content), mime


def upload_to_blob(buf: bytes, sha256_hex: str) -> Optional[str]:
    if not ENABLE_BLOB_UPLOAD:
        return None
    # Placeholder for blob storage integration (S3/GCS/Azure etc.).
    return None


# -----------------------------
# Database helpers (psycopg v3)
# -----------------------------
def get_resume_offset(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT last_seen_slug FROM cursors WHERE name=%s LIMIT 1", (CURSOR_NAME,))
        row = cur.fetchone()
        if not row or not row[0]:
            return 0
        try:
            data = json.loads(row[0])
            return int(data.get("offset", 0))
        except Exception:
            return 0


def save_progress(conn, offset: int):
    payload = json.dumps({"offset": offset, "timestamp": now_utc().isoformat()})
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM cursors WHERE name=%s LIMIT 1", (CURSOR_NAME,))
        row = cur.fetchone()
        if row:
            cur.execute(
                "UPDATE cursors SET last_seen_slug=%s, last_run_at=NOW() WHERE id=%s",
                (payload, row[0]),
            )
        else:
            cur.execute(
                "INSERT INTO cursors (name, last_seen_slug, last_run_at) VALUES (%s, %s, NOW())",
                (CURSOR_NAME, payload),
            )
    conn.commit()


def ensure_cursor_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS cursors (
                id serial PRIMARY KEY,
                name text UNIQUE NOT NULL,
                last_seen_slug text,
                last_run_at timestamptz
            )
            """
        )
    conn.commit()


def db_counts(conn) -> Tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM cases")
        cases = int(cur.fetchone()[0])
        cur.execute(f"SELECT COUNT(*) FROM {BATCH_TABLE}")
        documents = int(cur.fetchone()[0])
        return cases, documents


def upsert_case_meta(conn, slug: str, html_url: str, meta: Dict) -> Tuple[int, bool]:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO cases (govuk_slug, html_url, title, category, subcategory, published_at, decision_date)
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
            """,
            (
                slug,
                html_url,
                meta.get("title"),
                meta.get("category"),
                meta.get("subcategory"),
                meta.get("published"),
                meta.get("decisionDate"),
            ),
        )
        row = cur.fetchone()
        return int(row[0]), bool(row[1])


def document_exists_for_case(conn, case_id: int, pdf_url: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM documents WHERE case_id=%s AND pdf_url=%s LIMIT 1", (case_id, pdf_url))
        return cur.fetchone() is not None


def insert_document_row(
    conn,
    *,
    case_id: int,
    pdf_url: str,
    sha256_hex: Optional[str],
    bytes_len: Optional[int],
    mime: Optional[str],
    blob_url: Optional[str],
    filename: Optional[str],
    document_type: Optional[str],
    classification_method: str,
) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO {table} (
              case_id, pdf_url, sha256, bytes, mime, blob_url, filename,
              downloaded_at, processed, document_type, document_classification_method
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), FALSE, %s, %s)
            ON CONFLICT DO NOTHING
            RETURNING id
            """.format(table=BATCH_TABLE),
            (
                case_id,
                pdf_url,
                sha256_hex,
                bytes_len,
                mime,
                blob_url,
                filename,
                document_type,
                classification_method,
            ),
        )
        return cur.fetchone() is not None


# -----------------------------
# Main re-scrape job
# -----------------------------
def rescrape_and_classify():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is required")

    print("========================================")
    print("Starting RE-SCRAPE of tribunal decisions")
    print("========================================")
    print("Options:")
    print(f"  - Batch size: {BATCH_SIZE} cases/page")
    print(f"  - Delay between pages: {DELAY_MS}ms")
    print(f"  - Max pages: {MAX_PAGES}\n")
    print(f"  - Documents table: {BATCH_TABLE}")

    total_created_cases = 0
    total_updated_cases = 0
    total_new_docs = 0
    total_errors = 0
    items_processed = 0
    page_index = 0

    start_ts = time.time()

    http_session = build_http_session(timeout=30)
    download_session = build_http_session(timeout=60)

    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute("SET SESSION statement_timeout = '10min';")
        ensure_cursor_table(conn)

        init_cases, init_docs = db_counts(conn)
        print("Current database state:")
        print(f"  - Cases: {init_cases}")
        print(f"  - Documents: {init_docs}\n")

        start_offset = get_resume_offset(conn)
        print(f"Resuming from offset: {start_offset}\n")

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM cases")
            total_cases = int(cur.fetchone()[0])

        while page_index < MAX_PAGES:
            offset = start_offset + page_index * BATCH_SIZE

            if offset >= total_cases:
                print("No more cases to process. Re-scrape complete!")
                save_progress(conn, offset)
                break

            print(f"\n📄 Page {page_index + 1} (offset {offset})")
            print("─" * 40)

            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, govuk_slug, html_url
                    FROM cases
                    ORDER BY id
                    LIMIT %s OFFSET %s
                    """,
                    (BATCH_SIZE, offset),
                )
                case_rows = cur.fetchall()

            if not case_rows:
                print("No cases returned. Stopping.")
                save_progress(conn, offset)
                break

            page_created = 0
            page_updated = 0
            page_docs = 0
            page_start = time.time()

            for case_id, slug, html_url in case_rows:
                items_processed += 1
                try:
                    meta, pdfs = extract_pdfs_from_decision_page(html_url, http_session)

                    try:
                        _, inserted = upsert_case_meta(conn, normalized_pathname(html_url), html_url, meta)
                        if inserted:
                            total_created_cases += 1
                            page_created += 1
                            print("+", end="", flush=True)
                        else:
                            total_updated_cases += 1
                            page_updated += 1
                            print(".", end="", flush=True)
                    except Exception as exc:  # noqa: BLE001
                        total_errors += 1
                        print(f"\n  ⚠️  Case upsert error: {exc}")
                        conn.rollback()

                    for pdf in pdfs:
                        pdf_url = pdf["url"]
                        doc_type = pdf["document_type"]
                        if doc_type not in {"reasons", "decision"}:
                            doc_type = None
                            classification_method = "default"
                        else:
                            classification_method = "filename"

                        if document_exists_for_case(conn, case_id, pdf_url):
                            continue

                        sha_hex = None
                        bytes_len = None
                        mime = None
                        blob_url = None
                        filename = pdf_url.split("/")[-1] if "/" in pdf_url else None

                        try:
                            if STORE_PDF_BYTES or ENABLE_BLOB_UPLOAD:
                                buf, bytes_len, mime = download_pdf(pdf_url, download_session)
                                sha_hex = sha256_bytes(buf)
                                if ENABLE_BLOB_UPLOAD:
                                    blob_url = upload_to_blob(buf, sha_hex)
                            inserted = insert_document_row(
                                conn,
                                case_id=case_id,
                                pdf_url=pdf_url,
                                sha256_hex=sha_hex,
                                bytes_len=bytes_len,
                                mime=mime,
                                blob_url=blob_url,
                                filename=filename,
                                document_type=doc_type,
                                classification_method=classification_method,
                            )
                            if inserted:
                                page_docs += 1
                                total_new_docs += 1
                                print("📄", end="", flush=True)
                        except Exception as exc:  # noqa: BLE001
                            total_errors += 1
                            print(f"\n  ❌ PDF error for {pdf_url}: {exc}")
                            conn.rollback()

                except Exception as exc:  # noqa: BLE001
                    total_errors += 1
                    print(f"\n  ❌ Error processing case {case_id} ({html_url}): {exc}")
                    conn.rollback()

            page_time = f"{time.time() - page_start:.1f}"
            print("")
            print(
                f"  Created: {page_created} | Updated: {page_updated} | "
                f"Docs: {page_docs} | Time: {page_time}s"
            )

            save_progress(conn, offset + len(case_rows))
            sleep_ms(DELAY_MS)

            if (page_index + 1) % 10 == 0:
                elapsed_min = (time.time() - start_ts) / 60.0
                rate = items_processed / max(elapsed_min, 1e-6)
                print("")
                print(f"📊 Progress Report (Page {page_index + 1})")
                print(f"  Total created (cases): {total_created_cases}")
                print(f"  Total updated (cases): {total_updated_cases}")
                print(f"  Total new documents: {total_new_docs}")
                print(f"  Total errors: {total_errors}")
                print(f"  Time elapsed: {elapsed_min:.1f} minutes")
                print(f"  Rate: {rate:.1f} cases/minute")

            page_index += 1

        final_cases, final_docs = db_counts(conn)
        total_minutes = (time.time() - start_ts) / 60.0

        print("\n========================================")
        print("🎉 RE-SCRAPE COMPLETE!")
        print("========================================")
        print(f"Time taken: {total_minutes:.1f} minutes")
        print(f"Pages processed: {page_index}")
        print(f"Items processed: {items_processed}\n")
        print("Results:")
        print(f"  Cases: {init_cases} → {final_cases} (+{final_cases - init_cases})")
        print(
            f"  Documents ({BATCH_TABLE}): {init_docs} → {final_docs} (+{final_docs - init_docs})"
        )
        print(f"  Errors: {total_errors}\n")

        return {
            "created": total_created_cases,
            "updated": total_updated_cases,
            "documents": total_new_docs,
            "errors": total_errors,
            "pagesProcessed": page_index,
            "timeMinutes": round(total_minutes, 2),
        }


if __name__ == "__main__":
    rescrape_and_classify()
