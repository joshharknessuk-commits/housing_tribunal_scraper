"""Find cases where the website has >= 2 PDFs but the DB has only 1.

- Reads candidates from the DB by grouping dev.documents by case_id
  (exactly 1 row in DB per case) and joining to dev.cases for `govuk_slug` and
  `html_url`.
- Fetches each `html_url`, parses PDF links, and reports cases where the
  website exposes >=2 PDFs and the DB currently has fewer (1).
- Prints results to console; no DB writes.

Env vars:
  DATABASE_URL        Postgres connection
  DOCUMENTS_TABLE     default 'dev.documents'
  CASES_TABLE         default 'dev.cases'

CLI flags:
  --limit N           max cases to check (default 100)
  --offset N          start offset for paging candidates (default 0)
  --list-missing      include missing PDF URLs in the output
  --timeout SEC       HTTP timeout (default 15)
"""

from __future__ import annotations

import argparse
import os
from typing import Iterable, List, Sequence

import psycopg

from scripts.rescrape_cases import extract_pdfs_from_decision_page
from scraper.session import build_http_session


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--documents-table", default=os.getenv("DOCUMENTS_TABLE", "dev.documents"))
    p.add_argument("--cases-table", default=os.getenv("CASES_TABLE", "dev.cases"))
    p.add_argument("--limit", type=int, default=100)
    p.add_argument("--offset", type=int, default=0)
    p.add_argument("--list-missing", action="store_true")
    p.add_argument("--timeout", type=float, default=15.0)
    return p.parse_args()


def fetch_candidates(conn, *, docs_table: str, cases_table: str, limit: int, offset: int) -> Sequence[dict]:
    sql = f"""
        SELECT c.id AS case_id,
               c.govuk_slug,
               c.html_url,
               COUNT(d.id) AS db_count,
               ARRAY_REMOVE(ARRAY_AGG(d.pdf_url), NULL) AS db_urls
        FROM {cases_table} c
        JOIN {docs_table} d ON d.case_id = c.id
        WHERE c.html_url IS NOT NULL
        GROUP BY c.id, c.govuk_slug, c.html_url
        HAVING COUNT(d.id) = 1
        ORDER BY c.id
        LIMIT %s OFFSET %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit, offset))
        cols = [desc.name for desc in cur.description]
        rows = cur.fetchall()
    return [dict(zip(cols, r)) for r in rows]


def main() -> int:
    args = parse_args()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise SystemExit("DATABASE_URL is required")

    print("Scanning candidates (DB has exactly 1 document per case)...")
    print(f"  documents: {args.documents_table}")
    print(f"  cases    : {args.cases_table}")
    print(f"  limit/offset: {args.limit}/{args.offset}")

    total = 0
    hits = 0

    http = build_http_session(timeout=args.timeout)
    try:
        with psycopg.connect(db_url) as conn:
            conn.execute("SET SESSION statement_timeout = '5min';")
            candidates = fetch_candidates(
                conn,
                docs_table=args.documents_table,
                cases_table=args.cases_table,
                limit=args.limit,
                offset=args.offset,
            )

            if not candidates:
                print("No candidates with exactly 1 DB document found in this slice.")
                return 0

            print(f"Checking {len(candidates)} case page(s) on the web...\n")

            for row in candidates:
                total += 1
                case_id = row["case_id"]
                slug = row["govuk_slug"]
                url = row["html_url"]
                db_count = int(row["db_count"]) if row["db_count"] is not None else 0
                db_urls = set(row["db_urls"] or [])

                try:
                    meta, pdfs = extract_pdfs_from_decision_page(url, http)
                except Exception as exc:  # noqa: BLE001
                    print(f"- {case_id} slug={slug}  ERROR fetching page: {exc}")
                    continue

                web_urls = [p["url"] for p in pdfs]
                web_count = len(web_urls)

                if web_count >= 2 and web_count > db_count:
                    hits += 1
                    missing = list(sorted(set(web_urls) - db_urls))
                    print(f"+ {case_id} slug={slug}")
                    print(f"    page     : {url}")
                    print(f"    db_count : {db_count}")
                    print(f"    web_count: {web_count}")
                    if args.list_missing:
                        for m in missing:
                            print(f"    missing  : {m}")
                # else: quiet for non-hits

    finally:
        http.close()

    print("\nSummary:")
    print(f"  candidates checked: {total}")
    print(f"  cases with MORE on web: {hits}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

