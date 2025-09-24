from __future__ import annotations

import argparse
import os
from pathlib import Path

from dotenv import load_dotenv

from .pipeline import HousingTribunalScraper, ScrapeResult, connect_engine


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape housing tribunal decision PDFs")
    parser.add_argument("--base-url", default=os.getenv("TRIBUNAL_BASE_URL"), help="Listing page to crawl")
    parser.add_argument("--pages", type=int, default=int(os.getenv("MAX_PAGES", "5")), help="Number of listing pages to crawl")
    parser.add_argument("--output-dir", default=os.getenv("OUTPUT_DIR", "outputs"), help="Where to store downloaded PDFs")
    parser.add_argument("--no-download", action="store_true", help="Skip downloading PDFs (metadata only)")
    parser.add_argument("--persist", action="store_true", help="Insert metadata into Postgres")
    parser.add_argument("--table", default=os.getenv("SCRAPER_TABLE", "dev.documents"), help="Target database table")
    parser.add_argument("--tribunal", default=os.getenv("TRIBUNAL_NAME", "First-tier Tribunal (Housing)"), help="Human readable tribunal name")
    parser.add_argument("--timeout", type=float, default=float(os.getenv("REQUEST_TIMEOUT", "30")))
    parser.add_argument("--retry-total", type=int, default=int(os.getenv("REQUEST_RETRY_TOTAL", "3")))
    parser.add_argument("--retry-backoff", type=float, default=float(os.getenv("REQUEST_RETRY_BACKOFF", "0.5")))
    return parser.parse_args()


def run() -> ScrapeResult:
    load_dotenv()
    args = parse_args()

    if not args.base_url:
        raise SystemExit("Base URL not provided. Use --base-url or TRIBUNAL_BASE_URL env var.")

    engine = None
    if args.persist:
        neon_url = os.getenv("NEON_URL")
        if not neon_url:
            raise SystemExit("NEON_URL env var required to persist to database.")
        engine = connect_engine(neon_url)

    scraper = HousingTribunalScraper(
        base_url=args.base_url,
        output_dir=Path(args.output_dir),
        http_timeout=args.timeout,
        retry_total=args.retry_total,
        retry_backoff=args.retry_backoff,
        engine=engine,
        table=args.table,
    )

    result = scraper.scrape(
        max_pages=args.pages,
        tribunal_name=args.tribunal,
        download_pdfs=not args.no_download,
        persist_to_db=args.persist,
    )

    print(
        f"Scraped {len(result.documents)} documents | "
        f"Downloaded {len(result.downloaded)} files | "
        f"Inserted {result.db_rows_inserted} rows"
    )
    return result


def main() -> None:
    run()


if __name__ == "__main__":
    main()
