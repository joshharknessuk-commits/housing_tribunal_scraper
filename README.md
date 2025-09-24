# Housing Tribunal Scraper

Scrapes housing tribunal decision listings, captures PDF URLs and metadata,
optionally downloads the files, and can seed your Postgres/Neon table for
processing with the [Postgres URL PDF Text Extractor](../Pdf_Processing).

## Features
- Configurable listing URL with automatic pagination handling (`?page=` or `{page}` templates).
- Robust HTTP client with retries/backoff and user-agent control.
- BeautifulSoup-based parser that extracts PDF decision links, case IDs, dates, and metadata.
- Optional PDF download pipeline with checksum-based filenames.
- Optional Postgres insertion with unique PDF URLs (safe to re-run).

## Quickstart
1. **Clone & install**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Configure environment**
   ```bash
   cp .env.example .env
   # edit .env (TRIBUNAL_BASE_URL, OUTPUT_DIR, optional NEON_URL)
   ```

3. **Run the scraper**
   ```bash
python -m scraper.cli --pages 3
```
   Flags:
   - `--no-download`: metadata only (skip PDF download)
   - `--persist`: insert metadata into Postgres via `NEON_URL`
   - `--table`: override destination table (`dev.documents` by default)

4. **Typical workflow**
   - Scrape tribunal listings into your Neon table.
   - Switch to the PDF processing repo and run `process_pdfs.py` against the same table.

## Project layout
```
housing_tribunal_scraper/
├── scraper/
│   ├── __init__.py
│   ├── cli.py
│   ├── models.py
│   ├── parser.py
│   ├── pipeline.py
│   └── session.py
├── tests/
│   └── test_parser.py
├── .env.example
├── .gitignore
├── README.md
└── requirements.txt
```

## Tests
```
pytest
```

## Notes
- The parser is heuristic: adjust CSS selectors or regex in `parser.py` if your
  tribunal listings use different markup.
- When persisting to Postgres, rows with the same `pdf_url` are ignored on conflict.

## Re-scrape existing cases
If you already have rows in a `cases` table and want to backfill or classify
missing PDFs, use the re-scrape worker:

```bash
python scripts/rescrape_cases.py
```

Environment variables:

- `DATABASE_URL` *(required)* – Postgres connection string (e.g. Neon).
- `BATCH_SIZE` *(default 200)* – number of cases fetched per batch.
- `DELAY_MS` *(default 200)* – delay between batches in milliseconds.
- `DOCUMENTS_TABLE` *(default `dev.documents`)* – target documents table.
- `CASES_TABLE` *(default `dev.cases`)* – source/target cases table.
- `STORE_PDF_BYTES` *(default 1)* – disable to skip downloading binary content.
- `ENABLE_BLOB_UPLOAD` – set to `1` if you extend `upload_to_blob` with cloud storage.

The script maintains progress in a `cursors` table (row name defaults to
`rescrape_progress`) and mirrors the console output style of the original
TypeScript ingestion job.
