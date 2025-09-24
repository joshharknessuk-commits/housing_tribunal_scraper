from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Iterator, List, Optional
from urllib.parse import urlparse
from uuid import uuid4

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .models import DocumentRecord
from .parser import parse_listing_html
from .session import build_http_session


@dataclass
class ScrapeResult:
    documents: list[DocumentRecord] = field(default_factory=list)
    downloaded: list[Path] = field(default_factory=list)
    db_rows_inserted: int = 0


class HousingTribunalScraper:
    """Scrape housing tribunal decision listings and PDFs."""

    def __init__(
        self,
        *,
        base_url: str,
        output_dir: Path | None = None,
        http_timeout: Optional[float] = None,
        retry_total: int = 3,
        retry_backoff: float = 0.5,
        user_agent: Optional[str] = None,
        engine: Engine | None = None,
        table: str = "dev.documents",
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = build_http_session(
            timeout=http_timeout,
            retry_total=retry_total,
            retry_backoff=retry_backoff,
            user_agent=user_agent,
        )
        self.output_dir = Path(output_dir or "outputs").resolve()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.engine = engine
        self.table = table

    def scrape(
        self,
        *,
        max_pages: Optional[int] = None,
        stop_on_empty: bool = True,
        tribunal_name: str = "First-tier Tribunal (Housing)",
        download_pdfs: bool = True,
        persist_to_db: bool = False,
    ) -> ScrapeResult:
        """Scrape listing pages and optionally download PDFs / insert metadata."""

        result = ScrapeResult()

        for page, documents in enumerate(self.iter_documents(max_pages=max_pages, tribunal_name=tribunal_name), start=1):
            if not documents and stop_on_empty:
                break

            result.documents.extend(documents)

            if download_pdfs:
                for doc in documents:
                    try:
                        path = self.download_pdf(doc)
                        result.downloaded.append(path)
                    except Exception as exc:  # noqa: BLE001
                        print(f"⚠️  Failed to download {doc.document_url}: {exc}")

            if persist_to_db and self.engine:
                inserted = self._persist_documents(documents)
                result.db_rows_inserted += inserted

        return result

    def iter_documents(
        self,
        *,
        max_pages: Optional[int],
        tribunal_name: str,
    ) -> Iterator[List[DocumentRecord]]:
        page = 1
        while True:
            if max_pages is not None and page > max_pages:
                break

            url = self._page_url(page)
            response = self.session.get(url)
            response.raise_for_status()

            documents = parse_listing_html(response.text, url, tribunal_name)
            yield documents

            if not documents:
                break
            page += 1

    def download_pdf(self, document: DocumentRecord) -> Path:
        response = self.session.get(document.document_url, stream=True)
        response.raise_for_status()

        filename = self._filename_for(document)
        target_path = self.output_dir / filename
        with target_path.open("wb") as fh:
            for chunk in response.iter_content(chunk_size=64 * 1024):
                if chunk:
                    fh.write(chunk)
        return target_path

    def _persist_documents(self, documents: Iterable[DocumentRecord]) -> int:
        if not documents:
            return 0
        if not self.engine:
            raise RuntimeError("SQLAlchemy engine not configured")

        insert_sql = text(
            f"""
            INSERT INTO {self.table} (
                id,
                case_id,
                pdf_url,
                sha256,
                processed,
                process_attempts,
                raw_text,
                tribunal,
                metadata,
                downloaded_at,
                filename,
                last_error
            )
            VALUES (
                :id,
                :case_id,
                :pdf_url,
                :sha256,
                FALSE,
                0,
                NULL,
                :tribunal,
                CAST(:metadata AS jsonb),
                NULL,
                :filename,
                NULL
            )
            ON CONFLICT (pdf_url) DO NOTHING
            """
        )

        as_list = list(documents)
        payload = [
            {
                "id": str(uuid4()),
                "case_id": doc.case_id,
                "pdf_url": doc.document_url,
                "sha256": _dummy_sha_placeholder(doc.document_url),
                "tribunal": doc.tribunal,
                "metadata": json_dumps({
                    "case_id": doc.case_id,
                    "title": doc.title,
                    "decision_date": doc.decision_date.isoformat() if doc.decision_date else None,
                    "listing_url": doc.listing_url,
                    "metadata": doc.metadata,
                }),
                "filename": self._filename_for(doc),
            }
            for doc in as_list
        ]
        with self.engine.begin() as conn:
            result = conn.execute(insert_sql, payload)
        return result.rowcount or 0

    def _page_url(self, page: int) -> str:
        if "{page}" in self.base_url:
            return self.base_url.format(page=page)
        if page == 1:
            return self.base_url
        separator = "&" if "?" in self.base_url else "?"
        return f"{self.base_url}{separator}page={page}"

    def _filename_for(self, document: DocumentRecord) -> str:
        parsed = urlparse(document.document_url)
        name = os.path.basename(parsed.path)
        if name:
            return name
        return f"{document.case_id}.pdf"


def connect_engine(neon_url: str) -> Engine:
    engine_url = neon_url
    return create_engine(engine_url, pool_pre_ping=True)


def json_dumps(payload: dict[str, object]) -> str:
    import json

    return json.dumps(payload, default=str)


def _dummy_sha_placeholder(source: str) -> str:
    return hashlib.sha256(source.encode("utf-8")).hexdigest()
