from __future__ import annotations

import re
from datetime import datetime
from typing import Iterable, List
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from dateutil import parser as dateparser

from .models import DocumentRecord

CASE_ID_REGEX = re.compile(r"([A-Z]{1,4}\s*\d{2,4}[/\-]\d{2,4})")


def parse_listing_html(html: str, listing_url: str, tribunal_name: str) -> List[DocumentRecord]:
    """Parse a tribunal listing page and extract PDF decision links.

    The parser aims to be resilient to markup differences by scanning for the first
    link with a `.pdf` suffix inside each `article` node. Additional metadata is
    scraped opportunistically from definition lists or paragraph text.
    """

    soup = BeautifulSoup(html, "html.parser")
    documents: list[DocumentRecord] = []

    for article in soup.find_all("article"):
        link = _first_pdf_link(article, listing_url)
        if not link:
            continue

        title = link.get_text(strip=True) or "Housing tribunal decision"
        case_id = _extract_case_id(article, title) or f"generated-{len(documents)+1}"
        decision_date = _extract_date(article)

        metadata = {}
        summary_block = article.find(class_=re.compile("summary|metadata|details"))
        if summary_block:
            metadata.update(_extract_key_values(summary_block))

        documents.append(
            DocumentRecord(
                case_id=case_id,
                title=title,
                document_url=link,
                tribunal=tribunal_name,
                decision_date=decision_date,
                listing_url=listing_url,
                metadata=metadata,
            )
        )

    return documents


def _first_pdf_link(article, listing_url: str) -> str | None:
    link = article.find("a", href=re.compile(r"\.pdf(?=$|[?#])", re.I))
    if not link or not link.get("href"):
        return None
    return urljoin(listing_url, link["href"])


def _extract_case_id(article, fallback: str) -> str | None:
    text = article.get_text(" ", strip=True)
    match = CASE_ID_REGEX.search(text)
    if match:
        return match.group(1).replace(" ", "")
    # fallback: generate slug from fallback text
    slug = re.sub(r"[^A-Za-z0-9]+", "-", fallback).strip("-").lower()
    return slug or None


def _extract_date(article) -> datetime | None:
    time_tag = article.find("time")
    if time_tag and time_tag.get("datetime"):
        try:
            return dateparser.parse(time_tag["datetime"]).replace(tzinfo=None)
        except (ValueError, TypeError):
            pass
    if time_tag and time_tag.text:
        try:
            return dateparser.parse(time_tag.text, dayfirst=True, fuzzy=True).replace(tzinfo=None)
        except (ValueError, TypeError):
            pass

    # look for date-like text in paragraph snippets
    for candidate in article.find_all(text=re.compile(r"\b\d{1,2}\s+\w+\s+\d{4}\b")):
        try:
            return dateparser.parse(candidate, dayfirst=True, fuzzy=True).replace(tzinfo=None)
        except (ValueError, TypeError):
            continue
    return None


def _extract_key_values(node) -> dict[str, str]:
    metadata: dict[str, str] = {}

    definition_lists = node.find_all("dl")
    for dl in definition_lists:
        terms = dl.find_all("dt")
        values = dl.find_all("dd")
        for dt, dd in zip(terms, values):
            key = dt.get_text(strip=True).lower().replace(" ", "_")
            value = dd.get_text(" ", strip=True)
            if key:
                metadata[key] = value

    return metadata
