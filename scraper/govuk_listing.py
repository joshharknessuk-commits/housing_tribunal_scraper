from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from dateutil import parser as dateparser


@dataclass(slots=True)
class ListingEntry:
    """Single GOV.UK housing tribunal listing entry."""

    title: str
    url: str
    slug: str
    decided_at: Optional[datetime]
    subcategory: Optional[str]


def parse_listing_html(html: str, listing_url: str, *, default_domain: str = "https://www.gov.uk") -> list[ListingEntry]:
    """Parse finder HTML and return structured listing entries.

    The GOV.UK finder returns the search results markup either inline or via
    the `search_results` field on the `.json` endpoint. We scan the `<li>`
    nodes that contain decision titles, pull out the canonical URL, the
    decision date (if exposed), and any tribunal subcategory text.
    """

    soup = BeautifulSoup(html, "html.parser")
    entries: list[ListingEntry] = []

    for item in soup.select("li.gem-c-document-list__item"):
        anchor = item.find("a", href=True)
        if not anchor:
            continue
        href = anchor["href"].strip()
        absolute = urljoin(default_domain, href)
        slug = _normalise_path(absolute)

        decided_at = _extract_decided_at(item)
        subcategory = _extract_subcategory(item)
        title = anchor.get_text(" ", strip=True)

        entries.append(ListingEntry(title=title, url=absolute, slug=slug, decided_at=decided_at, subcategory=subcategory))

    return entries


def _normalise_path(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path
    return re.sub(r"/+$", "", path) or "/"


def _extract_decided_at(item) -> Optional[datetime]:
    time_tag = item.find("time")
    if not time_tag:
        return None
    if time_tag.has_attr("datetime") and time_tag["datetime"]:
        raw = time_tag["datetime"].strip()
    else:
        raw = time_tag.get_text(" ", strip=True)
    if not raw:
        return None
    try:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
            return datetime.fromisoformat(raw)
    except ValueError:
        pass
    try:
        value = dateparser.parse(raw, dayfirst=True, fuzzy=True)
        return value.replace(tzinfo=None)
    except (ValueError, TypeError):
        return None


def _extract_subcategory(item) -> Optional[str]:
    for meta_item in item.select("ul.gem-c-document-list__item-metadata li"):
        text = meta_item.get_text(" ", strip=True)
        if not text:
            continue
        lowered = text.lower()
        if lowered.startswith("tribunal decision sub category"):
            _, _, rest = text.partition(":")
            return rest.strip() or None
    return None


__all__ = ["ListingEntry", "parse_listing_html"]
