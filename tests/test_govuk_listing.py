from __future__ import annotations

from datetime import datetime

from scraper.govuk_listing import ListingEntry, parse_listing_html


SAMPLE_HTML = """
<div class="finder-results js-finder-results">
  <ul class="gem-c-document-list">
    <li class="gem-c-document-list__item">
      <div class="gem-c-document-list__item-title">
        <a class="govuk-link" href="/residential-property-tribunal-decisions/example-case-1">
          Example Case 1
        </a>
      </div>
      <ul class="gem-c-document-list__item-metadata">
        <li class="gem-c-document-list__attribute">
          Tribunal decision sub category: Leasehold disputes (management) - Service charges
        </li>
        <li class="gem-c-document-list__attribute">
          Decided: <time datetime="2025-08-12">12 August 2025</time>
        </li>
      </ul>
    </li>
    <li class="gem-c-document-list__item">
      <div class="gem-c-document-list__item-title">
        <a class="govuk-link" href="/residential-property-tribunal-decisions/example-case-2">
          Example Case 2
        </a>
      </div>
      <ul class="gem-c-document-list__item-metadata">
        <li class="gem-c-document-list__attribute">
          Tribunal decision sub category: Housing Act 2004 and Housing and Planning Act 2016 - Rent repayment orders
        </li>
        <li class="gem-c-document-list__attribute">
          Decided: <time>7 July 2024</time>
        </li>
      </ul>
    </li>
  </ul>
</div>
"""


def test_parse_listing_html_extracts_entries():
    entries = parse_listing_html(SAMPLE_HTML, "https://www.gov.uk/residential-property-tribunal-decisions")

    assert len(entries) == 2

    first = entries[0]
    assert first.slug == "/residential-property-tribunal-decisions/example-case-1"
    assert first.url == "https://www.gov.uk/residential-property-tribunal-decisions/example-case-1"
    assert first.subcategory == "Leasehold disputes (management) - Service charges"
    assert first.decided_at == datetime(2025, 8, 12)

    second = entries[1]
    assert second.decided_at == datetime(2024, 7, 7)
    assert second.subcategory.startswith("Housing Act 2004")
