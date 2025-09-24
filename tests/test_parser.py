from scraper.parser import parse_listing_html

SAMPLE_HTML = """
<html>
  <body>
    <article>
      <h2><a href="/decisions/sample-case-123.pdf">Sample case 123</a></h2>
      <time datetime="2024-02-12">12 February 2024</time>
      <p>Case reference: LON 00/123</p>
    </article>
  </body>
</html>
"""


def test_parse_listing_html_extracts_document():
    docs = parse_listing_html(SAMPLE_HTML, "https://example.org/list", "Tribunal")
    assert len(docs) == 1
    doc = docs[0]
    assert doc.document_url == "https://example.org/decisions/sample-case-123.pdf"
    assert doc.case_id in {"LON00/123", "lon00-123", "generated-1"}
