import threading
import http.server
import socketserver
import contextlib
import unittest

from scripts.rescrape_cases import (
    classify_document,
    normalized_pathname,
    extract_pdfs_from_decision_page,
    download_pdf,
)
from scraper.session import build_http_session


class _TestHandler(http.server.BaseHTTPRequestHandler):
    routes = {}

    def do_GET(self):  # noqa: N802 (BaseHTTPRequestHandler requirements)
        status, headers, body = self.routes.get(
            self.path,
            (404, {"Content-Type": "text/plain"}, b"not found"),
        )
        self.send_response(status)
        for key, value in headers.items():
            self.send_header(key, value)
        self.end_headers()
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.wfile.write(body)

    def log_message(self, format, *args):  # noqa: A003 (silence test server logging)
        return


@contextlib.contextmanager
def run_test_server(routes):
    handler_cls = _TestHandler
    handler_cls.routes = routes

    with socketserver.TCPServer(("127.0.0.1", 0), handler_cls) as httpd:
        httpd.allow_reuse_address = True
        host, port = httpd.server_address
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        try:
            yield f"http://{host}:{port}"
        finally:
            httpd.shutdown()
            thread.join()


class RescrapeUtilsTestCase(unittest.TestCase):
    def test_classify_document_by_link_text(self):
        self.assertEqual(classify_document("Statement of Reasons", "", "foo.pdf"), "reasons")
        self.assertEqual(classify_document("Tribunal decision", "", "foo.pdf"), "decision")
        self.assertEqual(classify_document("Other", "", "foo.pdf"), "unknown")

    def test_classify_document_by_filename(self):
        self.assertEqual(classify_document("", "", "/path/to/file_reasons.pdf"), "reasons")
        self.assertEqual(classify_document("", "", "/path/to/decision_final.pdf"), "decision")

    def test_normalized_pathname(self):
        self.assertEqual(normalized_pathname("https://example.com/foo/bar/"), "/foo/bar")
        self.assertEqual(normalized_pathname("https://example.com/foo/bar//"), "/foo/bar")

    def test_extract_and_download_pdf_via_http(self):
        html = """
        <html>
          <body>
            <article>
              <a href="/docs/decision.pdf">Tribunal decision PDF</a>
            </article>
            <article>
              <a href="/docs/reasons.pdf">Statement of Reasons</a>
            </article>
          </body>
        </html>
        """
        pdf_bytes = b"%PDF-1.4 minimal"

        routes = {
            "/case": (200, {"Content-Type": "text/html"}, html),
            "/docs/decision.pdf": (200, {"Content-Type": "application/pdf"}, pdf_bytes),
            "/docs/reasons.pdf": (200, {"Content-Type": "application/pdf"}, pdf_bytes),
        }

        with run_test_server(routes) as base_url:
            session = build_http_session(timeout=5)
            try:
                meta, pdfs = extract_pdfs_from_decision_page(f"{base_url}/case", session)
            finally:
                session.close()

            self.assertEqual(meta["title"], None)
            self.assertEqual(len(pdfs), 2)
            self.assertEqual({p["document_type"] for p in pdfs}, {"decision", "reasons"})

            # download one pdf to verify content preservation
            download_session = build_http_session(timeout=5)
            try:
                buf, length, mime = download_pdf(f"{base_url}/docs/decision.pdf", download_session)
            finally:
                download_session.close()
            self.assertEqual(buf, pdf_bytes)
            self.assertEqual(length, len(pdf_bytes))
            self.assertEqual(mime, "application/pdf")


if __name__ == "__main__":
    unittest.main()
