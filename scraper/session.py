from __future__ import annotations

import os
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def build_http_session(
    *,
    timeout: Optional[float] = None,
    retry_total: int = 3,
    retry_backoff: float = 0.5,
    user_agent: Optional[str] = None,
) -> requests.Session:
    """Create a `requests.Session` configured with retry/backoff logic."""

    session = requests.Session()
    retry = Retry(
        total=retry_total,
        connect=retry_total,
        read=retry_total,
        backoff_factor=retry_backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    headers = {}
    headers["User-Agent"] = user_agent or os.getenv("HTTP_USER_AGENT", "Housing-Tribunal-Scraper/1.0")
    session.headers.update(headers)

    # Attach timeout to session for convenience
    session.request = _timeout_wrapper(session.request, timeout or float(os.getenv("REQUEST_TIMEOUT", "30")))
    return session


def _timeout_wrapper(func, timeout: float):
    def wrapped(method, url, **kwargs):
        if "timeout" not in kwargs:
            kwargs["timeout"] = timeout
        return func(method, url, **kwargs)

    return wrapped
