"""HTTP client utilities — etag-aware GET with retry/backoff."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from fund_rank.obs.logging import get_logger

log = get_logger(__name__)


@dataclass
class FetchResult:
    """Result of an HTTP fetch.

    `content=None` means the server returned 304 Not Modified — caller should
    treat the most recent local copy as still authoritative.
    """

    content: bytes | None
    etag: str | None
    last_modified: str | None
    status_code: int


def make_client(*, timeout_seconds: int = 180, user_agent: str = "fund-rank/0.1") -> httpx.Client:
    return httpx.Client(
        timeout=httpx.Timeout(timeout_seconds, connect=30.0),
        follow_redirects=True,
        headers={"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"},
    )


def fetch_with_etag(
    client: httpx.Client,
    url: str,
    *,
    prior_etag: str | None = None,
    prior_last_modified: str | None = None,
    max_retries: int = 5,
    backoff_min: float = 2.0,
    backoff_max: float = 60.0,
) -> FetchResult:
    """GET with conditional If-None-Match / If-Modified-Since.

    Returns FetchResult with content=None on 304.
    Retries on transient network/5xx errors with exponential backoff.
    """
    headers: dict[str, str] = {}
    if prior_etag:
        headers["If-None-Match"] = prior_etag
    if prior_last_modified:
        headers["If-Modified-Since"] = prior_last_modified

    @retry(
        stop=stop_after_attempt(max_retries),
        wait=wait_exponential(multiplier=1.0, min=backoff_min, max=backoff_max),
        retry=retry_if_exception_type((httpx.RequestError, _Transient5xx)),
        reraise=True,
    )
    def _do() -> FetchResult:
        log.debug("http.fetch", url=url, has_prior_etag=bool(prior_etag))
        resp = client.get(url, headers=headers)
        if resp.status_code == 304:
            return FetchResult(
                content=None,
                etag=prior_etag,
                last_modified=prior_last_modified,
                status_code=304,
            )
        if 500 <= resp.status_code < 600:
            raise _Transient5xx(f"{resp.status_code} from {url}")
        if resp.status_code == 404:
            # 404 is a normal "not yet published" signal — not retried.
            return FetchResult(
                content=None,
                etag=None,
                last_modified=None,
                status_code=404,
            )
        resp.raise_for_status()
        return FetchResult(
            content=resp.content,
            etag=resp.headers.get("etag"),
            last_modified=resp.headers.get("last-modified"),
            status_code=resp.status_code,
        )

    return _do()


class _Transient5xx(Exception):
    """Marker for retryable 5xx responses."""


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
