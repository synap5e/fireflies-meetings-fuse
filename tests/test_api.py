"""Tests for the FirefliesClient HTTP/error path.

Mocks the underlying httpx transport so we exercise _post and the public
list/detail/user methods without ever touching the network. The fixes
under test:

- Malformed x-ratelimit-reset-api header → logged + ignored, request still succeeds
- Non-JSON response body → TransientAPIError (instead of an unhandled JSONDecodeError)
- 429 → RateLimitedError with retry_after parsed from the header
- 401/403 → FatalAPIError
- GraphQL errors-without-data → TransientAPIError
- One bad record in a list page → skipped, valid records returned
- get_user_email must not crash on TransientAPIError (startup safety)
"""

from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest

from fireflies_meetings.api import (
    FatalAPIError,
    FirefliesClient,
    RateLimitedError,
    TransientAPIError,
)


def _make_client(handler: Callable[[httpx.Request], httpx.Response]) -> FirefliesClient:
    return FirefliesClient("dummy-key", transport=httpx.MockTransport(handler))


def test_malformed_rate_limit_header_does_not_break_request() -> None:
    def handler(_req: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"x-ratelimit-reset-api": "not-a-number"},
            json={"data": {"transcripts": []}},
        )

    client = _make_client(handler)
    assert client.list_transcripts() == []


def test_html_body_raises_transient_error() -> None:
    def handler(_req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"<html>cloudflare 502</html>")

    client = _make_client(handler)
    with pytest.raises(TransientAPIError):
        client.list_transcripts()


def test_429_raises_rate_limited_with_retry_after() -> None:
    def handler(_req: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"x-ratelimit-reset-api": "30"})

    client = _make_client(handler)
    with pytest.raises(RateLimitedError) as exc_info:
        client.list_transcripts()
    assert exc_info.value.retry_after == 30.0


def test_401_raises_fatal_error() -> None:
    def handler(_req: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"error": "unauthorized"})

    client = _make_client(handler)
    with pytest.raises(FatalAPIError):
        client.list_transcripts()


def test_403_raises_fatal_error() -> None:
    def handler(_req: httpx.Request) -> httpx.Response:
        return httpx.Response(403, json={"error": "forbidden"})

    client = _make_client(handler)
    with pytest.raises(FatalAPIError):
        client.list_transcripts()


def test_graphql_errors_without_data_raises_transient() -> None:
    def handler(_req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"errors": [{"message": "boom"}]})

    client = _make_client(handler)
    with pytest.raises(TransientAPIError):
        client.list_transcripts()


def test_list_transcripts_skips_invalid_records() -> None:
    """One bad record (path-traversal id) plus two good ones returns the two good."""

    def handler(_req: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "data": {
                    "transcripts": [
                        {"id": "MEET01", "title": "good", "date": 1774891800000},
                        {"id": "../etc/passwd", "title": "bad", "date": 1774891800000},
                        {"id": "MEET02", "title": "also good", "date": 1774891800000},
                    ],
                },
            },
        )

    client = _make_client(handler)
    meetings = client.list_transcripts(max_pages=1)
    ids = [m.id for m in meetings]
    assert ids == ["MEET01", "MEET02"]


def test_get_user_email_returns_none_on_transient_error() -> None:
    """A GraphQL error block during the startup user query must NOT crash mount."""

    def handler(_req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"errors": [{"message": "internal"}]})

    client = _make_client(handler)
    assert client.get_user_email() is None


def test_get_user_email_returns_email_on_success() -> None:
    def handler(_req: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, json={"data": {"user": {"email": "alice@example.com"}}}
        )

    client = _make_client(handler)
    assert client.get_user_email() == "alice@example.com"


def test_get_user_email_returns_none_on_rate_limit() -> None:
    def handler(_req: httpx.Request) -> httpx.Response:
        return httpx.Response(429)

    client = _make_client(handler)
    assert client.get_user_email() is None


def test_get_user_email_returns_none_on_fatal() -> None:
    def handler(_req: httpx.Request) -> httpx.Response:
        return httpx.Response(401)

    client = _make_client(handler)
    assert client.get_user_email() is None
