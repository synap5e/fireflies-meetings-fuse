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

import json
from collections.abc import Callable

import httpx
import pytest

from fireflies_meetings.api import (
    FatalAPIError,
    FirefliesClient,
    RateLimitedError,
    TransientAPIError,
)
from fireflies_meetings.session_auth import SessionAuth


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


def test_5xx_raises_transient_error() -> None:
    """504 (and other 5xx) from the upstream gateway should map to TransientAPIError
    so the caller engages backoff rather than letting raw httpx.HTTPStatusError leak."""

    for status in (500, 502, 503, 504):
        def handler(_req: httpx.Request, code: int = status) -> httpx.Response:
            return httpx.Response(code)

        client = _make_client(handler)
        with pytest.raises(TransientAPIError):
            client.list_transcripts()


def test_graphql_errors_without_data_raises_transient() -> None:
    def handler(_req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"errors": [{"message": "boom"}]})

    client = _make_client(handler)
    with pytest.raises(TransientAPIError):
        client.list_transcripts()


def test_get_transcript_marks_partial_sentences_error() -> None:
    """A partial error on transcript.sentences must not look like a true empty transcript."""

    def handler(_req: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "data": {
                    "transcript": {
                        "id": "MEET01",
                        "title": "Live Standup",
                        "date": 1774891800000,
                        "is_live": True,
                        "sentences": None,
                        "speakers": [],
                    },
                },
                "errors": [
                    {
                        "message": "Something unexpected happened. Please try again",
                        "path": ["transcript", "sentences"],
                        "code": "INTERNAL_SERVER_ERROR",
                    },
                ],
            },
        )

    client = _make_client(handler)
    detail = client.get_transcript("MEET01")

    assert detail.sentences == []
    assert "transcript.sentences" in detail.transcript_error
    assert "INTERNAL_SERVER_ERROR" in detail.transcript_error


def test_get_transcript_falls_back_to_internal_meeting_captions() -> None:
    """Live caption fallback should use the internal meetingNote.captions query."""

    def handler(req: httpx.Request) -> httpx.Response:
        if req.url == httpx.URL("https://api.fireflies.ai/graphql"):
            return httpx.Response(
                200,
                json={
                    "data": {
                        "transcript": {
                            "id": "MEET01",
                            "title": "Live Standup",
                            "date": 1774891800000,
                            "is_live": True,
                            "organizer_email": "alice@example.com",
                            "participants": ["alice@example.com", "bob@example.com"],
                            "transcript_url": "https://app.fireflies.ai/view/MEET01",
                            "meeting_info": {
                                "fred_joined": True,
                                "silent_meeting": False,
                                "summary_status": "",
                            },
                            "sentences": None,
                            "speakers": [],
                        },
                    },
                    "errors": [
                        {
                            "message": "Something unexpected happened. Please try again",
                            "path": ["transcript", "sentences"],
                            "code": "INTERNAL_SERVER_ERROR",
                        },
                    ],
                },
            )

        if req.url == httpx.URL("https://app.fireflies.ai/api/v4/graphql"):
            payload = json.loads(req.content.decode())
            assert payload["operationName"] == "fetchNotepadMeeting"
            assert req.headers["origin"] == "https://app.fireflies.ai"
            assert req.headers["referer"] == "https://app.fireflies.ai/view/MEET01"
            return httpx.Response(
                200,
                json={
                    "data": {
                        "meetingNote": {
                            "_id": "MEET01",
                            "parseId": "MEET01",
                            "title": "Live Standup",
                            "date": "2026-03-31T12:00:00.000Z",
                            "creator_email": "alice@example.com",
                            "allEmails": "alice@example.com bob@example.com",
                            "captions": [
                                {
                                    "index": 0,
                                    "sentence": "Hello everyone.",
                                    "speaker_id": "speaker-1",
                                    "time": 5.0,
                                    "endTime": 7.0,
                                },
                            ],
                            "speakerMeta": {
                                "speaker-1": {"name": "Alice"},
                            },
                        },
                    },
                },
            )

        raise AssertionError(f"Unexpected request URL: {req.url}")

    client = FirefliesClient(
        "dummy-key",
        session_auth=SessionAuth(access_token="Bearer access-token", refresh_token="refresh-token"),
        transport=httpx.MockTransport(handler),
    )
    detail = client.get_transcript("MEET01")

    assert detail.transcript_error == ""
    assert [sentence.text for sentence in detail.sentences] == ["Hello everyone."]
    assert detail.sentences[0].speaker_name == "Alice"
    assert detail.meeting.organizer_email == "alice@example.com"
    assert detail.meeting.transcript_url == "https://app.fireflies.ai/view/MEET01"


def test_internal_fallback_sends_refresh_headers_and_cookies() -> None:
    """Internal fallback requests should include the full browser-session shape."""

    def handler(req: httpx.Request) -> httpx.Response:
        if req.url == httpx.URL("https://api.fireflies.ai/graphql"):
            return httpx.Response(
                200,
                json={
                    "data": {
                        "transcript": {
                            "id": "MEET01",
                            "title": "Live Standup",
                            "date": 1774891800000,
                            "is_live": True,
                            "sentences": None,
                            "speakers": [],
                        },
                    },
                    "errors": [
                        {
                            "message": "Something unexpected happened. Please try again",
                            "path": ["transcript", "sentences"],
                            "code": "INTERNAL_SERVER_ERROR",
                        },
                    ],
                },
            )

        if req.url == httpx.URL("https://app.fireflies.ai/api/v4/graphql"):
            assert req.headers["authorization"] == "access-token"
            assert req.headers["x-refresh-token"] == "refresh-token"
            assert req.headers["x-auth-provider"] == "gauth"
            cookie = req.headers["cookie"]
            assert "authorization=Bearer%20access-token" in cookie
            assert "x-cache=refresh-token" in cookie
            return httpx.Response(
                200,
                json={
                    "data": {
                        "meetingNote": {
                            "_id": "MEET01",
                            "parseId": "MEET01",
                            "title": "Live Standup",
                            "date": "2026-03-31T12:00:00.000Z",
                            "captions": [
                                {
                                    "index": 0,
                                    "sentence": "Hello everyone.",
                                    "speaker_id": "speaker-1",
                                    "time": 5.0,
                                    "endTime": 7.0,
                                },
                            ],
                            "speakerMeta": {
                                "speaker-1": {"name": "Alice"},
                            },
                        },
                    },
                },
            )

        raise AssertionError(f"Unexpected request URL: {req.url}")

    client = FirefliesClient(
        "dummy-key",
        session_auth=SessionAuth(access_token="Bearer access-token", refresh_token="refresh-token"),
        transport=httpx.MockTransport(handler),
    )
    detail = client.get_transcript("MEET01")

    assert [sentence.text for sentence in detail.sentences] == ["Hello everyone."]


def test_get_transcript_falls_back_to_internal_live_transcript_polling() -> None:
    """When meetingNote.captions is empty, poll the internal live transcript query."""

    def handler(req: httpx.Request) -> httpx.Response:
        if req.url == httpx.URL("https://api.fireflies.ai/graphql"):
            return httpx.Response(
                200,
                json={
                    "data": {
                        "transcript": {
                            "id": "MEET01",
                            "title": "Live Standup",
                            "date": 1774891800000,
                            "is_live": True,
                            "organizer_email": "alice@example.com",
                            "participants": ["alice@example.com", "bob@example.com"],
                            "transcript_url": "https://app.fireflies.ai/view/MEET01",
                            "meeting_info": {
                                "fred_joined": True,
                                "silent_meeting": False,
                                "summary_status": "",
                            },
                            "sentences": None,
                            "speakers": [],
                        },
                    },
                    "errors": [
                        {
                            "message": "Something unexpected happened. Please try again",
                            "path": ["transcript", "sentences"],
                            "code": "INTERNAL_SERVER_ERROR",
                        },
                    ],
                },
            )

        if req.url == httpx.URL("https://app.fireflies.ai/api/v4/graphql"):
            payload = json.loads(req.content.decode())
            operation = payload["operationName"]
            assert req.headers["origin"] == "https://app.fireflies.ai"
            assert req.headers["referer"] == "https://app.fireflies.ai/view/MEET01"

            if operation == "fetchNotepadMeeting":
                return httpx.Response(
                    200,
                    json={
                        "data": {
                            "meetingNote": {
                                "_id": "MEET01",
                                "parseId": "MEET01",
                                "title": "Live Standup",
                                "date": "2026-03-31T12:00:00.000Z",
                                "captions": [],
                            },
                        },
                    },
                )

            if operation == "getTranscriptFFAuth":
                assert payload["variables"] == {"meetingId": "MEET01"}
                return httpx.Response(200, json={"data": {"getTranscriptFFAuth": "realtime-token"}})

            if operation == "getLiveTranscript":
                assert payload["variables"] == {
                    "meetingId": "MEET01",
                    "realtimeToken": "realtime-token",
                }
                return httpx.Response(
                    200,
                    json={
                        "data": {
                            "getLiveTranscript": [
                                {
                                    "sentence": "Hello everyone.",
                                    "speaker_name": "Alice",
                                    "speaker_id": -1,
                                    "transcript_id": "65110",
                                    "time": 5.0,
                                    "endTime": 7.0,
                                },
                                {
                                    "sentence": "Status update.",
                                    "speaker_name": "Bob",
                                    "speaker_id": -2,
                                    "transcript_id": "65111",
                                    "time": 8.5,
                                    "endTime": 10.0,
                                },
                            ],
                        },
                    },
                )

        raise AssertionError(f"Unexpected request URL: {req.url}")

    client = FirefliesClient(
        "dummy-key",
        session_auth=SessionAuth(access_token="Bearer access-token", refresh_token="refresh-token"),
        transport=httpx.MockTransport(handler),
    )
    detail = client.get_transcript("MEET01")

    assert detail.transcript_error == ""
    assert [sentence.text for sentence in detail.sentences] == ["Hello everyone.", "Status update."]
    assert [sentence.index for sentence in detail.sentences] == [65110, 65111]
    assert detail.meeting.organizer_email == "alice@example.com"
    assert detail.meeting.transcript_url == "https://app.fireflies.ai/view/MEET01"


def test_get_transcript_preserves_partial_error_when_live_polling_has_no_token() -> None:
    """If internal live polling can't acquire a realtime token, keep the public partial-error state."""

    def handler(req: httpx.Request) -> httpx.Response:
        if req.url == httpx.URL("https://api.fireflies.ai/graphql"):
            return httpx.Response(
                200,
                json={
                    "data": {
                        "transcript": {
                            "id": "MEET01",
                            "title": "Live Standup",
                            "date": 1774891800000,
                            "is_live": True,
                            "sentences": None,
                            "speakers": [],
                        },
                    },
                    "errors": [
                        {
                            "message": "Something unexpected happened. Please try again",
                            "path": ["transcript", "sentences"],
                            "code": "INTERNAL_SERVER_ERROR",
                        },
                    ],
                },
            )

        if req.url == httpx.URL("https://app.fireflies.ai/api/v4/graphql"):
            payload = json.loads(req.content.decode())
            operation = payload["operationName"]
            if operation == "fetchNotepadMeeting":
                return httpx.Response(
                    200,
                    json={
                        "data": {
                            "meetingNote": {
                                "_id": "MEET01",
                                "parseId": "MEET01",
                                "title": "Live Standup",
                                "date": "2026-03-31T12:00:00.000Z",
                                "captions": [],
                            },
                        },
                    },
                )
            if operation == "getTranscriptFFAuth":
                return httpx.Response(200, json={"data": {"getTranscriptFFAuth": ""}})

        raise AssertionError(f"Unexpected request URL: {req.url}")

    client = FirefliesClient(
        "dummy-key",
        session_auth=SessionAuth(access_token="Bearer access-token", refresh_token="refresh-token"),
        transport=httpx.MockTransport(handler),
    )
    detail = client.get_transcript("MEET01")

    assert detail.sentences == []
    assert "transcript.sentences" in detail.transcript_error
    assert "INTERNAL_SERVER_ERROR" in detail.transcript_error


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


def test_list_active_meeting_ids_returns_ids() -> None:
    def handler(_req: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "data": {
                    "active_meetings": [
                        {"id": "MEET01"},
                        {"id": "MEET02"},
                    ],
                },
            },
        )

    client = _make_client(handler)
    assert client.list_active_meeting_ids() == ["MEET01", "MEET02"]


def test_list_active_meeting_ids_skips_malformed_records() -> None:
    def handler(_req: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "data": {
                    "active_meetings": [
                        {"id": "MEET01"},
                        {"name": "missing-id"},
                        "bad",
                        {"id": "MEET02"},
                    ],
                },
            },
        )

    client = _make_client(handler)
    assert client.list_active_meeting_ids() == ["MEET01", "MEET02"]
