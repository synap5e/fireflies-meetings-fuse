"""Integration tests for raw observation hooks at ingest boundaries."""

from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import httpx
import pytest

import fireflies_meetings.store as store_module
from fireflies_meetings.access_logs import AccessLogsOutcome, access_logs_ok
from fireflies_meetings.api import (
    FatalAPIError,
    FirefliesClient,
    RateLimitedError,
    TranscriptNotFoundError,
    TransientAPIError,
)
from fireflies_meetings.live_stream import CaptionCoalescer, make_broadcast_handler
from fireflies_meetings.models import Meeting, MeetingInfo, TranscriptDetail
from fireflies_meetings.raw import RawEnvelope
from fireflies_meetings.session_auth import SessionAuth
from fireflies_meetings.status_cache import StatusCache
from fireflies_meetings.store import MeetingStore


class _RecordingSink:
    def __init__(self) -> None:
        self.envelopes: list[RawEnvelope] = []

    def write(self, envelope: RawEnvelope) -> None:
        self.envelopes.append(envelope)


def _client(
    handler: httpx.MockTransport,
    sink: _RecordingSink,
    *,
    internal: bool = False,
) -> FirefliesClient:
    auth = SessionAuth(access_token="access-token") if internal else None
    return FirefliesClient(
        "dummy-key",
        session_auth=auth,
        transport=handler,
        raw_sink=sink,
    )


def _meeting(meeting_id: str = "MEET01", *, age_hours: float = 0.0) -> Meeting:
    del age_hours
    return Meeting(
        id=meeting_id,
        title="Planning",
        date_epoch_ms=1774891800000.0,
        duration_mins=0.0,
        meeting_info=MeetingInfo(summary_status=""),
    )


def test_public_client_methods_archive_successful_raw_responses() -> None:
    sink = _RecordingSink()

    def handler(request: httpx.Request) -> httpx.Response:
        payload = cast("dict[str, object]", json.loads(request.content))
        query = str(payload["query"])
        if "query Transcripts" in query:
            return httpx.Response(
                200,
                json={
                    "data": {
                        "transcripts": [
                            {
                                "id": "MEET01",
                                "title": "Planning",
                                "date": 1774891800000,
                            }
                        ]
                    }
                },
            )
        if "query Transcript(" in query:
            return httpx.Response(
                200,
                json={
                    "data": {
                        "transcript": {
                            "id": "MEET01",
                            "title": "Planning",
                            "date": 1774891800000,
                            "sentences": [],
                            "meeting_info": {"summary_status": "processing"},
                        }
                    }
                },
            )
        if "active_meetings" in query:
            return httpx.Response(200, json={"data": {"active_meetings": [{"id": "MEET01"}]}})
        if "user" in query:
            return httpx.Response(200, json={"data": {"user": {"email": "alice@example.com"}}})
        raise AssertionError(query)

    client = _client(httpx.MockTransport(handler), sink)
    assert [meeting.id for meeting in client.list_transcripts()] == ["MEET01"]
    assert client.get_transcript("MEET01").meeting.id == "MEET01"
    assert client.list_active_meeting_ids() == ["MEET01"]
    assert client.get_user_email() == "alice@example.com"

    assert [envelope.endpoint for envelope in sink.envelopes] == [
        "transcripts",
        "transcript",
        "active-meetings",
        "user",
    ]
    assert all(envelope.source == "fireflies-public-graphql" for envelope in sink.envelopes)
    assert all(envelope.outcome == "ok" for envelope in sink.envelopes)
    assert sink.envelopes[0].page_cursor == {"page": 1, "page_size": 50}


def test_public_error_classes_are_archived_before_raise() -> None:
    cases: list[tuple[int, type[Exception], str]] = [
        (429, RateLimitedError, "rate_limited"),
        (401, FatalAPIError, "auth_denied"),
        (500, TransientAPIError, "transient_error"),
    ]
    for status, error_type, outcome in cases:
        sink = _RecordingSink()
        client = _client(
            httpx.MockTransport(lambda _request, code=status: httpx.Response(code, json={"error": "boom"})),
            sink,
        )
        with pytest.raises(error_type):
            client.list_transcripts()
        assert sink.envelopes[-1].outcome == outcome

    malformed_sink = _RecordingSink()
    malformed = _client(
        httpx.MockTransport(lambda _request: httpx.Response(200, content=b"\xffnot-json")),
        malformed_sink,
    )
    with pytest.raises(TransientAPIError):
        malformed.list_transcripts()
    assert malformed_sink.envelopes[-1].body_encoding == "raw"
    assert malformed_sink.envelopes[-1].outcome == "transient_error"

    missing_sink = _RecordingSink()
    missing = _client(
        httpx.MockTransport(
            lambda _request: httpx.Response(
                200,
                json={
                    "data": {"transcript": None},
                    "errors": [{"code": "object_not_found"}],
                },
            )
        ),
        missing_sink,
    )
    with pytest.raises(TranscriptNotFoundError):
        missing.get_transcript("GONE01")
    assert missing_sink.envelopes[-1].outcome == "not_found"


def test_hive_and_internal_methods_archive_their_endpoint_responses() -> None:
    sink = _RecordingSink()
    responses: dict[object, object] = {
        "getChannelsList": {"data": {"getChannelsList": []}},
        "fetchChannelMeetings": {"data": {"getChannelMeetings": {"meetings": [], "total": 0}}},
        "getUserMeetingsForStatus": {
            "data": {"getUserMeetingsForStatus": {"meetings": [], "totalCount": 0}},
        },
        "GetMeetingSummaryAccessLogs": {"data": {"getMeetingSummaryAccessLogs": []}},
        "fetchNotepadMeeting": {
            "data": {
                "meetingNote": {
                    "_id": "MEET01",
                    "parseId": "MEET01",
                    "title": "Planning",
                    "date": "2026-03-31T12:00:00Z",
                    "captions": [],
                }
            }
        },
        "getTranscriptFFAuth": {"data": {"getTranscriptFFAuth": "token"}},
        "getLiveTranscript": {
            "data": {
                "getLiveTranscript": [
                    {
                        "transcript_id": "1",
                        "sentence": "Hello",
                        "time": 1.0,
                        "endTime": 2.0,
                    }
                ]
            }
        },
    }

    def handler(request: httpx.Request) -> httpx.Response:
        payload = cast("dict[str, object]", json.loads(request.content))
        operation = payload.get("operationName")
        if request.url.host == "api.fireflies.ai":
            return httpx.Response(200, json={"data": {"transcript": None}})
        assert operation in responses, f"Unexpected request: {request.url} {operation}"
        return httpx.Response(200, json=responses[operation])

    client = _client(httpx.MockTransport(handler), sink, internal=True)
    assert client.list_channels() == []
    assert client.list_channel_memberships(["CHANNEL01"]) == {}
    assert client.list_recent_status_meetings() == []
    assert client.get_access_logs("MEET01") == access_logs_ok([])
    assert client.get_internal_realtime_token("MEET01") == "token"
    assert [sentence.text for sentence in client.get_transcript("MEET01").sentences] == ["Hello"]

    observed = {(envelope.source, envelope.endpoint) for envelope in sink.envelopes}
    assert {
        ("fireflies-hive", "channels"),
        ("fireflies-hive", "channel-memberships"),
        ("fireflies-hive", "status-supplement"),
        ("fireflies-hive", "access-logs"),
        ("fireflies-hive", "realtime-token"),
        ("fireflies-hive", "transcript"),
        ("fireflies-hive", "live-transcript"),
    } <= observed


def test_socketio_archives_only_normalized_captions() -> None:
    sink = _RecordingSink()
    coalescer = CaptionCoalescer()
    handler = make_broadcast_handler(coalescer, raw_sink=sink, meeting_id="MEET01")

    handler({"transcript_id": "12", "sentence": "Accepted", "time": 1.0, "endTime": 2.0})
    handler({"transcript_id": "13", "sentence": "", "time": 2.0, "endTime": 3.0})

    assert len(sink.envelopes) == 1
    assert sink.envelopes[0].source == "socketio-transcription"
    assert sink.envelopes[0].body == {
        "transcript_id": "12",
        "sentence": "Accepted",
        "time": 1.0,
        "endTime": 2.0,
    }
    assert [transcript_id for transcript_id, _sentence in coalescer.drain_batch()] == ["12"]


class _StoreClient:
    def __init__(self, meeting: Meeting, *, not_found: bool = False) -> None:
        self.meeting = meeting
        self.not_found = not_found

    def list_transcripts(self, *, max_pages: int | None = None) -> list[Meeting]:
        del max_pages
        return [self.meeting]

    def list_recent_status_meetings(self, *, limit: int = 100) -> list[Meeting]:
        del limit
        return []

    def get_transcript(self, meeting_id: str) -> TranscriptDetail:
        if self.not_found:
            raise TranscriptNotFoundError(meeting_id)
        return TranscriptDetail(meeting=self.meeting)

    def get_access_logs(self, meeting_id: str) -> AccessLogsOutcome:
        del meeting_id
        return access_logs_ok([])


def test_store_synthetic_watch_active_id_and_not_found_hooks(tmp_path: Path) -> None:
    sink = _RecordingSink()
    meeting = _meeting()
    client = _StoreClient(meeting)
    store = MeetingStore(
        cast(FirefliesClient, client),
        status_cache=StatusCache(cache_dir=tmp_path / "cache"),
        raw_sink=sink,
    )
    store.refresh_list_if_needed()
    assert store.watch_meeting(meeting.id)
    store.sync_active_meeting_ids([meeting.id])
    client.not_found = True
    store.backfill_one(meeting.id)

    sources = [envelope.source for envelope in sink.envelopes]
    assert "synthetic-watch" in sources
    assert "synthetic-active-id" in sources
    assert "synthetic-not-found" in sources


def test_store_empty_placeholder_age_out_hook(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink = _RecordingSink()
    meeting = _meeting()
    client = _StoreClient(meeting)
    store = MeetingStore(
        cast(FirefliesClient, client),
        status_cache=StatusCache(cache_dir=tmp_path / "cache"),
        raw_sink=sink,
    )
    store.refresh_list_if_needed()
    monkeypatch.setattr(store_module.time, "time", lambda: meeting.date_epoch_ms / 1000.0 + 13 * 3600)
    store.backfill_one(meeting.id)

    [age_out] = [envelope for envelope in sink.envelopes if envelope.source == "synthetic-age-out"]
    assert age_out.endpoint == "empty-placeholder-age-out"
    assert age_out.outcome == "synthetic"
