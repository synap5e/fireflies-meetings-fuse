"""Fireflies.ai GraphQL API client."""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime

import httpx
from pydantic import ValidationError

from .models import Meeting, TranscriptDetail

log = logging.getLogger(__name__)

_ENDPOINT = "https://api.fireflies.ai/graphql"
_HIVE_ENDPOINT = "https://app.fireflies.ai/api/v4/hive"
_PAGE_SIZE = 50

# Recursive JSON type for untyped API responses
type JsonValue = str | int | float | bool | None | dict[str, JsonValue] | list[JsonValue]
type JsonObject = dict[str, JsonValue]

_LIST_QUERY = """
query Transcripts($limit: Int, $skip: Int) {
  transcripts(limit: $limit, skip: $skip) {
    id
    title
    date
    duration
    is_live
    organizer_email
    participants
    transcript_url
    meeting_info {
      fred_joined
      silent_meeting
      summary_status
    }
  }
}
"""

_USER_QUERY = """
query {
  user {
    email
  }
}
"""

_DETAIL_QUERY = """
query Transcript($id: String!) {
  transcript(id: $id) {
    id
    title
    date
    duration
    is_live
    organizer_email
    participants
    transcript_url
    speakers {
      id
      name
    }
    sentences {
      index
      text
      start_time
      end_time
      speaker_name
    }
    summary {
      keywords
      action_items
      overview
      gist
      short_summary
    }
    meeting_attendees {
      displayName
      email
    }
    meeting_info {
      fred_joined
      silent_meeting
      summary_status
    }
  }
}
"""


class RateLimitedError(Exception):
    """Raised when the API rate limit is hit."""

    def __init__(self, retry_after: float | None = None) -> None:
        super().__init__("Rate limited")
        self.retry_after = retry_after


class FatalAPIError(Exception):
    """Raised on 401/403 — stop all retries."""


class TransientAPIError(Exception):
    """GraphQL error or other transient failure — back off and retry later."""


class TranscriptNotFoundError(Exception):
    """Raised when a transcript ID no longer exists (404 / object_not_found).

    Permanent failure — the transcript was deleted from Fireflies and will
    never come back. Callers should write stub files and stop retrying.
    """


def _nest_meeting_fields(raw: JsonObject) -> JsonObject:
    """Promote a flat Fireflies transcript dict to one with a nested 'meeting' field.

    The detail query returns meeting fields (id, title, date, ...) as siblings
    of the transcript fields (sentences, speakers, ...). `TranscriptDetail`
    expects them under a `meeting` key, so we wrap the same dict in place — the
    sibling fields then get cleanly dropped by `Meeting`'s `extra="ignore"`.
    """
    if "meeting" in raw:
        return raw
    return {**raw, "meeting": raw}


# --- Internal "hive" API fallback ---
#
# The Fireflies web app uses an internal GraphQL endpoint at /api/v4/hive
# with session-based JWT auth. We use it as a fallback when the public
# `transcripts` list query is broken (returns 500). The session token
# expires every ~14 days and must be refreshed manually from the browser.

_HIVE_LIST_QUERY = """\
query fetchChannelMeetings($from: Int!, $size: Int!, $channelId: String!) {
  getChannelMeetings(from: $from, size: $size, channelId: $channelId) {
    total
    meetings {
      parseId
      date
      title
      creator_email
      durationMins
      validAttendees
      allEmails
      processMeetingStatus
      audioServiceMetadata { silentMeeting }
    }
  }
}"""

# Map internal processMeetingStatus to our summary_status values.
_HIVE_STATUS_MAP: dict[str, str] = {
    "completed": "processed",
}


def _hive_meeting_to_dict(raw: JsonObject) -> JsonObject:
    """Convert an internal hive API meeting to a dict that Meeting.model_validate accepts."""
    date_str = raw.get("date", "")
    epoch_ms: float = 0.0
    if isinstance(date_str, str) and date_str:
        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            epoch_ms = dt.timestamp() * 1000
        except ValueError:
            pass

    status = raw.get("processMeetingStatus", "")
    summary_status = _HIVE_STATUS_MAP.get(status, status) if isinstance(status, str) else ""

    audio_meta = raw.get("audioServiceMetadata")
    silent = audio_meta.get("silentMeeting", False) if isinstance(audio_meta, dict) else False

    parse_id = raw.get("parseId", "")

    participants: list[JsonValue] = []
    valid = raw.get("validAttendees")
    if isinstance(valid, list):
        for e in valid:
            if isinstance(e, str):
                participants.append(e)
    elif isinstance(raw.get("allEmails"), str):
        all_emails: str = raw["allEmails"]  # type: ignore[assignment]
        for e in all_emails.split():
            participants.append(e)

    return {
        "id": parse_id,
        "title": raw.get("title", ""),
        "date": epoch_ms,
        "duration": raw.get("durationMins") or 0,
        "is_live": False,
        "organizer_email": raw.get("creator_email", ""),
        "participants": participants,
        "transcript_url": f"https://app.fireflies.ai/view/{parse_id}",
        "meeting_info": {
            "fred_joined": False,
            "silent_meeting": silent,
            "summary_status": summary_status,
        },
    }


class FirefliesClient:
    """HTTP client for the Fireflies.ai GraphQL API."""

    def __init__(
        self,
        api_key: str,
        *,
        session_token: str | None = None,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        headers: dict[str, str] = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        if transport is not None:
            self._client = httpx.Client(
                headers=headers, timeout=30.0, transport=transport,
            )
        else:
            self._client = httpx.Client(headers=headers, timeout=30.0)
        # Monotonic timestamp until which the bucket is known to be exhausted.
        # Set when a response reports remaining<=0 so the *next* call backs off
        # without discarding the response we already received.
        self._rate_limit_blocked_until: float = 0.0

        # Optional internal API client for fallback when the public
        # `transcripts` query is broken. Uses session-based JWT auth
        # from the Fireflies web app.
        self._hive_client: httpx.Client | None = None
        if session_token:
            self._hive_client = httpx.Client(
                headers={
                    "authorization": session_token,
                    "Content-Type": "application/json",
                },
                timeout=30.0,
            )

    def _post(self, query: str, variables: dict[str, JsonValue]) -> JsonObject:
        """Execute a GraphQL query, handle rate limit headers and errors."""
        if time.monotonic() < self._rate_limit_blocked_until:
            wait = self._rate_limit_blocked_until - time.monotonic()
            raise RateLimitedError(retry_after=wait)

        resp = self._client.post(_ENDPOINT, json={"query": query, "variables": variables})

        reset_header = resp.headers.get("x-ratelimit-reset-api")
        reset_secs: float | None
        try:
            reset_secs = float(reset_header) if reset_header else None
        except ValueError:
            log.warning("Malformed x-ratelimit-reset-api header: %r", reset_header)
            reset_secs = None

        if resp.status_code == 429:
            self._rate_limit_blocked_until = time.monotonic() + (reset_secs or 60.0)
            raise RateLimitedError(retry_after=reset_secs)

        if resp.status_code in (401, 403):
            raise FatalAPIError(f"Auth error: HTTP {resp.status_code}")

        resp.raise_for_status()

        try:
            body: JsonObject = resp.json()
        except (json.JSONDecodeError, ValueError) as e:
            raise TransientAPIError(f"Non-JSON response body: {e}") from e

        # Bucket-exhausted but the response itself succeeded — keep the data,
        # arm the next call to back off.
        remaining_header = resp.headers.get("x-ratelimit-remaining-api")
        if remaining_header is not None and int(remaining_header) <= 0:
            self._rate_limit_blocked_until = time.monotonic() + (reset_secs or 60.0)
            log.warning(
                "Rate limit bucket exhausted (reset in %ss); will block next call",
                reset_secs,
            )

        if "errors" in body and not body.get("data"):
            raise TransientAPIError(f"GraphQL errors: {body['errors']}")
        if "errors" in body:
            log.warning("GraphQL errors (partial data): %s", body["errors"])
        return body

    def list_transcripts(self, *, max_pages: int | None = None) -> list[Meeting]:
        """Fetch all transcripts, paginating through results.

        Falls back to the internal hive API if the public `transcripts`
        query returns errors and a session token is configured.
        """
        meetings: list[Meeting] = []
        skip = 0
        page = 0
        api_error = False

        while True:
            if max_pages is not None and page >= max_pages:
                break

            body = self._post(_LIST_QUERY, {"limit": _PAGE_SIZE, "skip": skip})
            data = body.get("data")
            raw_list = data.get("transcripts") if isinstance(data, dict) else None

            if not isinstance(raw_list, list) or not raw_list:
                if "errors" in body:
                    api_error = True
                break

            for raw in raw_list:
                if not isinstance(raw, dict):
                    continue
                try:
                    meetings.append(Meeting.model_validate(raw))
                except ValidationError as e:
                    log.warning("Skipping malformed transcript record: %s", e)
                    continue

            if len(raw_list) < _PAGE_SIZE:
                break

            skip += _PAGE_SIZE
            page += 1

        if not meetings and api_error and self._hive_client is not None:
            log.warning("Public transcripts query failed; falling back to internal API")
            return self._list_via_hive(max_pages=max_pages)

        return meetings

    def _list_via_hive(self, *, max_pages: int | None = None) -> list[Meeting]:
        """Fallback: list meetings via the internal Fireflies web API.

        Requires a session token (from browser login). The response shape
        differs from the public API, so we convert each meeting dict before
        validating.
        """
        assert self._hive_client is not None
        meetings: list[Meeting] = []
        offset = 0
        page = 0

        while True:
            if max_pages is not None and page >= max_pages:
                break

            variables: JsonObject = {
                "from": offset,
                "size": _PAGE_SIZE,
                "channelId": "all",
            }
            try:
                resp = self._hive_client.post(
                    _HIVE_ENDPOINT,
                    json={"query": _HIVE_LIST_QUERY, "variables": variables},
                )
                resp.raise_for_status()
                body: JsonObject = resp.json()
            except (httpx.HTTPError, json.JSONDecodeError, ValueError) as e:
                log.warning("Hive API fallback failed: %s", e)
                break

            channel = body.get("data")
            channel_meetings = (
                channel.get("getChannelMeetings")
                if isinstance(channel, dict) else None
            )
            raw_list = (
                channel_meetings.get("meetings")
                if isinstance(channel_meetings, dict) else None
            )

            if not isinstance(raw_list, list) or not raw_list:
                break

            for raw in raw_list:
                if not isinstance(raw, dict):
                    continue
                try:
                    converted = _hive_meeting_to_dict(raw)
                    meetings.append(Meeting.model_validate(converted))
                except (ValidationError, KeyError) as e:
                    log.warning("Skipping malformed hive meeting: %s", e)

            if len(raw_list) < _PAGE_SIZE:
                break

            offset += _PAGE_SIZE
            page += 1

        if meetings:
            log.info("Hive API fallback returned %d meetings", len(meetings))
        return meetings

    def get_transcript(self, meeting_id: str) -> TranscriptDetail:
        """Fetch full transcript detail including sentences and summary."""
        body = self._post(_DETAIL_QUERY, {"id": meeting_id})
        data = body.get("data")
        raw = data.get("transcript") if isinstance(data, dict) else None
        if not isinstance(raw, dict) or not raw:
            # Distinguish permanent 404 (deleted transcript) from transient errors.
            errors = body.get("errors")
            if isinstance(errors, list) and any(
                isinstance(e, dict) and e.get("code") == "object_not_found"
                for e in errors
            ):
                raise TranscriptNotFoundError(
                    f"Transcript {meeting_id} no longer exists"
                )
            raise TransientAPIError(f"No transcript data returned for {meeting_id}")
        return TranscriptDetail.model_validate(_nest_meeting_fields(raw))

    def get_user_email(self) -> str | None:
        """Fetch the authenticated user's email address."""
        try:
            body = self._post(_USER_QUERY, {})
        except (RateLimitedError, FatalAPIError, TransientAPIError, httpx.HTTPError):
            log.warning("Failed to fetch user email from API")
            return None
        data = body.get("data")
        user = data.get("user") if isinstance(data, dict) else None
        if not isinstance(user, dict):
            return None
        email = user.get("email")
        return str(email) if isinstance(email, str) and email else None

    def close(self) -> None:
        self._client.close()
        if self._hive_client is not None:
            self._hive_client.close()
