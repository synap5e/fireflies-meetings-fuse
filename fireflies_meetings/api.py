"""Fireflies.ai GraphQL API client."""

from __future__ import annotations

import json
import logging
import time

import httpx
from pydantic import ValidationError

from .models import Meeting, TranscriptDetail

log = logging.getLogger(__name__)

_ENDPOINT = "https://api.fireflies.ai/graphql"
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


class FirefliesClient:
    """HTTP client for the Fireflies.ai GraphQL API."""

    def __init__(
        self,
        api_key: str,
        *,
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
        """Fetch all transcripts, paginating through results."""
        meetings: list[Meeting] = []
        skip = 0
        page = 0

        while True:
            if max_pages is not None and page >= max_pages:
                break

            body = self._post(_LIST_QUERY, {"limit": _PAGE_SIZE, "skip": skip})
            data = body.get("data")
            raw_list = data.get("transcripts") if isinstance(data, dict) else None

            if not isinstance(raw_list, list) or not raw_list:
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

        return meetings

    def get_transcript(self, meeting_id: str) -> TranscriptDetail:
        """Fetch full transcript detail including sentences and summary."""
        body = self._post(_DETAIL_QUERY, {"id": meeting_id})
        data = body.get("data")
        raw = data.get("transcript") if isinstance(data, dict) else None
        if not isinstance(raw, dict):
            raw = {}
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
