"""Google Chat poller — discovers live Fireflies meetings.

When Fireflies' bot joins a Google Meet, it posts a message into the Meet
chat containing the live transcript URL:

    https://app.fireflies.ai/live/<meeting_id>?ref=live_chat

The Fireflies GraphQL API doesn't surface live meetings to non-admin users
(see CLAUDE.md), so we watch the user's Google Chat spaces for those URLs
and pass the extracted IDs to the MeetingStore.
"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import httpx
from google.auth.exceptions import GoogleAuthError, RefreshError
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

log = logging.getLogger(__name__)

_SCOPES = [
    "https://www.googleapis.com/auth/chat.messages.readonly",
    "https://www.googleapis.com/auth/chat.spaces.readonly",
]
_CHAT_API = "https://chat.googleapis.com/v1"

# Meeting IDs are further validated by models.Meeting — we just grab the
# candidate out of the URL here.
_LIVE_URL_RE = re.compile(r"https?://app\.fireflies\.ai/live/([A-Za-z0-9_-]{8,64})")


def find_client_secrets(secrets_dir: Path) -> Path | None:
    """Return the first `client_secret_*.json` in `secrets_dir`, if any."""
    matches = sorted(secrets_dir.glob("client_secret_*.json"))
    return matches[0] if matches else None


def authorize(client_secrets_path: Path, token_path: Path) -> None:
    """Run the OAuth flow interactively and save the token.

    Opens a browser; only call from a CLI command, not the mount service.
    """
    flow = InstalledAppFlow.from_client_secrets_file(str(client_secrets_path), _SCOPES)
    creds = flow.run_local_server(port=0)
    token_path.parent.mkdir(parents=True, exist_ok=True)
    token_path.write_text(creds.to_json())
    token_path.chmod(0o600)
    log.info("Saved Google Chat token to %s", token_path)


def load_credentials(token_path: Path) -> Credentials | None:
    """Load credentials from disk; refresh if expired; None if unusable."""
    if not token_path.exists():
        return None
    try:
        creds = Credentials.from_authorized_user_file(str(token_path), _SCOPES)
    except (OSError, ValueError, json.JSONDecodeError) as e:
        log.warning("Failed to load chat token: %s", e)
        return None
    if creds.valid:
        return creds
    if creds.expired and creds.refresh_token:
        try:
            creds.refresh(GoogleAuthRequest())
        except (RefreshError, GoogleAuthError) as e:
            log.warning("Failed to refresh chat token: %s", e)
            return None
        try:
            token_path.write_text(creds.to_json())
        except OSError as e:
            log.warning("Failed to persist refreshed token: %s", e)
        return creds
    return None


class ChatWatcher:
    """Polls Google Chat spaces for Fireflies live-meeting URLs."""

    def __init__(
        self,
        credentials: Credentials,
        *,
        token_path: Path | None = None,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._creds = credentials
        self._token_path = token_path
        if transport is not None:
            self._client = httpx.Client(timeout=30.0, transport=transport)
        else:
            self._client = httpx.Client(timeout=30.0)

    def _headers(self) -> dict[str, str]:
        if not self._creds.valid:
            try:
                self._creds.refresh(GoogleAuthRequest())
            except (RefreshError, GoogleAuthError) as e:
                raise httpx.HTTPError(f"Chat credentials refresh failed: {e}") from e
            if self._token_path is not None:
                try:
                    self._token_path.write_text(self._creds.to_json())
                except OSError as e:
                    log.warning("Failed to persist refreshed token: %s", e)
        token = self._creds.token
        if not isinstance(token, str):
            raise httpx.HTTPError("Chat credentials have no access token")
        return {"Authorization": f"Bearer {token}"}

    def _paginate(
        self, url: str, params: dict[str, Any], key: str, *, max_pages: int = 10,
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        next_token: str | None = None
        for _ in range(max_pages):
            page_params = {**params, "pageToken": next_token} if next_token else params
            resp = self._client.get(url, headers=self._headers(), params=page_params)
            resp.raise_for_status()
            body_raw: Any = resp.json()
            if not isinstance(body_raw, dict):
                break
            body = cast("dict[str, Any]", body_raw)
            raw = body.get(key, [])
            if isinstance(raw, list):
                raw_items = cast("list[Any]", raw)
                items.extend(cast("dict[str, Any]", x) for x in raw_items if isinstance(x, dict))
            token = body.get("nextPageToken")
            if not isinstance(token, str) or not token:
                break
            next_token = token
        return items

    def list_spaces(self) -> list[dict[str, Any]]:
        return self._paginate(f"{_CHAT_API}/spaces", {"pageSize": 1000}, "spaces")

    def list_messages_since(self, space_name: str, since_epoch: float) -> list[dict[str, Any]]:
        since_iso = datetime.fromtimestamp(since_epoch, tz=UTC).isoformat()
        return self._paginate(
            f"{_CHAT_API}/{space_name}/messages",
            {"filter": f'createTime > "{since_iso}"', "pageSize": 100},
            "messages",
        )

    def find_live_meeting_ids(self, *, lookback_seconds: float = 3600.0) -> set[str]:
        """Poll every space; return IDs found in recent messages."""
        since = time.time() - lookback_seconds
        found: set[str] = set()
        try:
            spaces = self.list_spaces()
        except httpx.HTTPError as e:
            log.warning("ChatWatcher: list_spaces failed: %s", e)
            return found

        log.debug("ChatWatcher: scanning %d spaces (lookback %.0fs)", len(spaces), lookback_seconds)
        for space in spaces:
            name = space.get("name")
            if not isinstance(name, str):
                continue
            try:
                messages = self.list_messages_since(name, since)
            except httpx.HTTPError as e:
                log.debug("ChatWatcher: messages.list failed for %s: %s", name, e)
                continue
            for msg in messages:
                text = msg.get("text")
                if not isinstance(text, str):
                    continue
                for match in _LIVE_URL_RE.finditer(text):
                    found.add(match.group(1))
        return found

    def close(self) -> None:
        self._client.close()
