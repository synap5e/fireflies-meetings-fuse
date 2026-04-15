"""Helpers for Fireflies web-session auth used by internal fallback APIs."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import cast
from urllib.parse import quote, unquote

_DEFAULT_SESSION_AUTH_PATH = "~/.config/fireflies-meetings/session.json"
_INTERNAL_ORIGIN = "https://app.fireflies.ai"
_INTERNAL_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)


def default_session_auth_path() -> Path:
    return Path(_DEFAULT_SESSION_AUTH_PATH).expanduser()


def _normalize_token(raw: str, *, strip_bearer: bool) -> str:
    value = unquote(raw).strip()
    if strip_bearer and value.startswith("Bearer "):
        return value[len("Bearer "):].strip()
    return value


@dataclass(frozen=True)
class SessionAuth:
    """Tokens and metadata for the Fireflies web app's internal API."""

    access_token: str
    refresh_token: str | None = None
    auth_provider: str = "gauth"

    def __post_init__(self) -> None:
        access_token = _normalize_token(self.access_token, strip_bearer=True)
        if not access_token:
            raise ValueError("access_token must not be empty")
        refresh_token = None
        if self.refresh_token:
            refresh_token = _normalize_token(self.refresh_token, strip_bearer=False)
        auth_provider = self.auth_provider.strip() or "gauth"

        object.__setattr__(self, "access_token", access_token)
        object.__setattr__(self, "refresh_token", refresh_token)
        object.__setattr__(self, "auth_provider", auth_provider)

    @classmethod
    def from_env(cls) -> SessionAuth | None:
        access_token = os.environ.get("FIREFLIES_SESSION_TOKEN", "").strip()
        if not access_token:
            return None
        refresh_token = os.environ.get("FIREFLIES_REFRESH_TOKEN", "").strip() or None
        auth_provider = os.environ.get("FIREFLIES_AUTH_PROVIDER", "").strip() or "gauth"
        return cls(
            access_token=access_token,
            refresh_token=refresh_token,
            auth_provider=auth_provider,
        )

    @classmethod
    def load(cls, path: Path) -> SessionAuth:
        raw = json.loads(path.read_text())
        if not isinstance(raw, dict):
            raise ValueError(f"Session auth file {path} must contain a JSON object")
        typed_raw = cast("dict[str, object]", raw)

        access_token = typed_raw.get("access_token")
        refresh_token = typed_raw.get("refresh_token")
        auth_provider = typed_raw.get("auth_provider", "gauth")

        if not isinstance(access_token, str) or not access_token.strip():
            raise ValueError(f"Session auth file {path} is missing access_token")
        if refresh_token is not None and not isinstance(refresh_token, str):
            raise ValueError(f"Session auth file {path} has a non-string refresh_token")
        if not isinstance(auth_provider, str):
            raise ValueError(f"Session auth file {path} has a non-string auth_provider")

        return cls(
            access_token=access_token,
            refresh_token=refresh_token,
            auth_provider=auth_provider,
        )

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        data: dict[str, str] = {
            "access_token": self.access_token,
            "auth_provider": self.auth_provider,
        }
        if self.refresh_token:
            data["refresh_token"] = self.refresh_token
        path.write_text(json.dumps(data, indent=2) + "\n")

    @property
    def cookie_header(self) -> str:
        cookies = [f"authorization={quote(f'Bearer {self.access_token}')}"]
        if self.refresh_token:
            cookies.append(f"x-cache={quote(self.refresh_token)}")
        return "; ".join(cookies)


def internal_request_headers(session_auth: SessionAuth, *, referer: str) -> dict[str, str]:
    """Return browser-like headers for the Fireflies internal web-app API."""
    headers = {
        "Accept": "*/*",
        "Content-Type": "application/json",
        "Origin": _INTERNAL_ORIGIN,
        "Referer": referer,
        "User-Agent": _INTERNAL_USER_AGENT,
        "apollographql-client-name": "app.fireflies.ai",
        "apollographql-client-version": "fireflies-meetings-fuse",
        "x-graphql-client-name": "dashboard-ff",
        "x-graphql-client-version": "fireflies-meetings-fuse",
        "authorization": session_auth.access_token,
        "Cookie": session_auth.cookie_header,
    }
    if session_auth.refresh_token:
        headers["x-refresh-token"] = session_auth.refresh_token
        headers["x-auth-provider"] = session_auth.auth_provider
    return headers
