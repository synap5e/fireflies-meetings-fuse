"""Import a valid Fireflies browser session from Chrome/Chromium cookies."""

from __future__ import annotations

import hashlib
import shutil
import sqlite3
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import httpx
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from .session_auth import SessionAuth, internal_request_headers

_COOKIE_NAMES: Final = ("authorization", "x-cache")
_AUTH_CHECK_QUERY = """
query availableAnnouncements {
  availableAnnouncements {
    id
  }
}
"""
_AUTH_CHECK_ENDPOINT = "https://app.fireflies.ai/api/v4/hive"


@dataclass(frozen=True)
class _BrowserConfig:
    name: str
    config_dir: Path
    secret_application: str
    command: str


_BROWSERS: Final[dict[str, _BrowserConfig]] = {
    "chrome": _BrowserConfig(
        name="chrome",
        config_dir=Path.home() / ".config" / "google-chrome",
        secret_application="chrome",
        command="google-chrome-stable",
    ),
    "chromium": _BrowserConfig(
        name="chromium",
        config_dir=Path.home() / ".config" / "chromium",
        secret_application="chromium",
        command="chromium",
    ),
}


def _cookie_db_path(config: _BrowserConfig, profile: str) -> Path:
    direct = config.config_dir / profile / "Cookies"
    if direct.exists():
        return direct
    network = config.config_dir / profile / "Network" / "Cookies"
    if network.exists():
        return network
    raise FileNotFoundError(
        f"Could not find {config.name} cookie DB for profile {profile!r} under {config.config_dir}"
    )


def _safe_storage_password(config: _BrowserConfig) -> bytes:
    try:
        result = subprocess.run(
            ["secret-tool", "search", "application", config.secret_application],
            capture_output=True,
            check=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as e:
        raise RuntimeError(
            "Failed to read browser safe-storage key via secret-tool"
        ) from e

    for line in result.stdout.splitlines():
        if line.startswith("secret = "):
            return line.split(" = ", 1)[1].encode()
    raise RuntimeError(f"No safe-storage key found for {config.name}")


def _derive_linux_chrome_key(password: bytes) -> bytes:
    return hashlib.pbkdf2_hmac("sha1", password, b"saltysalt", 1, 16)


def _decrypt_cookie_value(
    encrypted_value: bytes,
    *,
    version: int,
    safe_storage_password: bytes,
) -> str:
    prefix = encrypted_value[:3]
    if prefix == b"v10":
        keys = (_derive_linux_chrome_key(b"peanuts"), _derive_linux_chrome_key(b""))
    elif prefix == b"v11":
        keys = (_derive_linux_chrome_key(safe_storage_password), _derive_linux_chrome_key(b""))
    else:
        raise RuntimeError(f"Unsupported cookie encryption prefix {prefix!r}")

    iv = b" " * 16
    for key in keys:
        decryptor = Cipher(algorithms.AES(key), modes.CBC(iv)).decryptor()
        plaintext = decryptor.update(encrypted_value[3:]) + decryptor.finalize()
        pad = plaintext[-1]
        unpadded = plaintext[:-pad]
        if version >= 24:
            unpadded = unpadded[32:]
        try:
            return unpadded.decode()
        except UnicodeDecodeError:
            continue
    raise RuntimeError("Failed to decrypt Chrome cookie")


def _load_session_auth_from_browser(config: _BrowserConfig, profile: str) -> SessionAuth:
    cookie_db = _cookie_db_path(config, profile)
    tmp_dir = Path(tempfile.mkdtemp(prefix="fireflies-cookies-"))
    tmp_db = tmp_dir / "Cookies"
    try:
        shutil.copy2(cookie_db, tmp_db)
        con = sqlite3.connect(tmp_db)
        try:
            version_row = con.execute("select value from meta where key='version'").fetchone()
            version = int(version_row[0]) if version_row is not None else 0
            rows = con.execute(
                """
                select name, value, encrypted_value
                from cookies
                where host_key like '%fireflies.ai%'
                  and name in ('authorization', 'x-cache')
                """
            ).fetchall()
        finally:
            con.close()

        password = _safe_storage_password(config)
        values: dict[str, str] = {}
        for name, value, encrypted_value in rows:
            if isinstance(value, str) and value:
                values[name] = value
                continue
            if isinstance(encrypted_value, bytes) and encrypted_value:
                values[name] = _decrypt_cookie_value(
                    encrypted_value,
                    version=version,
                    safe_storage_password=password,
                )

        access_token = values.get("authorization", "")
        refresh_token = values.get("x-cache")
        if not access_token:
            raise RuntimeError("No Fireflies authorization cookie found in browser profile")
        return SessionAuth(access_token=access_token, refresh_token=refresh_token)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _session_is_valid(session_auth: SessionAuth) -> bool:
    headers = internal_request_headers(session_auth, referer="https://app.fireflies.ai/")
    try:
        resp = httpx.post(
            _AUTH_CHECK_ENDPOINT,
            headers=headers,
            json={
                "operationName": "availableAnnouncements",
                "variables": {},
                "query": _AUTH_CHECK_QUERY,
            },
            timeout=30.0,
        )
        if resp.status_code != 200:
            return False
        body = resp.json()
    except (httpx.HTTPError, ValueError):
        return False
    data = body.get("data")
    return isinstance(data, dict) and "availableAnnouncements" in data


def refresh_session_auth(
    dest_path: Path,
    *,
    browser: str,
    profile: str,
    open_login: bool,
    wait_timeout: float,
) -> SessionAuth:
    """Load a valid Fireflies session from browser cookies and save it to disk.

    If the current browser cookies are invalid and `open_login` is true, open the
    Fireflies login page in the same browser profile and wait for the user to
    complete sign-in.
    """
    config = _BROWSERS.get(browser)
    if config is None:
        raise ValueError(f"Unsupported browser {browser!r}")

    opened_browser = False
    deadline = time.monotonic() + wait_timeout
    last_error = "No valid Fireflies browser session found"

    while True:
        try:
            session_auth = _load_session_auth_from_browser(config, profile)
            if _session_is_valid(session_auth):
                session_auth.save(dest_path)
                return session_auth
            last_error = "Browser cookies exist, but Fireflies rejected the session"
        except (FileNotFoundError, OSError, RuntimeError, sqlite3.DatabaseError) as e:
            last_error = str(e)

        if not open_login:
            raise RuntimeError(last_error)

        if not opened_browser:
            subprocess.Popen(
                [
                    config.command,
                    f"--profile-directory={profile}",
                    "--new-window",
                    "https://app.fireflies.ai/login",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            opened_browser = True

        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"Timed out waiting for a valid Fireflies browser session ({last_error})"
            )
        time.sleep(2)
