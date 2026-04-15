from collections.abc import Sequence

from google.auth.transport.requests import Request

class Credentials:
    token: str | None
    refresh_token: str | None
    @property
    def valid(self) -> bool: ...
    @property
    def expired(self) -> bool: ...
    @classmethod
    def from_authorized_user_file(
        cls, filename: str, scopes: Sequence[str] | None = ...,
    ) -> Credentials: ...
    def refresh(self, request: Request) -> None: ...
    def to_json(self) -> str: ...
