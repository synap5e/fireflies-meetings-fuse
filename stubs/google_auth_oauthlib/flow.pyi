from collections.abc import Sequence

from google.oauth2.credentials import Credentials

class InstalledAppFlow:
    @classmethod
    def from_client_secrets_file(
        cls, client_secrets_file: str, scopes: Sequence[str],
    ) -> InstalledAppFlow: ...
    def run_local_server(
        self, host: str = ..., port: int = ..., open_browser: bool = ...,
    ) -> Credentials: ...
