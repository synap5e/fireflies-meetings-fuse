from collections.abc import Callable, Mapping, Sequence

class ConnectionError(Exception): ...

class _ExceptionsNamespace:
    ConnectionError: type[ConnectionError]

exceptions: _ExceptionsNamespace

class Client:
    def __init__(
        self,
        *,
        reconnection: bool = ...,
        logger: bool = ...,
        engineio_logger: bool = ...,
        request_timeout: int = ...,
    ) -> None: ...
    def on(
        self,
        event: str,
        handler: Callable[[object], None] | None = ...,
        namespace: str | None = ...,
    ) -> None: ...
    def connect(
        self,
        url: str,
        headers: Mapping[str, str] = ...,
        auth: Mapping[str, str] | None = ...,
        transports: Sequence[str] | None = ...,
        namespaces: Sequence[str] | None = ...,
        socketio_path: str = ...,
        wait: bool = ...,
        wait_timeout: int = ...,
        retry: bool = ...,
    ) -> None: ...
    def disconnect(self) -> None: ...
