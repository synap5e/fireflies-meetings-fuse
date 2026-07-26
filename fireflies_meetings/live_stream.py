"""Socket.IO client for Fireflies' internal live transcript stream."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from typing import cast

import socketio
from pydantic import ValidationError

from .api import FirefliesClient
from .models import Sentence

log = logging.getLogger(__name__)

_LIVE_STREAM_URL = "https://realtime.firefliesapp.com"
_LIVE_STREAM_NAMESPACE = "/transcription"
_LIVE_STREAM_PATH = "/socket.io"


class LiveTranscriptStreamError(Exception):
    """Raised when the Fireflies internal live transcript stream fails."""


def normalize_stream_sentence(raw: object) -> tuple[str, Sentence] | None:
    if not isinstance(raw, dict):
        return None
    typed_raw = cast("dict[str, object]", raw)

    transcript_id = typed_raw.get("transcript_id")
    if transcript_id is None:
        return None
    transcript_key = str(transcript_id)
    if not transcript_key:
        return None

    try:
        sentence = Sentence.model_validate({
            "index": int(transcript_key) if transcript_key.isdigit() else 0,
            "sentence": typed_raw.get("sentence"),
            "time": typed_raw.get("time"),
            "endTime": typed_raw.get("endTime"),
            "speaker_name": typed_raw.get("speaker_name"),
        })
    except (ValidationError, ValueError) as e:
        log.warning("Skipping malformed live stream event: %s", e)
        return None

    if not sentence.text:
        return None
    return transcript_key, sentence


class CaptionCoalescer:
    """Latest-per-transcript_id inbox for live captions.

    python-engineio dispatches each incoming socketio message in its own
    daemon thread. If our handler blocks (e.g. on the store's rebuild
    lock), engineio spawns more threads that pile up — 7591 zombie threads
    observed in 24h of one live meeting before the drainer split.

    Fireflies also sends *progressive corrections* to the same
    transcript_id (final revisions of a partial sentence). A FIFO queue
    with drop-on-full preserves stale drafts and loses finals; instead we
    coalesce by transcript_id — a newer caption for the same row replaces
    the pending one. Bounded by the number of unique in-flight rows, not
    by arrival rate.
    """

    def __init__(self) -> None:
        self._pending: dict[str, tuple[str, Sentence]] = {}
        self._lock = threading.Lock()
        self._wakeup = threading.Event()
        self._stopping = threading.Event()

    def submit(self, transcript_id: str, sentence: Sentence) -> None:
        with self._lock:
            self._pending[transcript_id] = (transcript_id, sentence)
        self._wakeup.set()

    def drain_batch(self) -> list[tuple[str, Sentence]]:
        with self._lock:
            batch = list(self._pending.values())
            self._pending.clear()
        return batch

    def wait_for_work(self, timeout: float | None = None) -> bool:
        signalled = self._wakeup.wait(timeout=timeout)
        self._wakeup.clear()
        return signalled and not self._stopping.is_set()

    def stop(self) -> None:
        self._stopping.set()
        self._wakeup.set()

    @property
    def stopping(self) -> bool:
        return self._stopping.is_set()


def spawn_caption_drainer(
    meeting_id: str,
    coalescer: CaptionCoalescer,
    on_update: Callable[[str, Sentence], None],
) -> threading.Thread:
    def drain_captions() -> None:
        while not coalescer.stopping:
            coalescer.wait_for_work()
            for transcript_id, sentence in coalescer.drain_batch():
                try:
                    on_update(transcript_id, sentence)
                except Exception:
                    log.exception("Live caption apply failed for %s", meeting_id)
        # Final drain — apply anything the coalescer accumulated between
        # the last wait and stop() so we don't lose the meeting's last words.
        for transcript_id, sentence in coalescer.drain_batch():
            try:
                on_update(transcript_id, sentence)
            except Exception:
                log.exception("Live caption apply failed for %s", meeting_id)

    drainer = threading.Thread(
        target=drain_captions,
        name=f"live-caption-drainer-{meeting_id}",
        daemon=True,
    )
    drainer.start()
    return drainer


def make_broadcast_handler(
    coalescer: CaptionCoalescer,
) -> Callable[[object], None]:
    def on_transcription_broadcast(raw: object) -> None:
        normalized = normalize_stream_sentence(raw)
        if normalized is None:
            return
        transcript_id, sentence = normalized
        coalescer.submit(transcript_id, sentence)

    return on_transcription_broadcast


def shutdown_drainer(coalescer: CaptionCoalescer, drainer: threading.Thread) -> None:
    coalescer.stop()
    drainer.join(timeout=5.0)


def stream_live_transcript(
    client: FirefliesClient,
    meeting_id: str,
    *,
    on_update: Callable[[str, Sentence], None],
    stop_event: threading.Event,
) -> None:
    """Block until `stop_event` is set while streaming live caption updates."""
    token = client.get_internal_realtime_token(meeting_id)
    if token is None:
        return

    sio = socketio.Client(reconnection=False, logger=False, engineio_logger=False, request_timeout=10)
    coalescer = CaptionCoalescer()
    drainer = spawn_caption_drainer(meeting_id, coalescer, on_update)

    sio.on(
        "transcription.broadcast.event",
        handler=make_broadcast_handler(coalescer),
        namespace=_LIVE_STREAM_NAMESPACE,
    )

    url = (
        f"{_LIVE_STREAM_URL}?sample_rate=48000"
        f"&meetingId={meeting_id}&assistMode=general-assist"
    )
    headers = {
        "Origin": "https://app.fireflies.ai",
        "Referer": f"https://app.fireflies.ai/view/{meeting_id}",
        "User-Agent": "Mozilla/5.0",
    }
    try:
        sio.connect(
            url,
            headers=headers,
            transports=["websocket"],
            namespaces=[_LIVE_STREAM_NAMESPACE],
            socketio_path=_LIVE_STREAM_PATH,
            wait_timeout=10,
            auth={"token": token, "meetingId": meeting_id},
        )
    except (OSError, ValueError, socketio.exceptions.ConnectionError) as e:
        shutdown_drainer(coalescer, drainer)
        raise LiveTranscriptStreamError(str(e)) from e
    try:
        while not stop_event.wait(1.0):
            pass
    finally:
        sio.disconnect()
        shutdown_drainer(coalescer, drainer)
