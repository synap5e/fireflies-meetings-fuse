"""Socket.IO client for Fireflies' internal live transcript stream."""

from __future__ import annotations

import logging
import queue
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

# Cap the in-flight caption buffer. python-engineio spawns one background
# thread per incoming message and calls our handler in it. If we call
# on_update inline, that thread blocks on the store's projection-rebuild
# lock while every subsequent caption spawns another thread that piles up
# behind it (7k+ zombie threads observed in ~24h of a single live meeting).
# Bounding the queue + dropping oldest keeps engineio's threads short-lived
# and gives the drainer room to coalesce.
_CAPTION_QUEUE_MAX = 1024


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


def spawn_caption_drainer(
    meeting_id: str,
    caption_queue: queue.Queue[tuple[str, Sentence] | None],
    on_update: Callable[[str, Sentence], None],
) -> threading.Thread:
    def drain_captions() -> None:
        while True:
            item = caption_queue.get()
            if item is None:
                return
            transcript_id, sentence = item
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
    meeting_id: str,
    caption_queue: queue.Queue[tuple[str, Sentence] | None],
) -> Callable[[object], None]:
    dropped = 0

    def on_transcription_broadcast(raw: object) -> None:
        nonlocal dropped
        normalized = normalize_stream_sentence(raw)
        if normalized is None:
            return
        try:
            caption_queue.put_nowait(normalized)
        except queue.Full:
            dropped += 1
            if dropped % 100 == 1:
                log.warning(
                    "Live caption drainer for %s falling behind; dropped %d captions",
                    meeting_id, dropped,
                )

    return on_transcription_broadcast


def shutdown_drainer(
    caption_queue: queue.Queue[tuple[str, Sentence] | None],
    drainer: threading.Thread,
) -> None:
    caption_queue.put(None)
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
    caption_queue: queue.Queue[tuple[str, Sentence] | None] = queue.Queue(maxsize=_CAPTION_QUEUE_MAX)
    drainer = spawn_caption_drainer(meeting_id, caption_queue, on_update)

    sio.on(
        "transcription.broadcast.event",
        handler=make_broadcast_handler(meeting_id, caption_queue),
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
        shutdown_drainer(caption_queue, drainer)
        raise LiveTranscriptStreamError(str(e)) from e
    try:
        while not stop_event.wait(1.0):
            pass
    finally:
        sio.disconnect()
        shutdown_drainer(caption_queue, drainer)
