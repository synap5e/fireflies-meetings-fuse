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

    def on_transcription_broadcast(raw: object) -> None:
        normalized = normalize_stream_sentence(raw)
        if normalized is None:
            return
        transcript_id, sentence = normalized
        on_update(transcript_id, sentence)

    sio.on(
        "transcription.broadcast.event",
        handler=on_transcription_broadcast,
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
        raise LiveTranscriptStreamError(str(e)) from e
    try:
        while not stop_event.wait(1.0):
            pass
    finally:
        sio.disconnect()
