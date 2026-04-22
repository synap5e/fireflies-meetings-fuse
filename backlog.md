# Backlog

## Live Transcript Stream File

### What It Is

Add a live-only `live-transcript.md` alongside the existing `transcript.md`.

- `transcript.md` stays the authoritative snapshot file.
- `live-transcript.md` is an append-only, best-effort live stream.
- `live-transcript.md` exists only while `is_live=True`.

### Why We'd Want It

It would make plain `tail -f` work cleanly for live meetings.

- Readers get transcript-so-far from byte 0, then block for more data.
- When the meeting ends, readers drain the remaining bytes and get EOF.

### Contract Notes

- The file is visible through both `/live/<slug>/live-transcript.md` and the dated tree path while the meeting is live.
- New opens after the meeting ends get `ENOENT`.
- Existing open file descriptors keep reading buffered data until EOF.
- The stream is best-effort append-only output.
- There is no resync or rewrite protocol for corrected rows.

### Rough Size

- Implementation on top of the baseline fix: about 210-320 LOC
- Tests on top of the baseline fix: about 70-120 LOC

### Risks

- Fireflies WebSocket rows are mutable, so superseded fragments may appear in `live-transcript.md` even if they do not survive into final `transcript.md`.
