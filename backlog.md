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

## Audit & Clarify Source-Merging Semantics

### What It Is

The store now folds in meetings from up to four discovery sources:

1. Public `transcripts` GraphQL query (the API key path) — finished transcripts only.
2. Internal `getChannelMeetings` (hive endpoint, fallback) — used only when the public call errors.
3. Internal `getUserMeetingsForStatus` (hive endpoint, supplemental) — surfaces meetings in any state, including processing/errored/audio-too-small.
4. Google Chat watcher — IDs only, fed via `watch_meeting`.
5. `active_meetings` GraphQL query — IDs only, used to mark live state.

Today's merge logic is ad-hoc: public list wins on conflict, status fills gaps, Chat/active_meetings flip `is_live=True`. There's no single owner-per-field model, and conflicts (e.g. status reports `processMeetingStatus=failed` while public returns a transcript) aren't reasoned about explicitly.

### Why It Matters

- Conflict cases work today but are accidental. Adding a sixth source or changing one method's behavior could silently corrupt state.
- The status API exposes useful diagnostics (`errorDetail`, `puppetExitReason`) we currently throw away — we'd want them rendered into stub `summary.md` files, but that needs the merge story tightened first.
- Stale state from any source (e.g. status flipping a meeting to "failed" after the public API has the transcript) could hide good data.

### Acceptance

- Document each source's authority per field (id, title, date, is_live, summary_status, transcript content, error details).
- Update `_fetch_meetings` and `watch_meeting` / `sync_active_meeting_ids` to reflect that document.
- Add `errorDetail` / `puppetExitReason` to the status query and surface them in stub renderings.
- Add tests covering at least: public-only, status-only, public+status conflict, public→status downgrade-blocked, Chat-discovers-after-status.

### Rough Size

- Documentation + refactor: ~200-400 LOC including tests.

## Cache 404s from Chat Watcher

### What It Is

When the Chat watcher discovers a `app.fireflies.ai/live/<id>` URL whose `getTranscript(id)` returns 404, the watcher keeps retrying that ID for the full 7-day lookback window.

### Symptom

Observed in journal: a single ID firing `watch_meeting(<id>): transcript not found, skipping` every ~55s for hours. Over the 7-day lookback that's ~11k pointless requests per stuck ID.

### Why It Happens

`watch_meeting` swallows `TranscriptNotFoundError` and returns without recording anything. Next Chat poll re-discovers the same URL, calls `watch_meeting` again, gets the same 404. No memory of "we already tried this."

### Fix Sketch

Cache a "tried, 404'd" set keyed by meeting ID with a TTL (e.g. 6h, then expire so a delayed transcript gets a chance). `watch_meeting` short-circuits if ID is in the negative cache. Could live alongside `_backoff` in `MeetingStore`.

### Rough Size

- Implementation: ~30-50 LOC plus a small test in `tests/`.
