# Backlog

Ideation, not spec. Canonical intent lives in [`AGENTS.md`](AGENTS.md) and [`README.md`](README.md); the reasoning behind past decisions is in [`docs/decision-log.md`](docs/decision-log.md).

Reconciled against the post-CQRS codebase on 2026-08-28. Ordered by what's actually blocking.

---

## Blocking

### Get memory under control

The service is stopped because of this. ~1.5 GB steady, 2.1 GB peak, 438 MB swap; a restart doesn't clear it. Breakdown and the reason the projection is fully materialized are in the decision log.

Options, costed but not chosen:

1. Stop pre-rendering `transcript.md` bytes; render on read from the parsed detail. ~1 hour, estimated ~800 MB. Note this puts work back on the read path — it must not become a *disk* read.
2. `mmap` + a custom sentence iterator instead of parsed pydantic in the projection.
3. Split the process (rejected once as overkill).
4. Port to the out-of-process Rust engine, which sidesteps the Python heap entirely.

Option 4 is entangled with the architecture review below, which is why nothing has been picked.

### Hold the architecture review

Agreed and never scheduled. The question is port to the `vfsd` Rust engine vs refactor pyfuse3 in place. Feeding into it:

- Are five concurrent trio loops the right decomposition?
- Should ghost/overlap folding move from render time to ingest time?
- Does `mine/` deserve a parallel subtree, or is it a shell alias?
- Should `_terminal_meeting_ids` be persisted, or the late-caption bug class be fixed by dropping the socket earlier?
- Should channels be first-class rather than a symlink fold?

`fuse_ops.py` (784 LOC) is on the chopping block either way. If the port wins, the producer is `fireflies_meetings/vfs_feed.py` (~150–250 LOC) answering Hello+Subscribe on `unix:/run/user/1000/fireflies-meetings/vfs-feed.sock`; shadow mountpoints already exist. Hard requirements already given to the engine owner: **stable inodes across restarts** (agents record `file:line` references) and a **never-fetch read path**.

---

## Correctness

### Negative cache for Chat-watcher 404s

`watch_meeting` swallows `TranscriptNotFoundError` without recording anything, so a Chat-discovered ID that 404s is retried every ~55s for the full 7-day lookback — roughly 11k pointless requests per stuck ID. Cache "tried, 404'd" keyed by meeting id with a ~6h TTL so a delayed transcript still gets a chance. ~30–50 LOC plus a test; lives alongside `_backoff` in `store.py`.

### Surface hive diagnostics

`getUserMeetingsForStatus` returns `errorDetail` and `puppetExitReason` — the diagnostics the public API lacks — and we throw them away. Add them to the query and render them into stub `summary.md` files so a failed meeting explains itself.

This is the surviving half of the old "audit & clarify source-merging semantics" item. The audit itself is **done**: `resolver.py` now holds a data-driven per-field precedence table, and every `Meeting` field must have an entry or be dropped.

### Hive session expiry has no sentinel

Missing or expired session auth makes `access_logs` FAIL, which stops the resolver promoting meetings to `captured`, which stops backfill draining — silently. There are `AUTHENTICATION_EXPIRED` and `CHAT_AUTH_EXPIRED` markers but no equivalent for hive.

---

## Features

### Live transcript stream file

A live-only `live-transcript.md` alongside `transcript.md`, so `tail -f` works cleanly on a meeting in progress.

- `transcript.md` stays the authoritative, corrected snapshot.
- `live-transcript.md` is append-only, best-effort, and exists only while `is_live=True`. New opens after the meeting ends get `ENOENT`; already-open fds drain to EOF.
- Visible through both `/live/<slug>/` and the dated path while live.

Risk: Fireflies rows are mutable, so superseded fragments can appear here that never make it into the final `transcript.md`. There is deliberately no resync protocol.

~210–320 LOC plus ~70–120 LOC of tests. Partly pre-empted — `tail --debug -f` reports *polling* mode on this mount, so the attr-timeout fix may already be enough. Verify that before building this.

### Per-meeting force refetch

There is no way to re-fetch one meeting; `SIGUSR1` is all-or-nothing. This is what would fix expired signed media URLs, since captured meetings are immutable-forever in cache and their `video_url` / `audio_url` go stale after ~4 days. Either extend `SIGUSR1` or add a control path — the literal `_control/refetch/<id>` form was declined at 1780+ meetings, so it needs a parameterized shape.

### Cached media files

`audio.mp3` / `video.mp4` as real files in the tree, JIT-fetched with LRU eviction and a size cap. Parked deliberately: ~80 GB for the full archive, 300–500 LOC through the layer we want to delete. Revisit after the architecture review.

### Raw-driven build path

Phase 3b of the raw-archive work: rebuild the projection from `raw/` rather than from normalized captures, with a shadow-verify harness comparing the two. Unblocked, never started, ~600 LOC. Insurance rather than a fix for anything currently broken.

---

## Housekeeping

- `.wt/handoff/video-urls-in-meeting-json` worktree kept for reference; nothing to unwind.
- Confirm `~/.cache/fireflies-meetings.pre-cqrs` (198 MB) and `detail.legacy.<ts>` were removed.
- On pro-crastinator: `~/.cache/fireflies-meetings/`, `~/.config/fireflies-meetings/`, the unit symlink and `.env` were left in place as rollback safety after the move to flow.
- Dependabot flagged one moderate advisory (2026-05-02), never actioned.
- `transcript_error` never persists: 0 of 2195 cached details carry one. It is *not* dead code — `api.py` sets it on the live partial-error path, `projection.py` clears it once sentences arrive, `renderer.py` renders it — so it only ever exists in memory mid-meeting. Worth confirming it actually reaches a rendered file before trusting it as a diagnostic. (An earlier note called this dead code; that was about a pre-resolver branch that no longer exists.)
- **`participants` list/detail parser conflict is fixed (`0013bb8`)** — `api.py` now splits `allEmails` on commas, so future fetches come in clean. The ~1376 meetings already cached with comma-joined values are *not* repaired by this fix; they heal only when that meeting is refetched (backfill pass, notification refresh, or a `SIGUSR1` refresh).
- Rename the CQRS vocabulary in `commands.py` / `tests/test_cqrs.py` to match what it actually is.
