# CLAUDE.md

Project-specific guidance for Claude Code (and other agents) working on this codebase. End-user docs live in [`README.md`](README.md); this file is for code contributors.

## What this is

A read-only FUSE filesystem that exposes Fireflies.ai meetings as a date-indexed tree of markdown files. Two layers:

- **Boundary** — `api.py` talks to the Fireflies GraphQL API; `store.py` persists results under `~/.cache/fireflies-meetings/`.
- **Filesystem** — `fuse_ops.py` implements `pyfuse3.Operations`; `inode_map.py` maintains the path↔inode mapping; `__main__.py` wires everything together with a trio nursery (FUSE main + background backfill).

`renderer.py` is pure: turns model objects into the `summary.md` / `transcript.md` / `participants.md` / `meeting.json` / `open.sh` byte strings the FUSE layer serves.

## Verification target

All three must be clean:

```bash
uv run ruff check fireflies_meetings tests
uv run basedpyright fireflies_meetings tests   # 0 errors, 0 warnings, 0 notes
uv run pytest -x -q
```

basedpyright runs in **strict mode** — don't add `Any` returns, don't add `# pyright: ignore` pragmas without first trying to fix the underlying type. The codebase currently has zero pragmas.

## Conventions

### Pydantic at I/O boundaries

All models in `models.py` are `BaseModel` with `frozen=True`, `populate_by_name=True`, `extra="ignore"`. They're used at three boundaries:

1. **Fireflies API responses** — `Meeting.model_validate(raw)` and `TranscriptDetail.model_validate(raw)` in `api.py`. Field aliases (`date`→`date_epoch_ms`, `duration`→`duration_mins`, `displayName`→`display_name`, `meeting_attendees`→`attendees`) handle the GraphQL shape.
2. **Disk cache round-trip** — `e.meeting.model_dump()` / `Meeting.model_validate(m)` in `store._save_list_cache` / `_load_list_cache`. Same model handles both API and cache shapes via `AliasChoices`.
3. **Internal use** — every other module just passes typed `Meeting` / `TranscriptDetail` objects around. Never manually parse a dict; if a new field is needed, add it to the model.

**Don't** add hand-rolled `_parse_*` helpers. **Don't** mutate models post-construction — they're frozen, use `model.model_copy(update={...})`. The `_with_slug()` helper in `store.py` is the canonical example.

`TranscriptDetail` accepts a flat API dict directly thanks to its `_promote_flat_meeting` before-validator, which wraps the same dict as the nested `meeting` field. `extra="ignore"` on `Meeting` means the sibling fields (`sentences`, `speakers`, etc.) get cleanly dropped during meeting validation.

### Lazy imports in `__main__.py`

`pyfuse3` and `trio` are heavy and `pyfuse3` requires a working FUSE library. To keep `fireflies-meetings unmount` fast and to allow `--help` / `unmount` on systems without FUSE installed, the heavy imports live inside `cmd_mount`, `_run_mount`, and `_backfill_cache` instead of at module top level.

The `PLC0415` ruff rule (import-not-at-top-of-file) is per-file-ignored for `__main__.py` to allow this. Don't add new lazy imports outside that file without a real reason.

### Functional decomposition

State mutation belongs in a small number of places (`MeetingStore._entries`, `_file_cache`, `_backoff`). Everything else is pure: `_render_files`, `_with_slug`, `_make_slug`, the renderer, the `_parse_path` helper. Prefer adding stateless helper functions over instance methods that mutate `self`.

### Error handling at the API boundary

Three exception types in `api.py`:

- `RateLimitedError(retry_after)` — 429 or response with exhausted bucket. Caller backs off, retries.
- `FatalAPIError` — 401/403. Caller sets a fatal flag, stops all retries until restart or `kill -USR1`.
- `TransientAPIError` — GraphQL `errors` block with no `data`. Caller backs off, retries.

`_BackoffState` in `store.py` tracks the backoff window with exponential growth and jitter. New API call sites should funnel through the same backoff state — don't add ad-hoc retry loops.

### pyfuse3 NewTypes

`pyfuse3` uses `NewType` for `InodeT`, `FileHandleT`, `FlagT`. Wrap plain ints when assigning to typed fields:

```python
entry.st_ino = pyfuse3.InodeT(inode)
fi.fh = pyfuse3.FileHandleT(inode)
return pyfuse3.FileHandleT(inode)  # from opendir
```

This is what makes `fuse_ops.py` pragma-free. Don't reintroduce `# pyright: ignore` here.

## Operations

```bash
# Force the running service to refresh (clears list cache, backoff,
# non-completed file caches; completed meetings stay served from disk)
kill -USR1 $(pgrep -f 'fireflies-meetings mount')

# Inspect the on-disk cache
ls ~/.cache/fireflies-meetings/
ls ~/.cache/fireflies-meetings/detail/<meeting-id>/

# Nuke the cache and start over
systemctl --user stop fireflies-meetings
rm -rf ~/.cache/fireflies-meetings
systemctl --user start fireflies-meetings
```

Cache layout, backoff windows, and the rationale for the backfill schedule are documented in [`README.md`](README.md#caching--backfill).

## Tests

`tests/test_renderer.py` covers the rendering layer. The store and FUSE layers don't have tests — they're harder to fixture against the real API, and the smoke-test path is restarting the service against a real cache. If you change the boundary parsing in `api.py` or `store.py`, the minimum viable check is:

```bash
# Round-trip every cached meeting through the model
uv run python -c "
import json
from fireflies_meetings.models import Meeting
data = json.load(open('$HOME/.cache/fireflies-meetings/list.json'))
for m in data['meetings']:
    Meeting.model_validate(m)
print(f'OK: {len(data[\"meetings\"])} meetings round-trip')
"
```

## Things that look weird but are intentional

- **`extra="ignore"` on every model** — required so `TranscriptDetail` can pass the same flat API dict to its `meeting` field (which gets the meeting fields plus the sibling sentences/speakers/etc.). Don't tighten this without first understanding the flat-promotion logic in `_promote_flat_meeting`.
- **Two slug fields on `Meeting`** — well, one. `slug` is empty after API validation and populated by the store via `_with_slug()` (which `model_copy`s). The store then carries it over to the detail object the same way. This separation exists because the slug is computed once at list-fetch time and reused for the detail directory layout.
- **`_resolve_collisions` re-derives slug names every time** — yes, it's idempotent and fast, and avoiding it would require caching collision-resolved names somewhere keyed by date. Not worth the complexity.
- **`/live/` symlinks point at the date tree** — letting `cd /views/fireflies-meetings/live/<slug>` land in the right `YYYY-MM/DD/<slug>/` dir. Don't replace the symlinks with real directory entries; the symlink is the contract.
- **Asymmetric `is_live` state machine** — positive signals are `watch_meeting` (Google Chat URL discovery) and `sync_active_meeting_ids` (the `active_meetings` GraphQL query). The only negative signal is a terminal `summary_status` (`processed` / `skipped` / `missing_from_api`), picked up by `_fetch_meetings` on list refresh or `_merge_live_stream_state_locked` on detail fetch. `active_meetings` returns `[]` for non-admin users even when meetings are live, so absence there can't be trusted — `sync_active_meeting_ids` is positive-only. Likewise, `_fetch_meetings` preserves `is_live=True` across list refreshes unless the new entry is terminal, because the list API reports `is_live=False` even for in-progress meetings.
