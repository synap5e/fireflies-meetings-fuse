# AGENTS.md

Guidance for agents and contributors working on this codebase. End-user docs are in [`README.md`](README.md). Historical context — why things are shaped the way they are, what was tried and rejected — is in [`docs/decision-log.md`](docs/decision-log.md). Open work is in [`backlog.md`](backlog.md).

**Generated:** 2026-08-28 · **Commit:** `ba634ad` · **Branch:** `main`

## Overview

Read-only FUSE filesystem exposing Fireflies.ai meetings as a date-indexed markdown tree at `/views/fireflies-meetings/`. Python 3.12, uv, pyfuse3 + trio, httpx, pydantic. ~7.5k LOC across 20 modules, 165 tests.

## Architecture

Four layers. Data flows one way; the read path never touches the network.

```
API boundary        api.py, browser_auth.py, session_auth.py, chat_watcher.py, live_stream.py
      ↓ typed models (models.py) + raw archive (raw.py)
Orchestration       store.py — the only stateful coordinator: TTLs, backfill, backoff, locks
      ↓ Command objects (commands.py)
Write/derive        commands.CommandProcessor.apply() — the single writer
                    capture.py (durable normalized facts) → resolver.py (field precedence)
                    → projection.py (fold, place, render to bytes)
      ↓ frozen Projection
Read                fuse_ops.py serves projection bytes; inode_map.py holds path↔inode
```

**Naming warning:** `commands.py` and `tests/test_cqrs.py` use CQRS vocabulary. It is aspirational, not accurate — every command is `receive external fact → write cache → rebuild projection`, with zero user intent. This is change-data-capture into a materialized view. Don't reason about it as CQRS.

**Projection is fully materialized in memory.** Every meeting's six rendered files exist as bytes before any read. This is deliberate: lazy per-read disk loads take FUSE latency from µs to ms. It is also why memory is the project's biggest open problem.

## Where to look

| Task | File | Note |
|---|---|---|
| Add/change a `Meeting` field | `models.py` **and** `resolver.py` | A field with no precedence rule is silently dropped at resolve |
| New Fireflies endpoint | `api.py` | Wire through `RawSink`; funnel errors into the three exception types |
| Change what a file contains | `renderer.py` | Pure; `projection.py` calls it at build time |
| Change the directory tree | `projection.py` | Placement, ghost/overlap folding, symlinks, collisions |
| Change caching/TTL/backfill | `store.py` + `__main__.py` | Loops live in the nursery in `__main__.py` |
| Change FUSE syscall behaviour | `fuse_ops.py` | Attr timeouts, `direct_io`, symlink kinds |
| Disk format | `capture.py` | `list.json`, `channels.json`, `meetings/<id>/{detail,access_logs}.json` |
| Immutable API archive | `raw.py` | `raw/<source>/<UTC-date>.jsonl` + `_dedup/` + `_last_seen/` |

## Module map

| Module | LOC | Role |
|---|---|---|
| `api.py` | 1335 | Public GraphQL + internal hive/notepad + realtime token. Raw archival on every response |
| `projection.py` | 934 | Pure `(captures, time) → Projection`. Folding, placement, rendering |
| `fuse_ops.py` | 784 | `pyfuse3.Operations`. Owner wants this layer deleted (see decision log) |
| `__main__.py` | 705 | CLI (`mount`/`auth-session`/`auth-chat`/`unmount`) + trio nursery |
| `resolver.py` | 595 | Pure `MeetingEvidence → ResolvedMeeting` via a data-driven precedence table |
| `store.py` | 577 | Stateful coordinator + read facade. Owns locks, TTLs, `_BackoffState` |
| `models.py` | 435 | Frozen pydantic boundary models |
| `capture.py` | 398 | Durable normalized captures; `CaptureSnapshot` / `CaptureStore` |
| `renderer.py` | 283 | Pure model → bytes for the six leaf files |
| `commands.py` | 225 | Command union + `CommandProcessor` (the single writer) |
| `raw.py` | 221 | Append-only JSONL archive of every API observation |
| `live_stream.py` | 221 | Socket.IO captions + `CaptionCoalescer` |
| `browser_auth.py` / `session_auth.py` | 236 / 125 | Chrome cookie extraction; internal-session token |
| `chat_watcher.py` | 191 | Google Chat polling for `app.fireflies.ai/live/<id>` URLs |
| `access_logs.py`, `inode_map.py`, `slug.py`, `status_cache.py` | 41–75 | Outcome types, inode bookkeeping, slugging, cache dir |

## Invariants

These are load-bearing. Breaking one produces a bug that took days to find the first time.

- **Never block or fetch on the read path.** `read()`/`readdir()`/`lookup()` serve from the in-memory projection only. A network call here reproduces the ENOENT-cascade class of bug that made `rg` over the tree fail.
- **`CommandProcessor.apply()` is the single writer** and must run under `store`'s lock. Several background loops reach it via `trio.to_thread.run_sync`, and it mutates non-atomic state. The lock existed but was never acquired once; that shipped.
- **Every `Meeting` field needs a `resolver.py` precedence entry** or it is lost during resolution.
- **Raw API responses are immutable and never deleted.** Derived state is always rebuildable from `raw/`. Never discard one source's fields because another source has newer data.
- **Never let a 404 destroy cached content.** Both 404 paths check disk for non-stub content before overwriting.
- **Absence is never a negative signal.** The list API is a cumulative superset, reports `is_live=false` for in-progress meetings, and never returns `summary_status`. `active_meetings` returns `[]` for non-admin users. Only a terminal `summary_status` (or the 12h age-out) may clear live state.
- **Captions are latest-wins per `transcript_id`**, never FIFO-drop. Fireflies sends progressive corrections to the same row id.
- **Never do blocking work in a socketio handler.** `python-engineio` dispatches each message on a fresh thread (`run_async=True`); a blocking handler leaked 7591 threads.
- **`InodeMap` is not owned by the projection.** It tracks kernel lookup counts across projection swaps; recreating it hands out wrong files or ENOENT for open handles.
- **`/live/` and `/channels/` entries are symlinks into the date tree.** The symlink is the contract; don't promote them to real directories.

## Conventions

- **Pydantic at I/O boundaries only.** Frozen, `populate_by_name=True`, `extra="ignore"`. No hand-rolled `_parse_*` helpers; no post-construction mutation — `model_copy(update={...})`. `AliasChoices` lets one model round-trip both API and cache shapes.
- **Pure functions over stateful methods.** When ruff `C901` trips, extract a stateless helper — not another method on `self`, and not a `noqa`.
- **Type suppressions are near-zero and stay that way.** Current state: one `# type: ignore[assignment]` in `api.py:362`, seven in tests, one file-level `# pyright: reportPrivateUsage=false` in `tests/test_backoff.py`. Zero `# pyright: ignore`, zero `# noqa`. Prefer a stub in `stubs/` over a pragma — that's why `stubs/google*` and `stubs/socketio` exist.
- **Wrap pyfuse3 `NewType`s** (`InodeT`, `FileHandleT`, `FlagT`) rather than suppressing the type error.
- **Lazy imports are confined to `__main__.py`** (per-file `PLC0415` ignore) so `unmount`/`--help` work without FUSE installed. Don't add them elsewhere.
- **Tests never hit the network** — `httpx.MockTransport`, fakes, `tmp_path`, `monkeypatch`. `tests/conftest.py` pins `TZ=Pacific/Auckland` because dates are derived from epochs at projection time and the two dev hosts are 19h apart.

## Workflow

- **No PRs.** Work lands on `main`. Handoffs get a worktree at `.wt/handoff/<name>/` on branch `handoff/<name>`, reviewed file-by-file, then `git merge --ff-only`.
- **All three gates, every time, before declaring anything done:**
  ```bash
  uv run ruff check fireflies_meetings tests
  uv run basedpyright fireflies_meetings tests   # 0 errors, 0 warnings, 0 notes
  uv run pytest -x -q                            # 165 passing
  ```
- **Then restart and smoke-check** the live service before committing. The service is an editable uv install, so a restart picks up code changes.
- Give real estimates. Padded ones get called out.

## Operations

```bash
systemctl --user status fireflies-meetings
journalctl --user -u fireflies-meetings -n 50 --no-pager

# Force refresh without restart (resets list freshness, backoff, fatal-auth flag).
# Note: pgrep is unreliable for this unit; use MainPID.
kill -USR1 $(systemctl --user show -p MainPID fireflies-meetings | cut -d= -f2)

ls ~/.cache/fireflies-meetings/            # list.json, channels.json, meetings/, raw/
```

Never run `fireflies-meetings mount` by hand while the service owns the mountpoint — it just starts a second, failing mount. `systemctl --user stop` first.

## Current state (2026-08-28)

- **The service is `failed` / stopped**, deliberately shut down over memory (~1.5 GB steady, 2.1 GB peak, 438 MB swap). Restart did not fix it. This is the top open problem.
- **`ba634ad` (media URLs in `meeting.json`) is inert** until the service restarts — no cached detail has the new fields yet. Code on `main` ≠ code running.
- Residual ~25–31% CPU is traced to `sync_active_meeting_ids` emitting `ListRefreshed` every 30s with no diff, forcing a full O(N) rebuild twice a minute.
- An architecture review (port to the Rust `vfsd` engine vs refactor pyfuse3 in place) was agreed but never held. `fuse_ops.py` is on the chopping block either way.

See [`docs/decision-log.md`](docs/decision-log.md) for the full set of diagnosed-but-unfixed issues and the reasoning behind rejected designs.
