# Decision log

Why the code looks the way it does. Reconstructed from the owner session transcript (2026-04-10 → 2026-08-28) and git history.

Read this before proposing a redesign — most obvious ideas have already been tried, costed, or rejected here for a reason. Conventions and invariants live in [`AGENTS.md`](../AGENTS.md); open work in [`backlog.md`](../backlog.md).

---

## 1. How the storage layer got here

| Date | Shift | Driver |
|---|---|---|
| 2026-04-09 | Initial: `store.MeetingStore` with `_entries` + `_file_cache` + disk sentinels | — |
| 2026-04-09 | FUSE listing made API-free (`5ad611d`) | A full `rg` took 47s and cascaded ENOENT |
| 2026-05-22 | Completion state collapsed to one source of truth (`fe0441f`) | `completed/<id>` marker dir duplicated `detail/<id>/.complete` |
| 2026-07-16 | Storage rewritten as capture + projection + commands (`90d6ec5`) | Ad-hoc merge rules were unreviewable; `readdir` listed files that had no bytes |
| 2026-07-26 | Field-merge rules extracted to `resolver.py` (`57f9c69`) | Precedence was scattered across `_capture_state`, `_merge_refresh_entry`, `_terminal_meeting_ids`, `_looks_abandoned` |
| 2026-07-27 | Every API observation archived to `raw/` (`63dd80c`) | Derived state must be rebuildable; no source's fields may be lost to another's |
| 2026-07-27 → 08-15 | Per-meeting fast path replaces global rebuild (`3aa6526`, `c60adfa`) | One caption cost an O(1780) rebuild; CPU sat at 85–90% |

**Cache layout moved with the rewrite.** `detail/<id>/` → `meetings/<id>/detail.json`. 1563 meetings migrated in ~4s with zero API calls, because the rendered `detail.json` already contained everything (sentences, summary, speakers, attendees, access logs). Legacy trees were kept as `detail.legacy.<ts>`.

**The CQRS name is wrong and known to be wrong.** The owner's own framing: the commands carry "zero user intent — every one of them is `receive external fact → write cache → rebuild projection`." Facts arrive immutable but are stored collapsed to latest-per-resource. It is CDC into a materialized view. The vocabulary in `commands.py` / `tests/test_cqrs.py` is a naming debt, not a design claim.

---

## 2. Rejected designs

Don't re-propose these without new information.

**Storage and data**

- **SQLite/Postgres for the event store.** Per-meeting folds are directory-local, so `listdir` + read beats a SQL round-trip; there are no cross-meeting analytical queries; and "the moment we ship SQL, we lose *just cat the file* debuggability." The only SQLite in the tree is read-only access to Chrome's cookie DB.
- **On-disk projection checkpoint.** Rebuild cost was measured and accepted.
- **File-per-caption event storage.** 4 KB blocks × ~3600 captions/hour = 14–28 MiB per meeting-hour. Chunked JSONL instead.
- **Splitting `ListRefreshed` into N per-meeting events.** 1500 meetings × 30s cadence = 1500 writes every 30s, and it invents an event that doesn't correspond to what the API actually returned.
- **Deleting data on compaction.** Compaction is index-building. No raw file is ever deleted.
- **Capturing raw inside `capture.py`.** By that layer the data is already merged and normalized — page merges, sticky rules, `setdefault`, plus synthesized non-API facts (404 stubs, 12h age-out, `watch_meeting`). Raw must be hooked at the API/socket boundary.
- **Cheap set-diff to find API-dropped meetings.** Structurally uninformative: `list.json` is a cumulative superset, so nothing ever leaves it.

**Filesystem behaviour**

- **`notify_store` to fix cold ENOENT.** It only affects inodes the kernel has already looked up, and in-progress files use `direct_io=True`, which bypasses the page cache entirely. `invalidate_inode` is the right tool. Full-tree FUSE roundtrip overhead measured at ~100 ms — under `rg`'s own parse time.
- **Blocking on first `open()` of an in-progress meeting.** Breaks the never-block-on-read invariant that the ENOENT fix bought.
- **`/live/<slug>` as a real directory.** The symlink is the contract.
- **Hiding zero-duration ghost meetings.** The owner wants them folded into `ghost/`, not suppressed: "just so that we can still see the JSON and stuff."
- **Merging overlapping transcripts into one meeting.** Judged much harder than folding under a primary; the accepted design is fold-under-longest-duration plus `_overlap_warning.md` whenever the parent isn't a strict superset of its children.
- **Naming the overlap fold `reconnect/`.** Reconnection isn't the only cause (multiple bots, platform recording splits).
- **Verbose diagnostics inside rendered files.** Pollutes grep. Minimal `_Transcript pending_` in the file; diagnostics go to `meeting.json` and the top-level `BACKFILL_IN_PROGRESS`.
- **`_control/refetch/<meeting_id>` as literal control paths.** 1780+ meetings and growing; deferred pending a parameterized form.

**Features**

- **Cached `audio.mp3` / `video.mp4` in the tree** (2026-08-28). Costed at ~80 GB for the full archive and 300–500 LOC through `fuse_ops.py`. Cut to four URL fields in `meeting.json` (~20–30 LOC) — shipped as `ba634ad`.
- **Streaming FIFO `/live/<slug>.stream`.** Designed in full (replaying FIFO, per-fh offset, trio wakeups, EOF on completion) then deferred: `tail --debug -f` reported *polling* mode on this mount, so the attr-timeout fix probably suffices, at ~3× the code.
- **Deriving `/channels/` from `channelIds` on the public `transcripts` query.** `_LIST_QUERY` doesn't request it and nothing maps channel id → name.
- **Manual live-meeting registration; becoming a Fireflies admin; Gmail/Slack as the live-URL source.** All declined at the outset. Google Chat is the source of truth.
- **Google Chat app interactive features.** Enabling them forces a Connection-settings webhook URL with no use here.

**Process**

- **Pushing WIP branches to origin.** Solo repo; squash-merge to local `main`, push that.
- **PyO3 or a full Rust rewrite** for the FUSE engine. If the port happens, it is an out-of-process engine: trio is entrenched and browser-session auth stays Python.

---

## 3. Fireflies API notes

Empirically established, mostly the hard way.

**Discovery sources and their semantics**

| Source | Auth | Returns | Limits |
|---|---|---|---|
| public `transcripts` | API key | Finished transcripts only | ~24h lag; the only source of full transcript text |
| public `active_meetings` | API key | Live meeting IDs | `[]` for non-admin accounts — absence proves nothing |
| hive `getChannelMeetings` | session JWT | Meeting list | Fallback only, when the public call errors |
| hive `getUserMeetingsForStatus` | session JWT | Any state, real-time | Also exposes `errorDetail`, `puppetExitReason` |
| Google Chat | OAuth | `live/<id>` URLs, IDs only | Misses meetings the bot never joined |

**Field-level gotchas**

- The list API **never returns `summary_status`**, and reports `is_live=false` for in-progress meetings. It cannot clear live state — the root cause of the stuck-live bug class.
- `getUserMeetingsForStatus` returns `startTime` as **numeric epoch milliseconds**, not ISO. Assuming ISO silently rejected every record.
- `title`: list wins — detail synthesizes a `"Jul 22, HH:MM PM"` placeholder for untitled meetings.
- `date_epoch_ms`: list wins — detail rounds to the scheduled minute.
- `duration_mins`: detail wins if non-zero.
- `participants` conflicts (1376 of 1780) are a **parser bug** in `api.py`, not data: detail returns `['a,b']` where list returns `['a','b']`. Masked by resolver precedence. **Fixed in `0013bb8`** — `api.py` now splits `allEmails` on commas at the source; the ~1376 meetings already cached with the comma-joined value stay that way until refetched.
- `summary_status` has three flavours and provenance must survive: real terminal, synthetic `missing_from_api` from a 404, synthetic from the 12h age-out. The synthetic ones are deliberately reversible.
- `video_url` / `audio_url` exist on the public `Transcript` type. Signed CloudFront URLs, ~4-day TTL, `Expires=` epoch in the query string. Often audio-only (screen-share meetings). No size metadata.
- Calendar placeholders return **200 OK with `dur=0`, `sentences=0`, `summary_status=""`** — and, more recently, with the full *scheduled* duration, which is why zero-duration ghost detection alone stopped working.
- Meeting IDs are ULIDs and encode a creation timestamp — useful for dating records the list API won't.
- `fetchChannelMeetings(channelId="all")` returns a **truncated** set (14 memberships vs 139 when iterated per-channel). Always loop per channel.
- The live Socket.IO stream has **no replay on connect**, and `get_transcript` has no `since:` parameter — so any catch-up probe re-downloads all sentences.

**Transport**

- Cloudflare in front of `api.fireflies.ai` blocks the default `python-httpx/x.y.z` User-Agent with an **HTML 403 interstitial**. This was misdiagnosed as an expired API key for hours. A browser-like UA is required, and 401/403 with an HTML body must map to `TransientAPIError`, not `FatalAPIError`.
- The Google Chat API 404s on **every** endpoint unless a Chat app is registered in the same GCP project as the OAuth client — even for read-only user-context calls. After registering it, the running service still 404'd until restarted, because its access token predated the app.
- Session auth is a Chrome-cookie-derived `x-cache` JWT with ~14-day life. It does not survive a copy between hosts.

---

## 4. Platform gotchas

- **pyfuse3 `attr_timeout`/`entry_timeout` default to 300.0s.** Unset, the kernel cached a live transcript's size for five minutes; the file appeared frozen at 1013 bytes while the real content was 43 KB. Dynamic paths use a 0.0 timeout, static ones 300.0.
- **pyfuse3 emits no inotify events**, so inotify-mode `tail -f` blocks forever.
- **`python-engineio` dispatches each socketio message on a fresh thread** (`run_async=True`). An inline handler that took the store lock leaked 7591 threads and 1.5 GB.
- **glibc heap fragmentation is a real line item.** Dropping a 434 MB field released 0 MB until `malloc_trim(0)` was called after each rebuild. 200–400 MB of the resident set is fragmentation.
- **Parsed pydantic is 3–5× the disk JSON.**
- **journald retains only the current boot for this unit** — pre-restart forensics are unrecoverable.
- **`kill -USR1 $(pgrep …)` is unreliable here**; use `systemctl --user show -p MainPID`.
- **NixOS:** `programs.fuse.userAllowOther = true` is required (the service uses `allow_root`), and `fusermount3` lives at `/run/wrappers/bin/`, which is *not* on the systemd user-service lookup PATH. `Environment=` sets runtime env, not lookup PATH — hence the absolute path in the unit.
- **Chrome on NixOS stores cookies with `v11` keyring encryption** the extractor can't decrypt. Workaround: relaunch with `--password-store=basic` and an isolated profile.
- **`git filter-repo` removes the `origin` remote** as a safety measure.
- **Dates derive from epochs at projection time**, so the tree structure differs between hosts 19h apart. `tests/conftest.py` pins TZ; the production consequence is unaddressed.

---

## 5. Diagnosed, not fixed

Ordered roughly by cost.

1. **Memory floor: ~1.5 GB steady, 2.1 GB peak, 438 MB swap.** Restart doesn't fix it. Breakdown: ~90 MB rendered `transcript.md` bytes, ~25 MB other rendered files, ~40 MB `Projection.nodes` (~40k keys), ~10 MB pydantic models, ~150 MB interpreter/imports, +300–400 MB transient during `_rebuild()`, 200–400 MB glibc fragmentation. Costed option not taken: stop pre-rendering `transcript.md` bytes (~1 hour of work, estimated ~800 MB). **The service is currently stopped over this.**
2. **`sync_active_meeting_ids` emits `ListRefreshed` every 30s with no diff**, forcing a full O(N) rebuild twice a minute. This is the residual ~25–31% CPU. **Fixed in `b24f32b`** — the sync now skips the rebuild unless a meeting newly flips to live; effect lands at the next service restart.
3. **`StatusSupplemented` uses `setdefault`** (`commands.py`) and so never updates an existing status. **Fixed in `1e5f560`** — it now gap-fills empty fields and updates non-terminal status on an already-known meeting, instead of a no-op.
4. **Chat-watcher 404 retry storm.** `watch_meeting` swallows `TranscriptNotFoundError` without recording anything, so a Chat-discovered ID that 404s is retried every ~55s for the full 7-day lookback (~11k pointless requests per stuck ID). Fix sketch: negative cache with TTL. See `backlog.md`.
5. **`participants` parser bug in `api.py`** — comma-joined single-element list from the detail endpoint (§3).
6. **Hive-auth-missing cascade:** missing session auth ⇒ `access_logs` FAILED ⇒ the resolver never promotes to `captured` ⇒ backfill never drains. There is no `SESSION_EXPIRED` sentinel for hive the way there is for the API key and Chat token.
7. **`BackfillDiagnostic` fields are dead code** — `_diagnostics` is never written, so `last_poll_attempt: never` in the sentinel is always misleading. **Fixed in `928cf58`** — the class and its sentinel output were deleted outright.
8. **One shared `_BackoffState` across four endpoint kinds.**
9. **`transcript_error` branch in `projection.py`** — zero occurrences across 1780 cached meetings; safe to delete.
10. **Zombie record `01KNMTSHS6…`** (2026-04-07 all-hands) with `date=0`, outside the 100-record status window. Harmless; the date filter keeps it out of `/live/`.
11. **Expired signed media URLs never self-heal**, because captured meetings are immutable-forever in cache. Shipped knowingly with `ba634ad`.

---

## 6. Bug index

The commits the owner calls "the scars" — each is a load-bearing fix worth reading before touching the same area.

| Commit | Bug |
|---|---|
| `5ad611d` | 47s grep + ENOENT cascade; FUSE listing made API-free |
| `0ee114d` | Cloudflare HTML 403 misread as fatal auth failure |
| `b953c72` | Live-meeting state machine: four interacting bugs clobbering `is_live` |
| `27eaa25` | 5xx bypassed backoff; backfill churned at 3s/req through a 24h outage |
| `caa581e` | 404 refetch overwrote real cached transcripts with stubs |
| `060ce95` | `channelId="all"` returned a truncated membership set |
| `84caa9a` | Meetings stuck `_Summary pending_` — list `is_live` can never be cleared by the list |
| `0d0eed7` | Placeholders now carry full scheduled duration; ghost detection missed them |
| `91a78eb` | `BACKFILL_IN_PROGRESS` never drained: empty placeholders retried forever |
| `92039cd` | 7591 leaked threads from per-message socketio dispatch |
| `6ab5a84` | Caption queue dropped *newer* corrections; late captions re-injected after terminal |
| `f375729` | Legacy raw import clobbered `_last_seen` with stale mtimes |
| `c60adfa` | 85–90% CPU: every command JSON-parsed all 1780 detail files |

---

## 7. Working agreements

- No PRs. Handoff worktrees under `.wt/handoff/<name>/`, reviewed file-by-file, `merge --ff-only`.
- Data preservation beats freshness. Never discard a source's fields because another has newer data.
- Rate limits get generous headroom even at the cost of wall-clock ("15 seconds between each fetch, and just accept that it'll take all day").
- Plans of any size get adversarial review by a second model before implementation.
- Real estimates only. Padded ones get called out — a "2 hour" 90-LOC task took 35 minutes.
- `backlog.md` is ideation, not spec. Canonical intent is `AGENTS.md` + `README.md` + recent commit messages.
