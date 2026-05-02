# fireflies-meetings-fuse

Read-only FUSE filesystem exposing your [Fireflies.ai](https://fireflies.ai/) meetings as a date-indexed, grep-able tree of markdown files.

```
/views/fireflies-meetings/
├── 2026-04/02/team-leads-intro-focus/
│   ├── summary.md       # AI summary, action items, keywords
│   ├── transcript.md    # full transcript with [MM:SS] timestamps
│   ├── participants.md  # speaker list with computed talk-time %
│   ├── meeting.json     # raw structured data
│   └── open.sh          # opens the meeting in your browser
├── live/                # symlinks to currently-live meetings
└── mine/                # same date tree, filtered to meetings YOU organized
```

Once mounted, your entire meeting history is just files you can `rg`, `cat`, `bat`, glob, or feed to any tool that takes a path.

## Why

The Fireflies web UI and the GraphQL API are great when you know what you're looking for, but they're a poor fit for "I remember someone mentioning X three weeks ago". A FUSE mount turns the whole archive into something you can pipe through `ripgrep` in 50ms.

The tree is also a perfect substrate for AI agents — point Claude Code or Cursor at `/views/fireflies-meetings/` and they can answer "find the meeting where we discussed the auth migration" without ever calling an API.

## Requirements

- Linux with FUSE 3 (`fusermount3`)
- Python ≥ 3.12
- [`uv`](https://github.com/astral-sh/uv) (recommended) or any PEP 517 builder
- A Fireflies.ai account with API access

## Install

```bash
git clone https://github.com/synap5e/fireflies-meetings-fuse.git
cd fireflies-meetings-fuse
uv sync
```

Get a Fireflies API key from <https://app.fireflies.ai/integrations/custom/fireflies>, then:

```bash
cp .env.example .env
$EDITOR .env  # paste your key
```

Try it out manually before installing as a service:

```bash
mkdir -p /views/fireflies-meetings
uv run fireflies-meetings mount /views/fireflies-meetings
```

In another terminal:

```bash
ls /views/fireflies-meetings/
rg "auth migration" /views/fireflies-meetings/
```

`Ctrl-C` to unmount, or from another shell:

```bash
uv run fireflies-meetings unmount /views/fireflies-meetings
```

## Run as a systemd user service

A unit file ships in the repo. The defaults assume the repo lives at `~/agentic/fireflies-meetings-fuse/` and your `.env` is alongside it. Edit `WorkingDirectory`, `EnvironmentFile`, and `ExecStart` if you installed elsewhere.

```bash
cp fireflies-meetings.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now fireflies-meetings
systemctl --user status fireflies-meetings
```

The service auto-mounts at `/views/fireflies-meetings/`, restarts on failure, and pre-unmounts any stale mount before starting.

## Live Transcript Fallback Auth

Live meetings are awkward because Fireflies' public `transcript.sentences` field often fails with `INTERNAL_SERVER_ERROR` while a meeting is in progress. This project therefore has an internal web-app fallback, but that fallback needs the same short-lived browser session that `app.fireflies.ai` uses.

When that session is available, the mount first tries internal GraphQL polling and also starts an internal Socket.IO live-transcript stream for active meetings. The stream improves freshness, while the reopen path still forces a fresh detail fetch for live files so missed stream events do not leave the transcript stale.

The easiest way to refresh it is:

```bash
uv run fireflies-meetings auth-session
```

That command reads the current Chrome session cookies for `app.fireflies.ai`, writes them to `~/.config/fireflies-meetings/session.json`, and restarts the user service if it is running. If the browser session is stale, it opens Fireflies' login page and waits for you to sign in.

You can still configure the tokens manually via env vars or JSON if you prefer, but `auth-session` is the supported path.

The daemon also does a best-effort non-interactive refresh from your local browser cookies at startup. If Chrome/Chromium already has a valid Fireflies session and the desktop keyring is available, the service refreshes `session.json` itself without opening a browser. You can override the browser/profile with `FIREFLIES_SESSION_BROWSER` and `FIREFLIES_SESSION_PROFILE`.

## Filesystem layout

```
/views/fireflies-meetings/
├── YYYY-MM/                   # one dir per month
│   └── DD/                    # one dir per day
│       └── <meeting-slug>/    # slug derived from the meeting title
│           ├── summary.md
│           ├── transcript.md
│           ├── participants.md
│           ├── meeting.json
│           └── open.sh        # executable, runs xdg-open on the transcript URL
├── live/                      # contains a symlink per currently-live meeting,
│                              # named by slug, → ../YYYY-MM/DD/<slug>
└── mine/                      # entire date tree filtered to meetings you organized
                               # (only appears if FIREFLIES_USER_EMAIL is configured)
```

While a meeting is still being processed by Fireflies, an extra `_in_progress` file appears in its directory. The other files render whatever's available so far and re-fetch every 60 seconds.

Live transcript files are optimized for reopening, not long-held file descriptors. Fresh opens (`cat`, `bat`, editors reopening the file) pick up the latest content, but `tail -f` is not reliable yet because it keeps one handle open while the live transcript is refreshed behind the scenes.

If the API rejects your token (401/403), an `AUTHENTICATION_EXPIRED` file appears at the mount root with recovery instructions and all background fetches stop until the service is restarted.

## Searching

```bash
# Find any meeting that mentions "kubernetes"
rg kubernetes /views/fireflies-meetings/

# Just my meetings
rg kubernetes /views/fireflies-meetings/mine/

# Just summaries (skip transcripts)
rg kubernetes /views/fireflies-meetings/**/summary.md

# Meetings from last week
ls /views/fireflies-meetings/2026-04/0{2..8}/
```

## Caching & backfill

State lives under `${XDG_CACHE_HOME:-~/.cache}/fireflies-meetings/`:

```
~/.cache/fireflies-meetings/
├── list.json              # cached meeting list, reloaded on startup
├── detail/<meeting_id>/   # rendered files for completed meetings (served from disk)
└── completed/<meeting_id> # empty marker — "fully processed, never re-fetch"
```

How fetching works:

- **List**: refreshed every ~30 min (±30% jitter). Initial fetch pages through everything; subsequent refreshes only pull page 1 and merge, so older meetings are preserved.
- **Detail**: lazily fetched on first file access. Completed meetings are written to `detail/<id>/` and served from disk forever after. In-progress / live meetings re-fetch every 60s.
- **Background backfill**: a trio task wakes 5 seconds after mount and walks the list of un-cached meetings at ~20 fetches/min, persisting each completed meeting to disk. This is why the cache fills in gradually after a fresh mount.
- **Backoff**: API failures trigger exponential backoff (30s → 15min, ±25% jitter). 429s honor `x-ratelimit-reset-api`. 401/403 set a fatal flag and stop all retries until restart.

To start fresh:

```bash
systemctl --user stop fireflies-meetings
rm -rf ~/.cache/fireflies-meetings
systemctl --user start fireflies-meetings
```

## Operations

```bash
# Logs
journalctl --user -u fireflies-meetings -n 50 --no-pager

# Force a full cache refresh without restarting (clears list cache, backoff,
# and any non-completed file caches; completed meetings stay served from disk)
kill -USR1 $(pgrep -f 'fireflies-meetings mount')

# Stale-mount recovery
fusermount3 -u /views/fireflies-meetings && systemctl --user restart fireflies-meetings

# Manual debug mount
uv run fireflies-meetings mount --debug
```

## Configuration

Session auth can come from either `~/.config/fireflies-meetings/session.json` or env vars in `.env` (loaded by systemd via `EnvironmentFile=`).

| Variable | Required | Purpose |
|---|---|---|
| `FIREFLIES_API_KEY` | ✓ | Bearer token for the Fireflies GraphQL API |
| `FIREFLIES_USER_EMAIL` |   | Enables the `mine/` subtree. If unset, the service queries the API at startup to resolve it; if that also fails, `mine/` is hidden |
| `FIREFLIES_SESSION_TOKEN` |   | Access token for the Fireflies web app's internal fallback API |
| `FIREFLIES_REFRESH_TOKEN` |   | Refresh token paired with `FIREFLIES_SESSION_TOKEN`; improves live-caption fallback fidelity |
| `FIREFLIES_AUTH_PROVIDER` |   | Internal auth provider label for the web-session fallback. Default: `gauth` |

The CLI also accepts `--api-key <path>` to read the key from a file (default `~/.config/fireflies-meetings/api_key`) and `--session-auth <path>` to override the web-session JSON path. Env vars win if both are present.

## Development

```bash
uv sync
uv run pytest -x -q
uv run ruff check fireflies_meetings tests
uv run basedpyright fireflies_meetings tests
```

All three should be clean before sending a PR. The codebase follows a Pydantic-at-I/O-boundaries convention — see [`CLAUDE.md`](CLAUDE.md) for the conventions and architecture overview.

## License

[AGPL-3.0-or-later](LICENSE). If you run a modified version as a network-accessible service, the AGPL requires you to make your modifications available to its users.

## Status & disclaimer

Unofficial. Not affiliated with or endorsed by Fireflies.ai. Built against the public Fireflies GraphQL API; if they break it, this will too.
