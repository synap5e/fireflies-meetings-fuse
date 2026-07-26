#!/usr/bin/env python3
"""Generate the checked-in resolver corpus from the current capture cache."""

from __future__ import annotations

import json
from pathlib import Path

FIXTURE_IDS = (
    "01KV9E2A67XNMESD5AKP5KATSH",
    "01KRYRZ3XSDW563YA8G7ZBEW33",
    "01KFKFT8F5E88FYQ6QR9R1TXXA",
    "01KX4N9PA70E4M874RDG13X92P",
    "01KEJ5VXZVD9D5A0EM59C6YA96",
    "01KY5KYMJV24A6JXE283MJ3SGM",
    "01KS8VAT08HQXHX43CXV6WP6K5",
    "01KNSM06V695H1PCP3YXKSECHJ",
)


def main() -> None:
    cache = Path("~/.cache/fireflies-meetings").expanduser()
    target = Path(__file__).parents[1] / "tests" / "fixtures" / "resolver"
    target.mkdir(parents=True, exist_ok=True)
    list_capture = json.loads((cache / "list.json").read_text())
    meetings = {meeting["id"]: meeting for meeting in list_capture["meetings"]}
    for meeting_id in FIXTURE_IDS:
        meeting_dir = cache / "meetings" / meeting_id
        detail = json.loads((meeting_dir / "detail.json").read_text())
        # Conflict tests need the real meeting observations, but not entire
        # private transcripts. Three genuine rows exercise sentence parsing.
        detail["sentences"] = detail.get("sentences", [])[:3]
        detail["speakers"] = []
        detail["summary"] = None
        detail["attendees"] = []
        access_path = meeting_dir / "access_logs.json"
        access_logs = json.loads(access_path.read_text()) if access_path.is_file() else None
        fixture = {
            "meeting_id": meeting_id,
            "list_meeting": meetings[meeting_id],
            "detail": detail,
            "access_logs": access_logs,
        }
        (target / f"{meeting_id}.json").write_text(
            json.dumps(fixture, ensure_ascii=False, indent=2) + "\n",
        )


if __name__ == "__main__":
    main()
