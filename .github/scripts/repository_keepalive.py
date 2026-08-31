#!/usr/bin/env python3
"""Refresh a small default-branch state file before public schedules reach 60 days idle."""

from __future__ import annotations

import datetime as dt
import json
import os
import pathlib
from typing import Any

UTC = dt.timezone.utc
DEFAULT_THRESHOLD_DAYS = 28


def parse_time(value: Any) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def should_refresh(
    previous: dt.datetime | None,
    now: dt.datetime,
    threshold: dt.timedelta,
    *,
    force: bool = False,
) -> bool:
    return force or previous is None or now - previous >= threshold


def main() -> int:
    path = pathlib.Path(
        os.environ.get(
            "KEEPALIVE_STATE_PATH",
            ".github/state/repository-keepalive.json",
        )
    )
    threshold_days = int(os.environ.get("KEEPALIVE_THRESHOLD_DAYS", DEFAULT_THRESHOLD_DAYS))
    force = os.environ.get("KEEPALIVE_FORCE", "").lower() in {"1", "true", "yes"}
    now = dt.datetime.now(UTC)

    previous_document: dict[str, Any] = {}
    if path.is_file():
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                previous_document = loaded
        except (OSError, json.JSONDecodeError):
            pass

    previous = parse_time(previous_document.get("committed_at"))
    changed = should_refresh(
        previous,
        now,
        dt.timedelta(days=threshold_days),
        force=force,
    )

    document = previous_document
    if changed:
        path.parent.mkdir(parents=True, exist_ok=True)
        document = {
            "schema_version": 1,
            "purpose": "Default-branch activity marker for GitHub's public scheduled-workflow inactivity rule",
            "committed_at": iso(now),
            "repository": os.environ.get("GITHUB_REPOSITORY"),
            "threshold_days": threshold_days,
            "trigger": os.environ.get("GITHUB_EVENT_NAME"),
        }
        path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")

    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a", encoding="utf-8") as handle:
            handle.write(f"changed={'true' if changed else 'false'}\n")
            handle.write(f"previous_at={iso(previous) if previous else ''}\n")
            handle.write(f"current_at={document.get('committed_at', '')}\n")

    print(
        json.dumps(
            {
                "changed": changed,
                "previous_at": iso(previous) if previous else None,
                "current_at": document.get("committed_at"),
                "threshold_days": threshold_days,
                "force": force,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
