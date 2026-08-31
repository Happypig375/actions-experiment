#!/usr/bin/env python3
"""Dispatch stale public information producers and publish diagnostic telemetry."""

from __future__ import annotations

import base64
import datetime as dt
import json
import os
import pathlib
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

UTC = dt.timezone.utc
API_VERSION = "2022-11-28"
ACTIVE_STATUSES = {"queued", "in_progress", "pending", "requested", "waiting"}
ORDINARY_MAX_AGE_SECONDS = 75 * 60
WATCHDOG_STATUS_FRESH_FOR_SECONDS = 45 * 60


@dataclass(frozen=True)
class Producer:
    name: str
    workflow: str
    branch: str
    timestamp_field: str
    max_age_seconds: int = ORDINARY_MAX_AGE_SECONDS
    cooldown_seconds: int = 15 * 60


PRODUCERS = (
    Producer("GitHub feed", "important-update-github-feed.yml", "chatgpt-important-update-feed", "generated_at"),
    Producer("Public feed", "important-update-public-feed.yml", "chatgpt-important-update-public-feed", "generated_at"),
    Producer("Tibo feed", "important-update-tibo-feed.yml", "chatgpt-important-update-tibo-feed", "generated_at"),
    Producer("Reddit media feed", "important-update-reddit-media-feed.yml", "chatgpt-important-update-reddit-media-feed", "generated_at"),
    Producer("Outage recovery feed", "important-update-outage-recovery.yml", "chatgpt-important-update-backlog-feed", "generated_at"),
)


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


class GitHub:
    def __init__(self, repository: str, token: str, default_branch: str) -> None:
        if "/" not in repository:
            raise ValueError("GITHUB_REPOSITORY must be owner/name")
        self.repository = repository
        self.token = token
        self.default_branch = default_branch
        self.base = f"https://api.github.com/repos/{repository}"

    def request(
        self,
        method: str,
        path: str,
        *,
        body: dict[str, Any] | None = None,
        allow_not_found: bool = False,
    ) -> Any:
        data = None if body is None else json.dumps(body).encode("utf-8")
        req = urllib.request.Request(
            self.base + path,
            data=data,
            method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.token}",
                "User-Agent": "Happypig375-public-info-aggregators-watchdog/1.0",
                "X-GitHub-Api-Version": API_VERSION,
                **({"Content-Type": "application/json"} if data is not None else {}),
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                raw = response.read()
        except urllib.error.HTTPError as exc:
            if allow_not_found and exc.code == 404:
                return None
            detail = exc.read().decode("utf-8", "replace")[:1000]
            raise RuntimeError(f"GitHub API {exc.code} {method} {path}: {detail}") from exc
        return None if not raw else json.loads(raw)

    def branch_document(self, producer: Producer) -> dict[str, Any] | None:
        branch = urllib.parse.quote(producer.branch, safe="")
        result = self.request("GET", f"/contents/index.json?ref={branch}", allow_not_found=True)
        if result is None or not result.get("content"):
            return None
        return json.loads(base64.b64decode(result["content"]).decode("utf-8"))

    def recent_runs(self, producer: Producer) -> list[dict[str, Any]]:
        workflow = urllib.parse.quote(producer.workflow, safe="")
        result = self.request("GET", f"/actions/workflows/{workflow}/runs?per_page=10")
        return list((result or {}).get("workflow_runs") or [])

    def dispatch(self, producer: Producer) -> None:
        workflow = urllib.parse.quote(producer.workflow, safe="")
        self.request(
            "POST",
            f"/actions/workflows/{workflow}/dispatches",
            body={"ref": self.default_branch},
        )


def read_heartbeat() -> dict[str, Any] | None:
    path = pathlib.Path(".github/state/important-update-heartbeat.json")
    if not path.is_file():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def write_status(document: dict[str, Any]) -> None:
    target = pathlib.Path(
        os.environ.get("WATCHDOG_STATUS_DIR", "/tmp/important-update-watchdog-status")
    )
    target.mkdir(parents=True, exist_ok=True)
    (target / "index.json").write_text(json.dumps(document, indent=2), encoding="utf-8")


def append_summary(rows: list[dict[str, Any]], checked_at: dt.datetime) -> None:
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return
    with open(summary_path, "a", encoding="utf-8") as summary:
        summary.write("## Public information feed watchdog\n\n")
        summary.write(f"Checked `{iso(checked_at)}`.\n\n")
        summary.write("| Producer | Age | Threshold | Action |\n")
        summary.write("|---|---:|---:|---|\n")
        for row in rows:
            age = row.get("age_seconds")
            age_text = "missing" if age is None else f"{age // 60} min"
            threshold = f"{row.get('max_age_seconds', 0) // 60} min"
            action = str(row.get("action", "unknown"))
            if row.get("error"):
                action += f": {row['error']}"
            summary.write(f"| {row['producer']} | {age_text} | {threshold} | {action} |\n")


def main() -> int:
    repository = os.environ.get("GITHUB_REPOSITORY", "")
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN") or ""
    default_branch = os.environ.get("DEFAULT_BRANCH", "master")
    current_workflow = os.environ.get("CURRENT_WORKFLOW_FILE", "")
    dry_run = os.environ.get("WATCHDOG_DRY_RUN", "").lower() in {"1", "true", "yes"}
    if not repository or not token:
        print("GITHUB_REPOSITORY and GH_TOKEN/GITHUB_TOKEN are required", file=sys.stderr)
        return 2

    github = GitHub(repository, token, default_branch)
    now = dt.datetime.now(UTC)
    rows: list[dict[str, Any]] = []
    failures = 0
    dispatch_count = 0

    for producer in PRODUCERS:
        row: dict[str, Any] = {
            "producer": producer.name,
            "workflow": producer.workflow,
            "branch": producer.branch,
        }
        try:
            document = github.branch_document(producer)
            published = parse_time(document.get(producer.timestamp_field) if document else None)
            age_seconds = None if published is None else max(0, int((now - published).total_seconds()))
            stale = age_seconds is None or age_seconds > producer.max_age_seconds
            row.update(
                {
                    "published_at": iso(published) if published else None,
                    "age_seconds": age_seconds,
                    "max_age_seconds": producer.max_age_seconds,
                    "stale": stale,
                }
            )
            if not stale:
                row["action"] = "fresh"
                rows.append(row)
                continue
            if producer.workflow == current_workflow:
                row["action"] = "skip-current-workflow"
                rows.append(row)
                continue

            runs = github.recent_runs(producer)
            active = next((run for run in runs if run.get("status") in ACTIVE_STATUSES), None)
            if active:
                row.update(
                    {
                        "action": "already-active",
                        "run_id": active.get("id"),
                        "run_status": active.get("status"),
                    }
                )
                rows.append(row)
                continue

            latest = runs[0] if runs else None
            latest_created = parse_time(latest.get("created_at") if latest else None)
            if latest_created and (now - latest_created).total_seconds() < producer.cooldown_seconds:
                row.update(
                    {
                        "action": "cooldown",
                        "run_id": latest.get("id"),
                        "run_status": latest.get("status"),
                        "run_conclusion": latest.get("conclusion"),
                    }
                )
                rows.append(row)
                continue

            if dry_run:
                row["action"] = "would-dispatch"
            else:
                github.dispatch(producer)
                row["action"] = "dispatched"
                dispatch_count += 1
                print(f"::notice::{producer.name} was stale; dispatched {producer.workflow}")
        except Exception as exc:  # Keep one bad producer from blocking the others.
            failures += 1
            row.update({"action": "error", "error": str(exc)})
            print(f"::warning::{producer.name} watchdog check failed: {exc}")
        rows.append(row)

    run_id = os.environ.get("GITHUB_RUN_ID")
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    status = {
        "schema_version": 3,
        "purpose": "Public information feed watchdog telemetry",
        "checked_at": iso(now),
        "fresh_for_seconds": WATCHDOG_STATUS_FRESH_FOR_SECONDS,
        "repository": repository,
        "default_branch": default_branch,
        "trigger": {
            "event_name": os.environ.get("GITHUB_EVENT_NAME"),
            "ref": os.environ.get("GITHUB_REF"),
            "sha": os.environ.get("GITHUB_SHA"),
            "actor": os.environ.get("GITHUB_ACTOR"),
            "run_id": run_id,
            "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
            "run_url": f"{server}/{repository}/actions/runs/{run_id}" if run_id else None,
        },
        "heartbeat": read_heartbeat(),
        "dry_run": dry_run,
        "dispatch_count": dispatch_count,
        "failure_count": failures,
        "all_fresh": all(row.get("action") == "fresh" for row in rows),
        "producers": rows,
    }
    write_status(status)
    print(json.dumps(status, indent=2))
    append_summary(rows, now)
    return 0 if failures < len(PRODUCERS) else 1


if __name__ == "__main__":
    raise SystemExit(main())
