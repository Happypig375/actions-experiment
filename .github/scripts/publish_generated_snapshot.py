#!/usr/bin/env python3
"""Publish a generated directory to an orphan branch without timestamp rollback.

Generated Important Update branches are snapshots rather than normal history. Multiple
scheduled/manual/watchdog runs can nevertheless overlap or finish out of order. This
publisher builds an orphan snapshot commit, compares its embedded timestamp with the
currently published snapshot, and updates the branch with force-with-lease retries.
An older or equal candidate is skipped instead of rolling the branch backward.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass
from typing import Any, Sequence

UTC = dt.timezone.utc


class PublishError(RuntimeError):
    """Raised when a generated snapshot cannot be published safely."""


@dataclass(frozen=True)
class PublishResult:
    action: str
    branch: str
    candidate_timestamp: str
    current_timestamp: str | None
    candidate_commit: str
    attempts: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "branch": self.branch,
            "candidate_timestamp": self.candidate_timestamp,
            "current_timestamp": self.current_timestamp,
            "candidate_commit": self.candidate_commit,
            "attempts": self.attempts,
        }


def run(
    args: Sequence[str],
    *,
    cwd: pathlib.Path,
    check: bool = True,
    capture: bool = True,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(args),
        cwd=cwd,
        check=check,
        text=True,
        capture_output=capture,
    )


def parse_timestamp(value: Any) -> dt.datetime:
    if isinstance(value, bool) or value is None:
        raise ValueError(f"invalid timestamp: {value!r}")
    if isinstance(value, (int, float)):
        return dt.datetime.fromtimestamp(float(value), tz=UTC)
    text = str(value).strip()
    if not text:
        raise ValueError("timestamp is empty")
    if text.isdigit():
        return dt.datetime.fromtimestamp(float(text), tz=UTC)
    parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def extract_field(document: Any, dotted_path: str) -> Any:
    value = document
    for segment in dotted_path.split("."):
        if not segment:
            raise ValueError(f"invalid dotted field path: {dotted_path!r}")
        if not isinstance(value, dict) or segment not in value:
            raise KeyError(f"field {dotted_path!r} is missing at {segment!r}")
        value = value[segment]
    return value


def read_metadata(path: pathlib.Path, timestamp_field: str) -> tuple[str, dt.datetime]:
    document = json.loads(path.read_text(encoding="utf-8"))
    raw = extract_field(document, timestamp_field)
    return str(raw), parse_timestamp(raw)


def copy_snapshot(source_dir: pathlib.Path, destination: pathlib.Path) -> None:
    if not source_dir.is_dir():
        raise PublishError(f"source directory does not exist: {source_dir}")
    entries = list(source_dir.iterdir())
    if not entries:
        raise PublishError(f"source directory is empty: {source_dir}")
    for entry in entries:
        if entry.name == ".git":
            continue
        target = destination / entry.name
        if entry.is_dir():
            shutil.copytree(entry, target, symlinks=True)
        else:
            shutil.copy2(entry, target, follow_symlinks=False)


def fetch_remote_head(repo_root: pathlib.Path, remote: str, branch: str) -> str | None:
    remote_ref = f"refs/remotes/{remote}/{branch}"
    run(["git", "update-ref", "-d", remote_ref], cwd=repo_root, check=False)
    fetched = run(
        [
            "git",
            "fetch",
            "--quiet",
            "--force",
            remote,
            f"refs/heads/{branch}:{remote_ref}",
        ],
        cwd=repo_root,
        check=False,
    )
    if fetched.returncode != 0:
        return None
    resolved = run(
        ["git", "rev-parse", "--verify", remote_ref],
        cwd=repo_root,
        check=False,
    )
    return resolved.stdout.strip() or None if resolved.returncode == 0 else None


def read_commit_metadata(
    repo_root: pathlib.Path,
    commit_sha: str,
    metadata_file: str,
    timestamp_field: str,
) -> tuple[str, dt.datetime] | None:
    shown = run(
        ["git", "show", f"{commit_sha}:{metadata_file}"],
        cwd=repo_root,
        check=False,
    )
    if shown.returncode != 0:
        return None
    try:
        document = json.loads(shown.stdout)
        raw = extract_field(document, timestamp_field)
        return str(raw), parse_timestamp(raw)
    except (ValueError, KeyError, json.JSONDecodeError) as exc:
        raise PublishError(
            f"published {metadata_file} on {commit_sha} has no valid "
            f"{timestamp_field}: {exc}"
        ) from exc


def build_orphan_commit(
    repo_root: pathlib.Path,
    source_dir: pathlib.Path,
    commit_message: str,
) -> tuple[str, pathlib.Path, str]:
    temp_root = pathlib.Path(tempfile.mkdtemp(prefix="generated-snapshot-"))
    worktree = temp_root / "worktree"
    branch_name = f"generated-snapshot-{uuid.uuid4().hex}"
    try:
        run(["git", "worktree", "add", "--detach", str(worktree), "HEAD"], cwd=repo_root)
        run(["git", "switch", "--orphan", branch_name], cwd=worktree)
        run(["git", "rm", "-rf", "."], cwd=worktree, check=False)
        copy_snapshot(source_dir, worktree)
        run(["git", "add", "-A"], cwd=worktree)
        run(["git", "config", "user.name", "github-actions[bot]"], cwd=worktree)
        run(
            [
                "git",
                "config",
                "user.email",
                "41898282+github-actions[bot]@users.noreply.github.com",
            ],
            cwd=worktree,
        )
        run(["git", "commit", "-m", commit_message], cwd=worktree)
        commit_sha = run(["git", "rev-parse", "HEAD"], cwd=worktree).stdout.strip()
        return commit_sha, temp_root, branch_name
    except Exception:
        run(["git", "worktree", "remove", "--force", str(worktree)], cwd=repo_root, check=False)
        run(["git", "branch", "-D", branch_name], cwd=repo_root, check=False)
        shutil.rmtree(temp_root, ignore_errors=True)
        raise


def cleanup_orphan(repo_root: pathlib.Path, temp_root: pathlib.Path, branch_name: str) -> None:
    worktree = temp_root / "worktree"
    run(["git", "worktree", "remove", "--force", str(worktree)], cwd=repo_root, check=False)
    run(["git", "branch", "-D", branch_name], cwd=repo_root, check=False)
    run(["git", "worktree", "prune"], cwd=repo_root, check=False)
    shutil.rmtree(temp_root, ignore_errors=True)


def publish_snapshot(
    *,
    repo_root: pathlib.Path,
    source_dir: pathlib.Path,
    branch: str,
    metadata_file: str,
    timestamp_field: str,
    commit_message: str,
    remote: str = "origin",
    max_attempts: int = 4,
    dry_run: bool = False,
) -> PublishResult:
    repo_root = repo_root.resolve()
    source_dir = source_dir.resolve()
    metadata_path = source_dir / metadata_file
    if not metadata_path.is_file():
        raise PublishError(f"metadata file does not exist: {metadata_path}")
    candidate_raw, candidate_time = read_metadata(metadata_path, timestamp_field)
    candidate_sha, temp_root, local_branch = build_orphan_commit(
        repo_root, source_dir, commit_message
    )

    try:
        last_current_raw: str | None = None
        for attempt in range(1, max_attempts + 1):
            expected = fetch_remote_head(repo_root, remote, branch)
            current = (
                read_commit_metadata(
                    repo_root,
                    expected,
                    metadata_file,
                    timestamp_field,
                )
                if expected
                else None
            )
            if current:
                current_raw, current_time = current
                last_current_raw = current_raw
                if candidate_time <= current_time:
                    return PublishResult(
                        action="skipped-not-newer",
                        branch=branch,
                        candidate_timestamp=candidate_raw,
                        current_timestamp=current_raw,
                        candidate_commit=candidate_sha,
                        attempts=attempt,
                    )

            if dry_run:
                return PublishResult(
                    action="would-publish",
                    branch=branch,
                    candidate_timestamp=candidate_raw,
                    current_timestamp=last_current_raw,
                    candidate_commit=candidate_sha,
                    attempts=attempt,
                )

            lease = f"refs/heads/{branch}:{expected or ''}"
            pushed = run(
                [
                    "git",
                    "push",
                    f"--force-with-lease={lease}",
                    remote,
                    f"{candidate_sha}:refs/heads/{branch}",
                ],
                cwd=repo_root,
                check=False,
            )
            if pushed.returncode == 0:
                return PublishResult(
                    action="published",
                    branch=branch,
                    candidate_timestamp=candidate_raw,
                    current_timestamp=last_current_raw,
                    candidate_commit=candidate_sha,
                    attempts=attempt,
                )

            print(
                f"Publication lease changed during attempt {attempt}; "
                "refreshing before retry.",
                file=sys.stderr,
            )
            time.sleep(attempt)

        raise PublishError(
            f"could not publish {branch} without overwriting a concurrent newer writer"
        )
    finally:
        cleanup_orphan(repo_root, temp_root, local_branch)


def append_summary(result: PublishResult) -> None:
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return
    with open(summary_path, "a", encoding="utf-8") as summary:
        summary.write("## Generated snapshot publication\n\n")
        summary.write(f"- Branch: `{result.branch}`\n")
        summary.write(f"- Action: `{result.action}`\n")
        summary.write(f"- Candidate timestamp: `{result.candidate_timestamp}`\n")
        if result.current_timestamp:
            summary.write(f"- Previous timestamp: `{result.current_timestamp}`\n")
        summary.write(f"- Attempts: `{result.attempts}`\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--branch", required=True)
    parser.add_argument("--metadata-file", default="index.json")
    parser.add_argument("--timestamp-field", required=True)
    parser.add_argument("--message", required=True)
    parser.add_argument("--remote", default="origin")
    parser.add_argument("--max-attempts", type=int, default=4)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = pathlib.Path(
        run(["git", "rev-parse", "--show-toplevel"], cwd=pathlib.Path.cwd()).stdout.strip()
    )
    try:
        result = publish_snapshot(
            repo_root=repo_root,
            source_dir=pathlib.Path(args.source_dir),
            branch=args.branch,
            metadata_file=args.metadata_file,
            timestamp_field=args.timestamp_field,
            commit_message=args.message,
            remote=args.remote,
            max_attempts=args.max_attempts,
            dry_run=args.dry_run,
        )
    except (PublishError, ValueError, KeyError, json.JSONDecodeError) as exc:
        print(f"::error::{exc}", file=sys.stderr)
        return 1
    print(json.dumps(result.as_dict(), indent=2))
    append_summary(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
