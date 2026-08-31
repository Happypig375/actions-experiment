#!/usr/bin/env python3
"""Regression tests for public-feed publication, watchdog, and keepalive primitives."""

from __future__ import annotations

import datetime as dt
import importlib.util
import json
import pathlib
import subprocess
import sys
import tempfile
import unittest

SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parents[1]


def load(name: str, path: pathlib.Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


publisher = load("publish_generated_snapshot", SCRIPT_DIR / "publish_generated_snapshot.py")
watchdog = load("important_update_feed_watchdog", SCRIPT_DIR / "important_update_feed_watchdog.py")
keepalive = load("repository_keepalive", SCRIPT_DIR / "repository_keepalive.py")


def git(args: list[str], cwd: pathlib.Path) -> str:
    return subprocess.check_output(["git", *args], cwd=cwd, text=True).strip()


class TimestampTests(unittest.TestCase):
    def test_iso_and_epoch_timestamps(self) -> None:
        iso = publisher.parse_timestamp("2026-08-29T02:24:44Z")
        epoch = publisher.parse_timestamp(int(iso.timestamp()))
        self.assertEqual(iso, epoch)

    def test_nested_field(self) -> None:
        self.assertEqual(
            publisher.extract_field({"outer": {"when": "x"}}, "outer.when"),
            "x",
        )


class WatchdogConfigurationTests(unittest.TestCase):
    def test_public_producers_and_contracts(self) -> None:
        self.assertEqual(watchdog.ORDINARY_MAX_AGE_SECONDS, 75 * 60)
        self.assertEqual(watchdog.WATCHDOG_STATUS_FRESH_FOR_SECONDS, 45 * 60)
        self.assertEqual(len(watchdog.PRODUCERS), 5)
        self.assertTrue(
            all(producer.max_age_seconds == 75 * 60 for producer in watchdog.PRODUCERS)
        )
        self.assertFalse(
            any("codex-usage" in producer.workflow for producer in watchdog.PRODUCERS)
        )

        workflows = [
            "important-update-github-feed.yml",
            "important-update-public-feed.yml",
            "important-update-tibo-feed.yml",
            "important-update-reddit-media-feed.yml",
            "important-update-outage-recovery.yml",
        ]
        for name in workflows:
            text = (ROOT / ".github" / "workflows" / name).read_text(encoding="utf-8")
            self.assertRegex(text, r"fresh_for_seconds(?:'|\")?\s*:\s*4500")
            self.assertNotRegex(text, r"fresh_for_seconds(?:'|\")?\s*:\s*7200")

    def test_dispatch_defaults_to_master(self) -> None:
        client = watchdog.GitHub("owner/repository", "token", "master")
        self.assertEqual(client.default_branch, "master")


class KeepaliveTests(unittest.TestCase):
    def test_missing_marker_refreshes(self) -> None:
        now = dt.datetime(2026, 8, 31, tzinfo=dt.timezone.utc)
        self.assertTrue(keepalive.should_refresh(None, now, dt.timedelta(days=28)))

    def test_marker_refreshes_at_threshold(self) -> None:
        now = dt.datetime(2026, 8, 31, tzinfo=dt.timezone.utc)
        self.assertFalse(
            keepalive.should_refresh(now - dt.timedelta(days=27), now, dt.timedelta(days=28))
        )
        self.assertTrue(
            keepalive.should_refresh(now - dt.timedelta(days=28), now, dt.timedelta(days=28))
        )

    def test_force_refreshes_recent_marker(self) -> None:
        now = dt.datetime(2026, 8, 31, tzinfo=dt.timezone.utc)
        self.assertTrue(
            keepalive.should_refresh(
                now,
                now,
                dt.timedelta(days=28),
                force=True,
            )
        )


class MonotonicPublicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = pathlib.Path(self.temp.name)
        self.remote = root / "remote.git"
        self.repo = root / "repo"
        self.source = root / "source"
        subprocess.run(["git", "init", "--bare", str(self.remote)], check=True, capture_output=True)
        subprocess.run(["git", "init", "-b", "master", str(self.repo)], check=True, capture_output=True)
        subprocess.run(["git", "config", "user.name", "test"], cwd=self.repo, check=True)
        subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=self.repo, check=True)
        (self.repo / "README.md").write_text("test\n", encoding="utf-8")
        subprocess.run(["git", "add", "README.md"], cwd=self.repo, check=True)
        subprocess.run(["git", "commit", "-m", "initial"], cwd=self.repo, check=True, capture_output=True)
        subprocess.run(["git", "remote", "add", "origin", str(self.remote)], cwd=self.repo, check=True)
        subprocess.run(["git", "push", "-u", "origin", "master"], cwd=self.repo, check=True, capture_output=True)
        self.source.mkdir()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def write_snapshot(self, timestamp: str, value: str) -> None:
        (self.source / "index.json").write_text(
            json.dumps({"generated_at": timestamp, "value": value}),
            encoding="utf-8",
        )
        (self.source / "payload.txt").write_text(value, encoding="utf-8")

    def publish(self):
        return publisher.publish_snapshot(
            repo_root=self.repo,
            source_dir=self.source,
            branch="generated-feed",
            metadata_file="index.json",
            timestamp_field="generated_at",
            commit_message="publish test snapshot",
        )

    def remote_document(self) -> dict[str, str]:
        subprocess.run(
            [
                "git",
                "fetch",
                "--quiet",
                "--force",
                "origin",
                "refs/heads/generated-feed:refs/remotes/origin/generated-feed",
            ],
            cwd=self.repo,
            check=True,
        )
        return json.loads(git(["show", "refs/remotes/origin/generated-feed:index.json"], self.repo))

    def test_older_writer_cannot_roll_branch_backward(self) -> None:
        self.write_snapshot("2026-08-29T02:00:00Z", "first")
        first = self.publish()
        self.assertEqual(first.action, "published")

        self.write_snapshot("2026-08-29T01:00:00Z", "older")
        older = self.publish()
        self.assertEqual(older.action, "skipped-not-newer")
        self.assertEqual(self.remote_document()["value"], "first")

        self.write_snapshot("2026-08-29T03:00:00Z", "newer")
        newer = self.publish()
        self.assertEqual(newer.action, "published")
        self.assertEqual(self.remote_document()["value"], "newer")


if __name__ == "__main__":
    unittest.main(verbosity=2)
