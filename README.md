# Public information aggregators

This repository runs public-source acquisition and preprocessing jobs for Hadrian Tang's ChatGPT assistant. It keeps recurring public data collection outside private repositories, where GitHub-hosted Actions minutes are metered.

The repository intentionally contains **public sources and sanitized derived data only**. Authenticated or personal feeds—currently the Codex account-usage feed—must remain in a private repository or another private execution/storage path.

## Published feeds

Each producer publishes a compact orphan-branch snapshot. Consumers should resolve a branch to an immutable commit SHA first, then read `index.json` and all selected views at that same SHA.

| Feed | Snapshot branch | Main views |
|---|---|---|
| Watched GitHub repositories | `chatgpt-important-update-feed` | `views/nu.json`, `views/angourimath.json` |
| Reddit and Codex-reset public sources | `chatgpt-important-update-public-feed` | `views/reddit.json`, `views/codex_resets.json` |
| Tibo Sottiaux public timeline | `chatgpt-important-update-tibo-feed` | `views/tibo.json` |
| Reddit screenshot OCR candidates | `chatgpt-important-update-reddit-media-feed` | `views/reddit_media.json` |
| Outage-aware recovery batch | `chatgpt-important-update-backlog-feed` | source-specific recovery views |
| Recovery cursors | `chatgpt-important-update-recovery-state` | `state.json` |
| Retained genuine backlog | `chatgpt-important-update-backlog-retained` | recovery views retained for 48 hours |
| Watchdog telemetry | `chatgpt-important-update-watchdog-status` | `index.json` |

Generated branches are caches, not canonical history. Publication is monotonic: an older or equal writer cannot roll a snapshot backward.

## Reliability model

- Independent producer schedules run away from the top of the hour.
- A public-repository watchdog checks feed timestamps, active runs, and dispatch cooldowns every 15 minutes.
- Ordinary hourly feeds have a 75-minute freshness contract; watchdog telemetry has a 45-minute contract.
- The outage-recovery producer maintains source-specific cursors with overlap and reports incomplete coverage explicitly.
- Consumers must fall back to direct source reads when a feed is stale, inconsistent, unavailable, or incomplete.

GitHub automatically disables scheduled workflows in public repositories after 60 days without repository activity. `.github/workflows/repository-keepalive.yml` therefore checks weekly and commits a small timestamp file to the default branch when its last keepalive is at least 28 days old. Normal maintenance commits also reset the inactivity window.

## Security boundary

Do not add:

- OAuth state, bearer tokens, cookies, API keys, or account identifiers;
- private-repository content;
- personal usage or billing data;
- secrets that would cause public workflow output, logs, artifacts, or generated branches to disclose private information.

All raw artifacts currently produced here are derived from already-public web or GitHub data and are retained briefly for diagnostics.

## Origin

The production workflows were migrated from the private `Happypig375/obsidian` vault after its private-repository Actions allowance was exhausted. The private vault remains the durable knowledge store and retains only private/authenticated automation.