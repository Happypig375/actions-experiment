# Direct public-source checkers

Finite, token-free acquisition commands extracted from this repository's existing public workflows. This directory creates no cron, daemon, scheduled task, webhook, or notification. An external caller invokes the required collector and reads its health.

The existing public snapshot branches remain the consumer interface. Consumers continue resolving a branch to an immutable commit SHA, then read `index.json` and all selected views at that same SHA. This runner can produce those same compact JSON views locally; an explicitly authorized connector publication step publishes verified outputs.

## Scope

- `github`: bryanedds/Nu and asc-community/AngouriMath
- `tibo`: the existing public @thsottiaux feed
- `media`: existing r/codex screenshot candidates and local OCR
- `public` and `recovery`: available for testing, but bundled source coverage must be verified before retiring their existing workflows

Only GitHub, Tibo, and Reddit media are accepted by the publication adapter. Private usage/account collection is excluded. Never add authentication state, tokens, cookies, private repositories, or personal usage data to this directory or its public output trees.

## Setup and direct invocation

Use Python 3.10+ and Tesseract for media OCR. No Python third-party package is needed for runtime. Install dependencies only from approved official sources. Keep runtime files outside version control.

    python runner.py status
    python runner.py run tibo media
    python runner.py run public
    python runner.py run --due
    python runner.py retain
    python runner.py verify
    python -m unittest discover -s tests -v

`runtime/` is the default state directory; `--state PATH` selects another local directory. `--due` only selects collectors whose last attempt is at least an hour old; it does not schedule future execution. Freshness remains 4,500 seconds. A caller must provide the required cadence before claiming a freshness guarantee.

GitHub's anonymous 60-request/hour/IP limit can be insufficient. `bridge_host.js` is a template for the tool-enabled operator to connect exact public repository GETs to an existing GitHub connector. Set its root to the installed directory and run it in that tool runtime, not Node. No credentials are exported. Each invocation uses a unique request-response directory. Never replace this with copied browser/CLI tokens.

`seed_existing.py SOURCE STATE` can initialize an empty state directory from previously materialized immutable public branch snapshots and their provenance. It imports original head SHAs, source cursors, OCR cache and retained backlog without rewriting timestamps. It refuses to overwrite initialized state.

## Snapshot and health contract

Read a pointer once, verify its SHA-256, then read every view from its immutable snapshot directory. Check source status and coverage in `runner.py status`, not merely generated_at. A recent failed acquisition is still a failure. Partial, capped, stale, malformed-timestamp, or unavailable sources require a disclosed gap or permitted direct fallback.

Recovery keeps the original 15-minute overlap and seven-day lookback bound. Failed or incomplete source cursors do not advance. Recovery state and views share one immutable snapshot, and pending candidates are persisted before its pointer advances. Genuine backlog remains available for 48 hours without ordinary runs overwriting or extending it.

Pending records are discovery candidates. OCR is untrusted. Public hot/recovery excerpts may produce enrichment candidates; the consumer remains responsible for significance checks and delivery deduplication. This runner sends no communications.

## Publication

`python publication.py github` prepares a validated, read-only publication plan; replace github with tibo or media. It rejects unapproved paths/branches, fixture/imported/private data, incomplete sources, stale or future snapshots, mixed timestamps, symlinks, and integrity mismatches.

`publication_host.js` is a connected-tool template, read-only by default. Enable remote writes only after explicit release authorization. It creates a complete replacement tree, parents its commit to the observed feed head, compares source timestamps, then uses `update_ref(force=false)`. Non-fast-forward races trigger a fresh immutable-head comparison and bounded retry. An uncertain update is checked before any further action. Published bytes are verified at the resulting immutable SHA.

The available connector cannot create orphan commits or perform force-with-lease. Therefore connector-published snapshots gain parented history. Their tree contents, branch names, immutable-SHA reads and timestamp monotonicity are preserved. Never approximate the old orphan publisher with an unchecked force update. Existing manual workflow_dispatch remains a rollback path; the old force-with-lease publisher can coexist safely during transition.

Retire a producer schedule only after live acquisition and publication both pass and the caller owns the cadence. Also exclude that producer from the old watchdog so it is not restarted. Keep bundled public/recovery workflows if any required source is unverified. Normal push/PR validation CI is unrelated and must remain intact.

## Maintenance and boundaries

`provenance.json` binds the reviewed workflow bytes and extracted collectors to source revision e0b0bf9358bb0f694eb381fa6f027b7e651de523. `extract_collectors.py` regenerates only acquisition Python, never workflow shell/publication steps. The vendored workflow copies are source references, not active workflows.

Runtime state, raw diagnostics, connector responses, evidence, release plans and machine-specific paths are intentionally excluded from this source backup. Protect local state and back it up separately if required. A repository checkout restores executable code, not recent cursors, notification state or acquisition evidence.
