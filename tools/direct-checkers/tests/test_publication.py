"""Offline regressions for the public-only connector publication boundary.

All connector state and generated snapshots are synthetic, isolated fixtures. These
checks perform no external fetch, remote write, or changes to active runtime state.
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
import importlib.util
import json
import pathlib
import sys
import tempfile
import unittest
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
spec = importlib.util.spec_from_file_location('checker_test_publication', ROOT / 'publication.py')
publication = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = publication
spec.loader.exec_module(publication)
UTC = dt.timezone.utc
BASE = dt.datetime(2026, 10, 1, 12, tzinfo=UTC)


def stamp(at=BASE):
    return at.isoformat().replace('+00:00', 'Z')


def sha(label):
    return hashlib.sha1(label.encode()).hexdigest()


def candidate(key='github', *, at=BASE, crlf=False):
    generated = stamp(at)
    common = {'schema_version': 1, 'generated_at': generated}
    index = {**common, 'fresh_for_seconds': 4500}
    docs = {'index.json': index}
    if key == 'github':
        index['repos'] = []
        for source, repo in [('nu', 'bryanedds/Nu'), ('angourimath', 'asc-community/AngouriMath')]:
            index['repos'].append({'key': source, 'full_name': repo, 'head_sha': sha(repo),
                                   'view_path': f'views/{source}.json', 'errors': []})
            docs[f'views/{source}.json'] = {**common, 'repo': repo, 'head_sha': sha(repo),
                                          'errors': [], 'commits': [], 'label': 'Caf\u00e9 \U0001f31f'}
    elif key == 'tibo':
        index['source'] = {'key': 'tibo', 'name': '@thsottiaux', 'view_path': 'views/tibo.json',
                           'status': 'ok', 'errors': []}
        docs['views/tibo.json'] = {**common, 'source': '@thsottiaux',
                                  'candidate_discovery_only': True, 'items': []}
    elif key == 'media':
        index.update(status='ok', ocr_candidate_count=0, view_path='views/reddit_media.json', errors=[])
        docs['views/reddit_media.json'] = {**common, 'source': 'r/codex media',
                                          'candidate_discovery_only': True, 'ocr_is_untrusted': True,
                                          'items': [], 'errors': []}
    files = {path: json.dumps(doc, ensure_ascii=False, indent=2) + '\n' for path, doc in docs.items()}
    if crlf:
        files = {path: text.replace('\n', '\r\n') for path, text in files.items()}
    return publication.Candidate(key, publication.FEEDS[key]['branch'], generated, 'a' * 64, files)


def changed_document(c, path, transform):
    files = dict(c.files)
    doc = json.loads(files[path])
    transform(doc)
    files[path] = json.dumps(doc)
    return dataclasses.replace(c, files=files)


class FakeBackend:
    """In-memory immutable Git objects, with real ancestry-sensitive ref updates."""
    def __init__(self, *, at=BASE - dt.timedelta(seconds=60)):
        self.calls = []
        self.trees = {}
        self.commits = {}
        self.counter = 0
        self.update_steps = []
        self.after_commit = None
        self.head_value = self.external(at, parent=None)
        self.initial_head = self.head_value

    def ident(self, prefix):
        self.counter += 1
        return sha(f'{prefix}-{self.counter}')

    def external(self, at, parent=None):
        tree = self.ident('external-tree')
        self.trees[tree] = {'index.json': json.dumps({'generated_at': stamp(at)}),
                            'obsolete-private-looking-file.json': '{}'}
        commit = self.ident('external-commit')
        self.commits[commit] = {'tree': tree, 'parent': parent}
        self.head_value = commit
        return commit

    def head(self, branch):
        self.calls.append(('head', branch))
        return self.head_value

    def file(self, commit, path):
        self.calls.append(('file', commit, path))
        return self.trees[self.commits[commit]['tree']][path]

    def tree(self, elements):
        self.calls.append(('tree', elements))
        assert all(e['mode'] == '100644' and e['type'] == 'blob' for e in elements)
        assert len({e['path'] for e in elements}) == len(elements)
        tree = self.ident('tree')
        self.trees[tree] = {e['path']: e['content'] for e in elements}
        return tree

    def commit(self, tree, parent, message):
        self.calls.append(('commit', tree, parent, message))
        assert parent in self.commits
        assert tree in self.trees
        commit = self.ident('commit')
        self.commits[commit] = {'tree': tree, 'parent': parent}
        if self.after_commit:
            self.after_commit(self, commit)
        return commit

    def ancestor(self, ancestor, child):
        while child is not None:
            if ancestor == child:
                return True
            child = self.commits[child]['parent']
        return False

    def update(self, branch, commit, *, force=False):
        self.calls.append(('update', branch, commit, force))
        assert force is False, 'A safe publisher must never force an update'
        if self.update_steps:
            self.update_steps.pop(0)(self, commit)
        if not self.ancestor(self.head_value, commit):
            raise publication.RefRejected('non-fast-forward')
        self.head_value = commit

    def mutations(self):
        return [call for call in self.calls if call[0] in ('tree', 'commit', 'update')]


class PublicationCase(unittest.TestCase):
    def setUp(self):
        self.clock = mock.patch.object(publication, 'now', return_value=BASE)
        self.clock.start()
        self.addCleanup(self.clock.stop)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.state = pathlib.Path(self.tmp.name) / 'state'

    def store(self, c, *, mode='live-connector-and-public'):
        hot = self.state / 'snapshots' / c.key / 'isolated-test-snapshot'
        hot.mkdir(parents=True)
        for path, content in c.files.items():
            target = hot / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content.encode('utf-8'))
        pointer = {'snapshot': str(hot.relative_to(self.state)), 'sha256': publication.content_hash(hot),
                   'generated_at': c.generated_at, 'mode': mode}
        (self.state / 'pointers').mkdir(exist_ok=True)
        (self.state / 'pointers' / f'{c.key}.json').write_text(json.dumps(pointer))
        return hot, pointer

    def update_pointer(self, key, transform):
        path = self.state / 'pointers' / f'{key}.json'
        p = json.loads(path.read_text())
        transform(p)
        path.write_text(json.dumps(p))

    def rehash(self, key, snapshot):
        self.update_pointer(key, lambda p: p.update(sha256=publication.content_hash(snapshot)))


class CandidateValidationTests(PublicationCase):
    def test_all_three_supported_public_candidates(self):
        for key in publication.FEEDS:
            with self.subTest(key=key):
                c = candidate(key)
                self.assertIs(c, publication.validate_candidate(c))

    def test_wrong_repository_rejected_before_backend_access(self):
        backend = FakeBackend()
        with self.assertRaises(publication.PublishBlocked):
            publication.Publisher(backend, repository='other/private-repository')
        self.assertEqual([], backend.calls)

    def test_wrong_branch_source_and_path_rejected_without_remote_reads(self):
        c = candidate()
        bad_candidates = [
            dataclasses.replace(c, key='recovery'),
            dataclasses.replace(c, branch='main'),
            dataclasses.replace(c, files={**c.files, '../private.json': '{}'}),
            dataclasses.replace(c, files={p: v for p, v in c.files.items() if p != 'views/nu.json'}),
            changed_document(c, 'views/nu.json', lambda d: d.update(repo='private/account')),
            changed_document(c, 'index.json', lambda d: d['repos'].append({'full_name': 'private/account'})),
            changed_document(candidate('tibo'), 'views/tibo.json', lambda d: d.update(source='@another')),
            changed_document(candidate('media'), 'views/reddit_media.json', lambda d: d.update(source='private media')),
        ]
        for index, bad in enumerate(bad_candidates):
            with self.subTest(case=index):
                backend = FakeBackend()
                with self.assertRaises(publication.PublishBlocked):
                    publication.Publisher(backend).publish(bad)
                self.assertEqual([], backend.calls)

    def test_index_source_identity_and_paths_must_match_public_view(self):
        cases = [
            changed_document(candidate('tibo'), 'index.json',
                             lambda d: d['source'].update(key='private_usage', name='different', view_path='../../private.json')),
            changed_document(candidate(), 'index.json',
                             lambda d: d['repos'][0].update(key='private_usage', view_path='../../private.json')),
        ]
        for c in cases:
            with self.subTest(key=c.key), self.assertRaises(publication.PublishBlocked):
                publication.validate_candidate(c)

    def test_explicit_private_payload_marker_is_rejected_recursively(self):
        for target in ['index.json', 'views/nu.json']:
            c = changed_document(candidate(), target, lambda d: d.update(nested=[{'private': True}]))
            with self.subTest(target=target), self.assertRaises(publication.PublishBlocked):
                publication.validate_candidate(c)

    def test_media_index_errors_rejected_even_when_view_has_no_errors(self):
        c = changed_document(candidate('media'), 'index.json',
                             lambda d: d.update(status='error', errors=['failed']))
        with self.assertRaises(publication.PublishBlocked):
            publication.validate_candidate(c)

    def test_private_authentication_fields_rejected_recursively(self):
        for field in publication.FORBIDDEN_KEYS:
            with self.subTest(field=field):
                c = changed_document(candidate(), 'views/nu.json',
                                     lambda d: d.update(extra=[{'nested': {field.upper(): 'synthetic'}}]))
                with self.assertRaises(publication.PublishBlocked):
                    publication.validate_candidate(c)

    def test_future_and_expired_candidate_rejected(self):
        for at in [BASE + dt.timedelta(minutes=6), BASE - dt.timedelta(seconds=4501)]:
            with self.subTest(at=at), self.assertRaises(publication.PublishBlocked):
                publication.validate_candidate(candidate(at=at))

    def test_mixed_timestamps_or_changed_freshness_rejected(self):
        for c in [changed_document(candidate(), 'views/nu.json', lambda d: d.update(generated_at=stamp(BASE - dt.timedelta(seconds=1)))),
                  changed_document(candidate(), 'index.json', lambda d: d.update(fresh_for_seconds=4501))]:
            with self.assertRaises(publication.PublishBlocked):
                publication.validate_candidate(c)

    def test_unhealthy_or_untrusted_sources_rejected(self):
        bad = [changed_document(candidate(), 'views/nu.json', lambda d: d.update(errors=['synthetic coverage error'])),
               changed_document(candidate('tibo'), 'index.json', lambda d: d['source'].update(status='unavailable')),
               changed_document(candidate('tibo'), 'views/tibo.json', lambda d: d.update(candidate_discovery_only=False)),
               changed_document(candidate('media'), 'views/reddit_media.json', lambda d: d.update(ocr_is_untrusted=False)),
               changed_document(candidate('media'), 'views/reddit_media.json', lambda d: d.update(items=[{'ocr_errors': ['synthetic OCR error']}]))]
        for c in bad:
            with self.subTest(key=c.key), self.assertRaises(publication.PublishBlocked):
                publication.validate_candidate(c)

    def test_prepare_preserves_exact_utf8_and_crlf_bytes(self):
        c = candidate(crlf=True)
        snapshot, pointer = self.store(c)
        prepared = publication.prepare_candidate(self.state, c.key)
        self.assertEqual(pointer['sha256'], prepared.snapshot_sha256)
        self.assertEqual(c.files, prepared.files)
        for path, content in prepared.files.items():
            self.assertEqual((snapshot / path).read_bytes(), content.encode('utf-8'))

    def test_prepare_rejects_fixture_imported_missing_or_unknown_modes(self):
        c = candidate()
        self.store(c)
        for mode in ['fixture', 'seed-existing', 'imported', None, 'live-private']:
            with self.subTest(mode=mode):
                self.update_pointer(c.key, lambda p: p.update(mode=mode))
                with self.assertRaises(publication.PublishBlocked):
                    publication.prepare_candidate(self.state, c.key)

    def test_prepare_rejects_unsupported_collectors(self):
        for key in ['public', 'recovery', 'private_usage', '../github']:
            with self.subTest(key=key), self.assertRaises(publication.PublishBlocked):
                publication.prepare_candidate(self.state, key)

    def test_prepare_rejects_snapshot_outside_scope(self):
        c = candidate()
        self.store(c)
        self.update_pointer(c.key, lambda p: p.update(snapshot='../private'))
        with self.assertRaises(publication.PublishBlocked):
            publication.prepare_candidate(self.state, c.key)

    def test_prepare_rejects_integrity_mismatch(self):
        c = candidate()
        snapshot, _ = self.store(c)
        (snapshot / 'views/nu.json').write_text('{}')
        with self.assertRaises(publication.PublishBlocked):
            publication.prepare_candidate(self.state, c.key)

    def test_prepare_rejects_symlink_file_even_with_matching_hash(self):
        c = candidate()
        snapshot, _ = self.store(c)
        target = snapshot / 'views/nu.json'
        outside = self.state.parent / 'outside-public-looking.json'
        outside.write_bytes(target.read_bytes())
        target.unlink()
        target.symlink_to(outside)
        self.rehash(c.key, snapshot)
        with self.assertRaises(publication.PublishBlocked):
            publication.prepare_candidate(self.state, c.key)

    def test_prepare_rejects_extra_private_local_files(self):
        c = candidate()
        snapshot, _ = self.store(c)
        (snapshot / 'account-usage.json').write_text('{"weekly":0}')
        self.rehash(c.key, snapshot)
        with self.assertRaises(publication.PublishBlocked):
            publication.prepare_candidate(self.state, c.key)

    def test_prepare_rechecks_health_in_snapshot(self):
        c = changed_document(candidate(), 'index.json', lambda d: d['repos'][0].update(errors=['incomplete']))
        self.store(c)
        with self.assertRaises(publication.PublishBlocked):
            publication.prepare_candidate(self.state, c.key)


class PublisherStateMachineTests(PublicationCase):
    def test_older_and_equal_snapshot_skip_without_mutations(self):
        for at in [BASE, BASE + dt.timedelta(seconds=1)]:
            with self.subTest(at=at):
                backend = FakeBackend(at=at)
                result = publication.Publisher(backend).publish(candidate())
                self.assertEqual('skipped-not-newer', result['action'])
                self.assertEqual([], backend.mutations())
                self.assertEqual(backend.initial_head, backend.head_value)

    def test_full_replacement_tree_exact_bytes_immutable_parent_and_no_force(self):
        for key in publication.FEEDS:
            with self.subTest(key=key):
                backend = FakeBackend()
                c = candidate(key, crlf=True)
                result = publication.Publisher(backend).publish(c)
                self.assertEqual('published', result['action'])
                self.assertEqual(backend.initial_head, result['parent'])
                written = backend.commits[result['commit']]
                self.assertEqual(backend.initial_head, written['parent'])
                self.assertEqual(c.files, backend.trees[written['tree']])
                self.assertNotIn('obsolete-private-looking-file.json', backend.trees[written['tree']])
                self.assertTrue(all(call[-1] is False for call in backend.calls if call[0] == 'update'))
                self.assertTrue(all(publication.SHA.fullmatch(call[1]) for call in backend.calls if call[0] == 'file'))

    def test_race_with_older_snapshot_rebuilds_commit_on_refetched_head(self):
        backend = FakeBackend(at=BASE - dt.timedelta(seconds=120))
        def race(b, commit):
            b.external(BASE - dt.timedelta(seconds=30), parent=b.head_value)
        backend.update_steps = [race]
        result = publication.Publisher(backend).publish(candidate())
        self.assertEqual('published', result['action'])
        self.assertEqual(2, result['attempts'])
        commits = [call for call in backend.calls if call[0] == 'commit']
        self.assertNotEqual(commits[0][2], commits[1][2])
        self.assertEqual(commits[1][2], result['parent'])
        self.assertEqual(1, sum(call[0] == 'tree' for call in backend.calls))

    def test_race_with_equal_or_newer_snapshot_skips_after_refetch(self):
        for at in [BASE, BASE + dt.timedelta(seconds=1)]:
            with self.subTest(at=at):
                backend = FakeBackend()
                backend.update_steps = [lambda b, commit: b.external(at, parent=b.head_value)]
                result = publication.Publisher(backend).publish(candidate())
                self.assertEqual('skipped-not-newer', result['action'])
                self.assertEqual(2, result['attempts'])
                self.assertEqual(1, sum(call[0] == 'update' for call in backend.calls))

    def test_concurrent_orphan_snapshot_forces_rebuild_never_force_update(self):
        backend = FakeBackend()
        backend.update_steps = [lambda b, commit: b.external(BASE - dt.timedelta(seconds=10), parent=None)]
        result = publication.Publisher(backend).publish(candidate())
        self.assertEqual('published', result['action'])
        self.assertEqual(2, result['attempts'])
        self.assertTrue(all(call[-1] is False for call in backend.calls if call[0] == 'update'))

    def test_retry_budget_is_bounded_and_never_advances_after_four_rejections(self):
        backend = FakeBackend()
        def reject(b, commit):
            raise publication.RefRejected('synthetic non-fast-forward')
        backend.update_steps = [reject] * 4
        with self.assertRaises(publication.PublishBlocked):
            publication.Publisher(backend).publish(candidate())
        self.assertEqual(4, sum(call[0] == 'update' for call in backend.calls))
        self.assertEqual(backend.initial_head, backend.head_value)

    def test_invalid_retry_budget_rejected_before_backend_access(self):
        for budget in [0, 5, -1]:
            with self.subTest(budget=budget):
                backend = FakeBackend()
                with self.assertRaises(publication.PublishBlocked):
                    publication.Publisher(backend).publish(candidate(), max_attempts=budget)
                self.assertEqual([], backend.calls)

    def test_unknown_update_failure_does_not_retry(self):
        backend = FakeBackend()
        def denied(b, commit):
            raise PermissionError('synthetic forbidden')
        backend.update_steps = [denied]
        with self.assertRaises((PermissionError, publication.PublishBlocked)):
            publication.Publisher(backend).publish(candidate())
        self.assertEqual(1, sum(call[0] == 'update' for call in backend.calls))
        self.assertEqual(backend.initial_head, backend.head_value)

    def test_uncertain_applied_update_is_verified_without_duplicate_write(self):
        backend = FakeBackend()
        def applied(b, commit):
            b.head_value = commit
            raise publication.RefUncertain('synthetic lost response')
        backend.update_steps = [applied]
        result = publication.Publisher(backend).publish(candidate())
        self.assertEqual('published-verified-after-uncertain-update', result['action'])
        self.assertEqual(1, sum(call[0] == 'update' for call in backend.calls))
        self.assertEqual(result['commit'], backend.head_value)
        for path in candidate().files:
            self.assertIn(('file', result['commit'], path), backend.calls)

    def test_uncertain_nonapplied_update_stops_without_retry(self):
        backend = FakeBackend()
        def unknown(b, commit):
            raise publication.RefUncertain('synthetic lost response')
        backend.update_steps = [unknown]
        with self.assertRaisesRegex(publication.PublishBlocked, 'uncertain'):
            publication.Publisher(backend).publish(candidate())
        self.assertEqual(1, sum(call[0] == 'update' for call in backend.calls))
        self.assertEqual(backend.initial_head, backend.head_value)

    def test_uncertain_update_with_newer_winner_stops_without_overwrite(self):
        backend = FakeBackend()
        def winner(b, commit):
            b.external(BASE + dt.timedelta(seconds=1), parent=b.head_value)
            raise publication.RefUncertain('synthetic lost response')
        backend.update_steps = [winner]
        result = publication.Publisher(backend).publish(candidate())
        self.assertEqual('superseded-after-uncertain-update', result['action'])
        self.assertEqual(1, sum(call[0] == 'update' for call in backend.calls))
        self.assertEqual(result['current_commit'], backend.head_value)

    def test_uncertain_applied_but_corrupted_tree_is_not_claimed_success(self):
        backend = FakeBackend()
        def corrupt(b, commit):
            b.head_value = commit
            b.trees[b.commits[commit]['tree']]['views/nu.json'] = '{}'
            raise publication.RefUncertain('synthetic lost response')
        backend.update_steps = [corrupt]
        with self.assertRaises(publication.PublishBlocked):
            publication.Publisher(backend).publish(candidate())
        self.assertEqual(1, sum(call[0] == 'update' for call in backend.calls))

    def test_success_response_followed_by_newer_head_is_classified_superseded(self):
        backend = FakeBackend()
        original_update = backend.update
        def update_then_newer(branch, commit, *, force=False):
            original_update(branch, commit, force=force)
            backend.external(BASE + dt.timedelta(seconds=1), parent=commit)
        backend.update = update_then_newer
        result = publication.Publisher(backend).publish(candidate())
        self.assertEqual('published-then-superseded', result['action'])
        self.assertNotEqual(result['commit'], result['current_commit'])

    def test_success_response_followed_by_timestamp_rollback_blocks_cutover(self):
        backend = FakeBackend()
        original_update = backend.update
        def update_then_rollback(branch, commit, *, force=False):
            original_update(branch, commit, force=force)
            backend.external(BASE - dt.timedelta(seconds=1), parent=None)
        backend.update = update_then_rollback
        with self.assertRaisesRegex(publication.PublishBlocked, 'backward'):
            publication.Publisher(backend).publish(candidate())

    def test_remote_invalid_or_missing_timestamp_fails_closed_before_write(self):
        for payload in ['not json', '{}', '{"generated_at":"invalid"}', '{"generated_at":"2026-10-01T12:00:00"}']:
            with self.subTest(payload=payload):
                backend = FakeBackend()
                backend.trees[backend.commits[backend.head_value]['tree']]['index.json'] = payload
                with self.assertRaises((publication.PublishBlocked, ValueError, KeyError)):
                    publication.Publisher(backend).publish(candidate())
                self.assertEqual([], backend.mutations())

    def test_remote_unreadable_or_future_snapshot_fails_closed(self):
        for failure in ['missing', 'future']:
            with self.subTest(failure=failure):
                backend = FakeBackend(at=BASE + dt.timedelta(minutes=6) if failure == 'future' else BASE)
                if failure == 'missing':
                    del backend.trees[backend.commits[backend.head_value]['tree']]['index.json']
                with self.assertRaises((publication.PublishBlocked, KeyError)):
                    publication.Publisher(backend).publish(candidate())
                self.assertEqual([], backend.mutations())

    def test_invalid_remote_head_sha_is_not_used_to_fetch_content(self):
        backend = FakeBackend()
        backend.head_value = 'main'
        with self.assertRaises(publication.PublishBlocked):
            publication.Publisher(backend).publish(candidate())
        self.assertFalse(any(call[0] == 'file' for call in backend.calls))

    def test_candidate_expiring_during_tool_calls_is_not_published(self):
        backend = FakeBackend(at=BASE - dt.timedelta(seconds=4500))
        c = candidate(at=BASE - dt.timedelta(seconds=4499))
        def advance_clock(b, commit):
            publication.now.return_value = BASE + dt.timedelta(seconds=2)
        backend.after_commit = advance_clock
        with self.assertRaisesRegex(publication.PublishBlocked, 'Stale'):
            publication.Publisher(backend).publish(c)
        self.assertFalse(any(call[0] == 'update' for call in backend.calls))


# Execute the actual functions.exec template with a fully synthetic tools object.
# No connector/tool call escapes this process; every API is an in-memory stub.
HOST_HARNESS = r'''
const fs = require('fs');
const request = JSON.parse(fs.readFileSync(0, 'utf8'));
const plan = request.plan;
let clock = Date.parse(request.now);
Date.now = () => clock;
const calls = [], outputs = [], trees = {}, commits = {};
let counter = 0, updates = 0;
const id = () => (++counter).toString(16).padStart(40, '0');
const ok = value => ({structuredContent:value});
const fail = message => ({isError:true, content:[{type:'text', text:message}]});
function external(when, parent=null) {
  const tree = id();
  trees[tree] = {'index.json':JSON.stringify({generated_at:new Date(when).toISOString()}), 'obsolete.json':'{}'};
  const commit = id(); commits[commit] = {tree, parent}; return commit;
}
let head = external(request.current);
const initial = head;
function ancestor(parent, child) {
  while (child) { if (parent===child) return true; child=commits[child].parent; }
  return false;
}
const tools = {
  exec_command:async args=>{calls.push(['validate-local',args]);return {exit_code:0,output:JSON.stringify(plan)};},
  mcp__codex_apps__github_fetch:async args=>{
    calls.push(['read',args.url]);
    if(args.url.includes('/branches/')) return ok({content:JSON.stringify({commit:{sha:head}})});
    const prefix='https://raw.githubusercontent.com/'+plan.repository+'/';
    if(!args.url.startsWith(prefix)) throw Error('Unexpected read target');
    const rest=args.url.slice(prefix.length), slash=rest.indexOf('/');
    const commit=rest.slice(0,slash), path=rest.slice(slash+1);
    return ok({content:trees[commits[commit].tree][path]});
  },
  mcp__codex_apps__github_create_tree:async args=>{
    calls.push(['tree',args]);
    if(args.repository_full_name!==plan.repository||args.base_tree_sha!==null)throw Error('Non-replacement tree');
    const tree=id();trees[tree]=Object.fromEntries(args.tree_elements.map(e=>[e.path,e.content]));return ok({sha:tree});
  },
  mcp__codex_apps__github_create_commit:async args=>{
    calls.push(['commit',args]);
    const commit=id(); commits[commit]={tree:args.tree_sha,parent:args.parent_sha};
    if(request.mode==='expire')clock+=4501000;
    return ok({sha:commit});
  },
  mcp__codex_apps__github_update_ref:async args=>{
    calls.push(['update',args]); updates++;
    if(args.force!==false||args.branch_name!==plan.branch||args.repository_full_name!==plan.repository)throw Error('Unsafe ref mutation');
    if(updates===1 && request.mode==='race_older')head=external(Date.parse(plan.generated_at)-1000,head);
    if(updates===1 && request.mode==='race_newer')head=external(Date.parse(plan.generated_at)+1000,head);
    if(request.mode==='denied')return fail('permission denied');
    if(request.mode==='risk_rejected')return fail('This action was rejected due to unacceptable risk.');
    if(request.mode==='reject_all')return fail('not a fast-forward');
    if(request.mode==='uncertain_not_applied')return fail('synthetic timeout');
    if(request.mode==='uncertain_newer'){head=external(Date.parse(plan.generated_at)+1000,head);return fail('synthetic timeout');}
    if(!ancestor(head,args.sha))return fail('not a fast-forward');
    head=args.sha;
    if(request.mode==='uncertain_applied')return fail('synthetic timeout');
    if(request.mode==='newer_after_success')head=external(Date.parse(plan.generated_at)+1000,head);
    return ok({ref:'refs/heads/'+args.branch_name,object:{sha:args.sha}});
  }
};
const AsyncFunction=Object.getPrototypeOf(async function(){}).constructor;
(async()=>{
  let error=null;
  try {await new AsyncFunction('tools','text',request.source)(tools, value=>outputs.push(value));}
  catch(e){error=e.message;}
  process.stdout.write(JSON.stringify({error,calls,outputs,head,initial,trees,commits}));
})();
'''


class ConnectorHostTemplateTests(PublicationCase):
    def run_host(self, *, key='github', approved=True, mode='normal', current=None):
        import shutil
        import subprocess
        if not shutil.which('node'):
            self.skipTest('Node is unavailable for offline connector-host tests')
        c = candidate(key)
        plan = {'repository': publication.REPOSITORY, 'key': key, 'branch': c.branch,
                'generated_at': c.generated_at, 'snapshot_sha256': c.snapshot_sha256,
                'files': c.files, 'tree_elements': c.tree_elements(),
                'publication_mode': 'parented-fast-forward-only', 'remote_writes_performed': False}
        source = (ROOT / 'publication_host.js').read_text()
        source = source.replace('const key = "github";', f'const key = "{key}";')
        if approved:
            self.assertEqual(1, source.count('const approveRemoteWrite = false;'))
            source = source.replace('const approveRemoteWrite = false;', 'const approveRemoteWrite = true;')
        request = {'source': source, 'plan': plan, 'mode': mode, 'now': stamp(),
                   'current': stamp(current or BASE - dt.timedelta(seconds=60))}
        result = subprocess.run(['node', '-e', HOST_HARNESS], input=json.dumps(request),
                                text=True, capture_output=True, timeout=10, check=True)
        data=json.loads(result.stdout)
        data['diagnostics']=[v for v in data['outputs'] if 'stage' in v]
        data['outputs']=[v for v in data['outputs'] if 'action' in v]
        return data, c

    def test_host_is_read_only_by_default(self):
        result, _ = self.run_host(approved=False)
        self.assertIsNone(result['error'])
        self.assertEqual('would-publish', result['outputs'][0]['action'])
        self.assertEqual(result['initial'], result['head'])
        self.assertFalse(any(call[0] in ('tree', 'commit', 'update') for call in result['calls']))

    def test_host_exact_replacement_files_for_all_approved_feeds(self):
        for key in publication.FEEDS:
            with self.subTest(key=key):
                result, c = self.run_host(key=key)
                self.assertIsNone(result['error'])
                self.assertEqual('published', result['outputs'][0]['action'])
                commit = result['commits'][result['head']]
                self.assertEqual(result['initial'], commit['parent'])
                self.assertEqual(c.files, result['trees'][commit['tree']])
                self.assertTrue(all(call[1]['force'] is False for call in result['calls'] if call[0] == 'update'))

    def test_host_skips_equal_and_newer_remote_without_mutation(self):
        for when in [BASE, BASE + dt.timedelta(seconds=1)]:
            with self.subTest(when=when):
                result, _ = self.run_host(current=when)
                self.assertIsNone(result['error'])
                self.assertEqual('skipped-not-newer', result['outputs'][0]['action'])
                self.assertFalse(any(call[0] in ('tree', 'commit', 'update') for call in result['calls']))

    def test_host_rebuilds_after_older_race_and_skips_newer_race(self):
        for mode, action, attempts in [('race_older', 'published', 2), ('race_newer', 'skipped-not-newer', 1)]:
            with self.subTest(mode=mode):
                result, _ = self.run_host(mode=mode)
                self.assertIsNone(result['error'])
                self.assertEqual(action, result['outputs'][0]['action'])
                self.assertEqual(attempts, sum(call[0] == 'update' for call in result['calls']))

    def test_host_stops_after_four_non_fast_forward_rejections(self):
        result, _ = self.run_host(mode='reject_all')
        self.assertIn('exhausted', result['error'])
        self.assertEqual(4, sum(call[0] == 'update' for call in result['calls']))
        self.assertEqual(result['initial'], result['head'])

    def test_host_stops_immediately_on_permission_denial(self):
        result, _ = self.run_host(mode='denied')
        self.assertIn('denied', result['error'])
        self.assertEqual(1, sum(call[0] == 'update' for call in result['calls']))
        self.assertEqual(result['initial'], result['head'])

    def test_host_uncertain_applied_and_superseded_outcomes_verified(self):
        for mode, action in [('uncertain_applied', 'published-verified-after-uncertain-update'),
                             ('uncertain_newer', 'superseded-after-uncertain-update'),
                             ('newer_after_success', 'published-then-superseded')]:
            with self.subTest(mode=mode):
                result, _ = self.run_host(mode=mode)
                self.assertIsNone(result['error'])
                self.assertEqual(action, result['outputs'][0]['action'])
                self.assertEqual(1, sum(call[0] == 'update' for call in result['calls']))

    def test_host_uncertain_nonapplied_update_does_not_retry(self):
        result, _ = self.run_host(mode='uncertain_not_applied')
        self.assertIn('Uncertain ref update', result['error'])
        self.assertEqual(1, sum(call[0] == 'update' for call in result['calls']))
        self.assertEqual(result['initial'], result['head'])

    def test_host_retains_prepared_commit_and_error_diagnostics(self):
        for mode in ('uncertain_not_applied','risk_rejected'):
            with self.subTest(mode=mode):
                result,_=self.run_host(mode=mode)
                prepared=[v for v in result['diagnostics'] if v['stage']=='prepared-ref-update']
                errors=[v for v in result['diagnostics'] if v['stage']=='ref-update-error']
                self.assertEqual(1,len(prepared));self.assertEqual(1,len(errors))
                self.assertEqual(prepared[0]['commit'],errors[0]['commit'])
                self.assertTrue(errors[0]['result']['isError'])
                self.assertEqual(1,sum(call[0]=='update' for call in result['calls']))
                if mode=='risk_rejected':self.assertIn('denied',result['error'])

    def test_host_rechecks_freshness_immediately_before_ref_update(self):
        result, _ = self.run_host(mode='expire')
        self.assertIn('no longer fresh', result['error'])
        self.assertFalse(any(call[0] == 'update' for call in result['calls']))
        self.assertEqual(result['initial'], result['head'])


class MediaPrivacyTests(PublicationCase):
    def with_items(self, items, **view_extra):
        c = candidate('media')
        c = changed_document(c, 'views/reddit_media.json', lambda d: d.update(items=items, **view_extra))
        return changed_document(c, 'index.json', lambda d: d.update(
            candidate_count=len(items), ocr_candidate_count=sum(bool(i.get('ocr_text')) for i in items)))

    def derive(self, c):
        return publication.validate_candidate(publication._prepare_media_publication(c))

    def test_known_item_is_withheld_even_when_ocr_looks_safe(self):
        risky = {'id': next(iter(publication.MEDIA_WITHHELD_IDS)), 'url': 'https://example.org/reviewed',
                 'ocr_text': 'No risky words survived this OCR run', 'media_urls': ['https://example.org/image'],
                 'ocr_sources': ['https://example.org/ocr'], 'ocr_errors': []}
        safe = {'id': 'safe', 'url': 'https://example.org/safe', 'ocr_text': 'Public product demonstration', 'ocr_errors': []}
        original = self.with_items([risky, safe])
        before = dict(original.files)
        derived = self.derive(original)
        self.assertEqual(before, original.files)
        self.assertEqual(original.snapshot_sha256, derived.source_snapshot_sha256)
        self.assertNotEqual(original.snapshot_sha256, derived.snapshot_sha256)
        self.assertEqual(derived.snapshot_sha256, publication._files_hash(derived.files))
        self.assertEqual(original.generated_at, derived.generated_at)
        output = '\n'.join(derived.files.values())
        for value in (risky['id'], risky['url'], risky['ocr_text'], *risky['media_urls'], *risky['ocr_sources']):
            self.assertNotIn(value, output)
        index = json.loads(derived.files['index.json'])
        view = json.loads(derived.files['views/reddit_media.json'])
        self.assertEqual([safe], view['items'])
        self.assertEqual((1, 1), (index['candidate_count'], index['ocr_candidate_count']))
        self.assertEqual(1, index['publication_privacy']['excluded_candidate_count'])
        self.assertEqual(index['publication_privacy'], view['publication_privacy'])
        self.assertEqual('ok', index['status'])

    def test_sensitive_item_fields_are_withheld_without_echoing_values(self):
        for text in ['sk-example-truncated', 'Account balance is hidden', 'C:\\Users\\example\\private',
                     'Message User confidential transcript', 'contact@example.org', 'BEGIN PRIVATE KEY',
                     'api_key: syntheticexamplevalue']:
            with self.subTest(category=text.split()[0]):
                c = self.with_items([{'id': 'synthetic', 'ocr_text': text, 'ocr_errors': []}])
                with self.assertRaises(publication.PublishBlocked):
                    publication.validate_candidate(c)
                derived = self.derive(c)
                index = json.loads(derived.files['index.json'])
                self.assertEqual((0, 0, 'empty'), (index['candidate_count'], index['ocr_candidate_count'], index['status']))
                self.assertTrue(index['publication_privacy']['acquisition_coverage_complete'])

    def test_suspect_text_outside_items_fails_closed(self):
        c = self.with_items([], diagnostic='sk-example-truncated')
        with self.assertRaises(publication.PublishBlocked):
            self.derive(c)

    def test_quoted_and_multiline_ocr_is_scanned_before_json_escaping(self):
        for text in ['api_key: "syntheticexamplevalue"', 'password:\nsyntheticexamplevalue',
                     'prefix\nsk-example-truncated', 'Bearer abcdef1234\nsyntheticvalue5678']:
            with self.subTest(form=text.split(':')[0]):
                c = self.with_items([{'id': 'synthetic', 'ocr_text': text, 'ocr_errors': []}])
                with self.assertRaises(publication.PublishBlocked):
                    publication.validate_candidate(c)
                self.assertEqual([], json.loads(self.derive(c).files['views/reddit_media.json'])['items'])

    def test_withheld_reference_in_another_field_fails_closed(self):
        c = self.with_items([{'id': next(iter(publication.MEDIA_WITHHELD_IDS)), 'ocr_text': 'reviewed string "quoted"'}],
                            cache='reviewed string "quoted"')
        with self.assertRaises(publication.PublishBlocked):
            self.derive(c)

    def test_ordinary_technical_words_are_not_credential_values(self):
        c = self.with_items([{'id': 'safe', 'ocr_text': 'API tokens and bearer authentication overview', 'ocr_errors': []}])
        self.assertEqual(c, self.derive(c))

    def test_public_product_prices_and_cost_news_are_preserved(self):
        for text in ['The public product price is $20/month', 'The model costs $1 per million tokens',
                     'A good balance of speed and quality', 'Pricing starts at $25 with optional upgrades']:
            with self.subTest(text=text):
                c = self.with_items([{'id': 'safe', 'ocr_text': text, 'ocr_errors': []}])
                self.assertEqual(c, self.derive(c))

    def test_account_specific_balance_and_spend_are_withheld(self):
        for text in ['Your account balance is $18.57', 'Wallet balance: $18.57', 'Spent: $12.34',
                     '$18.57 balance', '$8.49 of $25.00 cap']:
            with self.subTest(text=text):
                c = self.with_items([{'id': 'synthetic', 'ocr_text': text, 'ocr_errors': []}])
                self.assertEqual([], json.loads(self.derive(c).files['views/reddit_media.json'])['items'])

    def test_count_or_provenance_tampering_is_blocked(self):
        c = self.derive(self.with_items([{'id': next(iter(publication.MEDIA_WITHHELD_IDS)), 'ocr_text': 'reviewed'}]))
        with self.assertRaises(publication.PublishBlocked):
            publication.validate_candidate(dataclasses.replace(c, source_snapshot_sha256=None))
        bad = changed_document(c, 'index.json', lambda d: d.update(candidate_count=99))
        with self.assertRaises(publication.PublishBlocked):
            publication.validate_candidate(bad)

    def test_repeated_prepare_preserves_source_files_pointer_and_timestamp(self):
        c = self.with_items([{'id': next(iter(publication.MEDIA_WITHHELD_IDS)), 'ocr_text': 'reviewed'}])
        snapshot, pointer = self.store(c, mode='live-anonymous')
        pointer_path = self.state / 'pointers' / 'media.json'
        before_pointer = pointer_path.read_bytes()
        before_files = {str(p.relative_to(snapshot)): p.read_bytes() for p in snapshot.rglob('*') if p.is_file()}
        first = publication.prepare_candidate(self.state, 'media')
        second = publication.prepare_candidate(self.state, 'media')
        self.assertEqual(first, second)
        self.assertEqual(pointer['sha256'], first.source_snapshot_sha256)
        self.assertEqual(pointer['sha256'], publication.content_hash(snapshot))
        self.assertEqual(before_pointer, pointer_path.read_bytes())
        self.assertEqual(before_files, {str(p.relative_to(snapshot)): p.read_bytes() for p in snapshot.rglob('*') if p.is_file()})
        self.assertEqual(c.generated_at, first.generated_at)

    def test_privacy_exclusion_cannot_hide_failed_acquisition(self):
        c = self.with_items([{'id': next(iter(publication.MEDIA_WITHHELD_IDS)), 'ocr_errors': ['failed OCR']}])
        self.store(c, mode='live-anonymous')
        with self.assertRaises(publication.PublishBlocked):
            publication.prepare_candidate(self.state, 'media')

    def test_direct_unsafe_publication_fails_before_any_remote_read_or_write(self):
        c = self.with_items([{'id': 'synthetic', 'ocr_text': 'sk-example-truncated'}])
        backend = FakeBackend()
        with self.assertRaises(publication.PublishBlocked):
            publication.Publisher(backend).publish(c)
        self.assertEqual([], backend.calls)


if __name__ == '__main__':
    unittest.main()
