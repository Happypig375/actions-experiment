"""Offline safety regressions for the local acquisition boundary.

No real network, auth values, workflow execution, or live state is used here.
"""
import contextlib
import hashlib
import datetime as dt
import importlib.util
import json
import os
import pathlib
import runpy
import tempfile
import unittest
import urllib.error
import urllib.parse
import urllib.request
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parents[1]


def load_module(name):
    spec = importlib.util.spec_from_file_location('checker_test_' + name, ROOT / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


runner = load_module('runner')
worker = load_module('worker')
UTC = dt.timezone.utc
BASE = dt.datetime(2026, 9, 30, 12, tzinfo=UTC)


@contextlib.contextmanager
def working_directory(path):
    previous = pathlib.Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


class StateCase(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = pathlib.Path(self.temporary.name)
        self.state = self.root / 'state'
        self.state.mkdir()
        self.clock = mock.patch.object(runner, 'now', return_value=BASE)
        self.clock.start()
        self.addCleanup(self.clock.stop)
        self.counter = 0

    def hot(self, *, at=BASE, sources=None, views=None, recovery_state=None, backlog=False):
        self.counter += 1
        path = self.root / ('hot-' + str(self.counter))
        (path / 'views').mkdir(parents=True)
        index = {'generated_at': runner.stamp(at), 'fresh_for_seconds': 4500,
                 'sources': sources if sources is not None else [
                     {'key': 'nu', 'status': 'ok', 'view_path': 'views/nu.json', 'errors': []}],
                 'has_backlog': backlog}
        runner.atomic(path / 'index.json', index)
        for key, document in (views or {'nu': {'commits': []}}).items():
            runner.atomic(path / 'views' / (key + '.json'), document)
        if recovery_state is not None:
            runner.atomic(path / 'state.json', recovery_state)
        return path

    def publish(self, hot, key='github'):
        return runner.publish(self.state, key, hot, {'mode': 'fixture'})

    def pointer(self, key='github'):
        return runner.read(self.state / 'pointers' / (key + '.json'))


class SnapshotTests(StateCase):
    def test_older_and_equal_snapshots_do_not_advance_pointer_or_pending(self):
        first = self.publish(self.hot())
        pending = (self.state / 'pending/github.json').read_bytes()
        for at in (BASE, BASE - dt.timedelta(seconds=1)):
            with self.subTest(at=at):
                result = self.publish(self.hot(at=at, views={'nu': {'commits': [{'sha': 'unexpected'}]}}))
                self.assertFalse(result['accepted'])
                self.assertEqual('older-or-equal', result['reason'])
                self.assertEqual(first['pointer'], self.pointer())
                self.assertEqual(pending, (self.state / 'pending/github.json').read_bytes())

    def test_newer_snapshot_advances(self):
        self.publish(self.hot(at=BASE - dt.timedelta(seconds=1)))
        result = self.publish(self.hot())
        self.assertTrue(result['accepted'])
        self.assertEqual(runner.stamp(BASE), self.pointer()['generated_at'])

    def test_future_snapshot_rejected(self):
        with self.assertRaisesRegex(ValueError, 'future'):
            self.publish(self.hot(at=BASE + dt.timedelta(minutes=6)))
        self.assertIsNone(self.pointer())

    def test_snapshot_is_immutable_copy_and_hash_covers_state_and_views(self):
        hot = self.hot(recovery_state={'sources': {'nu': {'last_success_at': runner.stamp(BASE)}}})
        result = self.publish(hot, 'recovery')
        pointer = result['pointer']
        snapshot = self.state / pointer['snapshot']
        self.assertEqual(pointer['sha256'], runner.content_hash(snapshot))
        (hot / 'state.json').write_text('{}')
        self.assertEqual(pointer['sha256'], runner.content_hash(snapshot))
        (snapshot / 'state.json').write_text('{}')
        self.assertNotEqual(pointer['sha256'], runner.content_hash(snapshot))

    def test_existing_corrupt_content_address_is_rejected(self):
        hot = self.hot()
        target = self.state / 'snapshots/github' / runner.content_hash(hot)
        target.mkdir(parents=True)
        (target / 'index.json').write_text('{}')
        with self.assertRaisesRegex(RuntimeError, 'hash mismatch'):
            self.publish(hot)
        self.assertIsNone(self.pointer())

    def test_previous_snapshot_hash_verified_before_starting_collector(self):
        self.publish(self.hot())
        snapshot = self.state / self.pointer()['snapshot']
        (snapshot / 'index.json').write_text('{}')
        with mock.patch.object(runner.subprocess, 'run') as subprocess_run:
            with self.assertRaisesRegex(RuntimeError, 'integrity'):
                runner.run_one(self.state, 'github')
            subprocess_run.assert_not_called()

    def test_recovery_cursor_is_in_same_snapshot_before_pointer_commit(self):
        expected = {'sources': {'nu': {'last_success_at': runner.stamp(BASE)}}}
        hot = self.hot(recovery_state=expected)
        atomic = runner.atomic
        commits = []

        def inspect_commit(path, document):
            if path == self.state / 'pointers/recovery.json':
                snapshot = self.state / document['snapshot']
                self.assertEqual(expected, runner.read(snapshot / 'state.json'))
                self.assertTrue((snapshot / 'index.json').is_file())
                self.assertTrue((snapshot / 'views/nu.json').is_file())
                self.assertEqual(document['sha256'], runner.content_hash(snapshot))
                commits.append(document)
            return atomic(path, document)

        with mock.patch.object(runner, 'atomic', side_effect=inspect_commit):
            self.publish(hot, 'recovery')
        self.assertEqual(1, len(commits))
        self.assertFalse((self.state / 'state.json').exists())

    def test_failed_snapshot_copy_cannot_advance_recovery_cursor(self):
        first = self.publish(self.hot(at=BASE - dt.timedelta(seconds=1), recovery_state={'cursor': 'old'}), 'recovery')
        with mock.patch.object(runner.shutil, 'copytree', side_effect=OSError('synthetic interrupted copy')):
            with self.assertRaises(OSError):
                self.publish(self.hot(recovery_state={'cursor': 'new'}), 'recovery')
        self.assertEqual(first['pointer'], self.pointer('recovery'))

    def test_failed_queue_or_dedup_write_cannot_advance_recovery_cursor(self):
        first = self.publish(self.hot(at=BASE - dt.timedelta(seconds=1), recovery_state={'cursor': 'old'}), 'recovery')
        atomic = runner.atomic
        for failing_path in (self.state / 'pending/recovery.json', self.state / 'dedup.json'):
            with self.subTest(failing_path=str(failing_path.relative_to(self.state))):
                def fail_selected_write(path, document):
                    if path == failing_path:
                        raise OSError('synthetic queue/dedup write failure')
                    return atomic(path, document)
                hot = self.hot(recovery_state={'cursor': 'new'}, views={'nu': {'commits': [{'sha': 'new'}]}})
                with mock.patch.object(runner, 'atomic', side_effect=fail_selected_write):
                    with self.assertRaises(OSError):
                        self.publish(hot, 'recovery')
                self.assertEqual(first['pointer'], self.pointer('recovery'))
                self.assertEqual({'cursor': 'old'}, runner.read(self.state / self.pointer('recovery')['snapshot'] / 'state.json'))

    def test_pointer_failure_keeps_candidates_durable_for_retry(self):
        self.publish(self.hot(at=BASE - dt.timedelta(seconds=1), recovery_state={'cursor': 'old'}), 'recovery')
        hot = self.hot(recovery_state={'cursor': 'new'}, views={'nu': {'commits': [{'sha': 'new'}]}})
        atomic = runner.atomic
        def fail_pointer(path, document):
            if path == self.state / 'pointers/recovery.json':
                raise OSError('synthetic pointer commit failure')
            return atomic(path, document)
        with mock.patch.object(runner, 'atomic', side_effect=fail_pointer):
            with self.assertRaises(OSError):
                self.publish(hot, 'recovery')
        queued = runner.read(self.state / 'pending/recovery.json')['new_or_changed_candidates']
        self.assertEqual(1, len(queued))
        result = self.publish(hot, 'recovery')
        self.assertTrue(result['accepted'])
        self.assertEqual(queued, runner.read(self.state / 'pending/recovery.json')['new_or_changed_candidates'])
        self.assertEqual({'cursor': 'new'}, runner.read(self.state / self.pointer('recovery')['snapshot'] / 'state.json'))

    def test_failed_process_does_not_replace_snapshot(self):
        first = self.publish(self.hot())
        process = mock.Mock(returncode=1, stderr='synthetic collector failure')
        with mock.patch.object(runner.subprocess, 'run', return_value=process):
            result = runner.run_one(self.state, 'github')
        self.assertFalse(result['accepted'])
        self.assertEqual('collector-failed', result['reason'])
        self.assertEqual(first['pointer'], self.pointer())
        self.assertEqual('collector-failed', runner.status(self.state)['feeds']['github']['last_attempt_reason'])


class HealthTests(StateCase):
    def test_unavailable_source_never_healthy(self):
        result = self.publish(self.hot(sources=[{'key': 'reddit', 'status': 'unavailable', 'errors': ['failed']}]))
        self.assertEqual('degraded', result['pointer']['health']['status'])
        self.assertEqual('unavailable', result['pointer']['health']['sources'][0]['status'])
        self.assertFalse((self.state / 'pointers/github.last-good.json').exists())

    def test_explicit_incomplete_source_never_healthy(self):
        result = self.publish(self.hot(sources=[{'key': 'reddit', 'status': 'ok', 'coverage_complete': False}]))
        self.assertEqual('degraded', result['pointer']['health']['status'])
        self.assertEqual('partial', result['pointer']['health']['sources'][0]['status'])

    def test_github_partial_errors_are_degraded_and_last_good_preserved(self):
        good = self.publish(self.hot(at=BASE - dt.timedelta(seconds=1)))['pointer']
        result = self.publish(self.hot(sources=[{'key': 'nu', 'head_sha': 'a', 'errors': ['workflow_runs failed']}]))
        self.assertEqual('degraded', result['pointer']['health']['status'])
        self.assertEqual(good, runner.read(self.state / 'pointers/github.last-good.json'))

    def test_public_partial_errors_cannot_be_reported_healthy(self):
        for mode, errors in (('json', ['hot JSON: source failed']), ('rss', ['new JSON: denied', 'hot RSS: source failed'])):
            with self.subTest(mode=mode):
                health = runner.source_health({'sources': [{'key': 'reddit', 'status': 'ok', 'acquisition_mode': mode, 'errors': errors}]}, self.root)
                self.assertEqual('partial', health[0]['status'])

    def test_successful_fallback_mirror_can_remain_healthy(self):
        for source in ({'key': 'reddit', 'status': 'ok', 'acquisition_mode': 'rss', 'errors': ['new JSON: denied', 'hot JSON: denied']},
                       {'key': 'tibo', 'status': 'ok', 'acquisition_mode': 'rsshub-chyi', 'errors': ['fxtwitter: denied']}):
            with self.subTest(source=source['key']):
                health = runner.source_health({'sources': [source]}, self.root)
                self.assertEqual('ok', health[0]['status'])

    def test_empty_media_with_acquisition_error_is_unavailable(self):
        result = self.publish(self.hot(sources=[{'key': 'reddit_media', 'status': 'empty', 'errors': ['RSS failed']}]), 'media')
        self.assertEqual('unavailable', result['pointer']['health']['sources'][0]['status'])

    def test_ocr_failure_is_partial(self):
        hot = self.hot(sources=[{'key': 'reddit_media', 'status': 'ok', 'view_path': 'views/reddit_media.json'}],
                       views={'reddit_media': {'items': [{'id': 'a', 'ocr_errors': ['engine missing']}]}})
        health = self.publish(hot, 'media')['pointer']['health']
        self.assertEqual('degraded', health['status'])
        self.assertEqual(1, health['sources'][0]['ocr_failed_candidates'])


class CandidateTests(StateCase):
    def test_repeat_old_candidate_suppressed_and_new_changed_kept(self):
        old = {'number': 1, 'title': 'old', 'state': 'open'}
        changed = {**old, 'state': 'closed'}
        fresh = {'number': 2, 'title': 'new', 'state': 'open'}
        first = self.publish(self.hot(at=BASE - dt.timedelta(seconds=2), views={'nu': {'issues': [old]}}))
        repeat = self.publish(self.hot(at=BASE - dt.timedelta(seconds=1), views={'nu': {'issues': [old]}}))
        update = self.publish(self.hot(views={'nu': {'issues': [changed, fresh]}}))
        self.assertEqual(1, first['new_or_changed_candidates'])
        self.assertEqual(0, repeat['new_or_changed_candidates'])
        self.assertEqual(2, update['new_or_changed_candidates'])
        pending = runner.read(self.state / 'pending/github.json')['new_or_changed_candidates']
        self.assertEqual(3, len(pending))
        self.assertEqual(3, len({(row['key'], row['version']) for row in pending}))

    def test_recovery_and_ordinary_same_commit_are_deduplicated(self):
        url = 'https://github.com/bryanedds/Nu/commit/abc'
        self.publish(self.hot(at=BASE - dt.timedelta(seconds=1), views={'nu': {'commits': [
            {'sha': 'abc', 'message': 'same', 'date': runner.stamp(BASE), 'url': url}]}}))
        result = self.publish(self.hot(views={'nu': {'events': [
            {'type': 'commit', 'sha': 'abc', 'title': 'same', 'timestamp': runner.stamp(BASE), 'url': url}]}}), 'recovery')
        self.assertEqual(0, result['new_or_changed_candidates'])

    def test_github_kinds_deduplicate_across_normal_and_recovery_shapes(self):
        timestamp = runner.stamp(BASE - dt.timedelta(minutes=1))
        cases = (
            ('issues', 'issue', {'number': 4, 'url': 'https://github.com/bryanedds/Nu/issues/4', 'updated_at': timestamp},
             {'number': 4, 'url': 'https://github.com/bryanedds/Nu/issues/4', 'timestamp': timestamp}),
            ('pulls', 'pull_request', {'number': 5, 'url': 'https://github.com/bryanedds/Nu/pull/5', 'updated_at': timestamp, 'recent_reviews': []},
             {'number': 5, 'url': 'https://github.com/bryanedds/Nu/pull/5', 'timestamp': timestamp}),
            ('releases', 'release', {'tag': 'v1', 'url': 'https://github.com/bryanedds/Nu/releases/tag/v1', 'published_at': timestamp, 'name': 'release'},
             {'tag': 'v1', 'url': 'https://github.com/bryanedds/Nu/releases/tag/v1', 'timestamp': timestamp, 'title': 'release'}),
            ('workflow_runs', 'workflow_run', {'id': 6, 'url': 'https://github.com/bryanedds/Nu/actions/runs/6', 'updated_at': timestamp, 'name': 'ci', 'conclusion': 'success'},
             {'id': 6, 'url': 'https://github.com/bryanedds/Nu/actions/runs/6', 'timestamp': timestamp, 'title': 'ci', 'conclusion': 'success'}),
        )
        normal = {group: [item] for group, kind, item, recovered in cases}
        recovered = {'events': [dict(item, type=kind) for group, kind, normal_item, item in cases]}
        first = self.publish(self.hot(at=BASE - dt.timedelta(seconds=2), views={'nu': normal}))
        repeat = self.publish(self.hot(at=BASE - dt.timedelta(seconds=1), views={'nu': recovered}), 'recovery')
        self.assertEqual(4, first['new_or_changed_candidates'])
        self.assertEqual(0, repeat['new_or_changed_candidates'])
        final = self.publish(self.hot(views={'nu': normal}))
        self.assertEqual(0, final['new_or_changed_candidates'])

    def test_github_updated_event_is_kept_after_recovery_dedup(self):
        url = 'https://github.com/bryanedds/Nu/issues/4'
        old = {'number': 4, 'url': url, 'state': 'open', 'updated_at': runner.stamp(BASE - dt.timedelta(minutes=1))}
        self.publish(self.hot(at=BASE - dt.timedelta(seconds=1), views={'nu': {'issues': [old]}}))
        new = {'type': 'issue', 'number': 4, 'url': url, 'state': 'closed', 'timestamp': runner.stamp(BASE)}
        result = self.publish(self.hot(views={'nu': {'events': [new]}}), 'recovery')
        self.assertEqual(1, result['new_or_changed_candidates'])

    def test_issue_and_pull_request_views_do_not_queue_same_pull_twice(self):
        pull = {'number': 5, 'url': 'https://github.com/bryanedds/Nu/pull/5', 'updated_at': runner.stamp(BASE)}
        result = self.publish(self.hot(views={'nu': {'issues': [dict(pull, kind='pull_request')], 'pulls': [pull]}}))
        self.assertEqual(1, result['new_or_changed_candidates'])
        self.assertEqual(1, len(runner.read(self.state / 'pending/github.json')['new_or_changed_candidates']))

    def test_dedup_expiry_prunes_old_unseen_entries(self):
        runner.atomic(self.state / 'dedup.json', {
            'stale': {'version': 'old', 'seen_at': runner.stamp(BASE - dt.timedelta(days=9))},
            'recent': {'version': 'keep', 'seen_at': runner.stamp(BASE - dt.timedelta(days=7))}})
        self.publish(self.hot())
        seen = runner.read(self.state / 'dedup.json')
        self.assertNotIn('stale', seen)
        self.assertIn('recent', seen)


class RetentionTests(StateCase):
    def test_genuine_backlog_survives_ordinary_and_no_backlog_runs(self):
        original = self.publish(self.hot(at=BASE - dt.timedelta(seconds=2), backlog=True, recovery_state={'cursor': 'backlog'}), 'recovery')['pointer']
        runner.retain_backlog(self.state)
        self.publish(self.hot(at=BASE - dt.timedelta(seconds=1)), 'github')
        self.publish(self.hot(recovery_state={'cursor': 'ordinary'}, backlog=False), 'recovery')
        result = runner.retain_backlog(self.state)
        batches = runner.read(self.state / 'retained.json')['batches']
        self.assertEqual(1, result['retained_batches'])
        self.assertEqual(original['sha256'], batches[0]['sha256'])
        self.assertEqual({'cursor': 'backlog'}, runner.read(self.state / batches[0]['snapshot'] / 'state.json'))

    def test_live_backlog_is_not_added_twice_or_extended(self):
        self.publish(self.hot(backlog=True), 'recovery')
        runner.retain_backlog(self.state)
        first = runner.read(self.state / 'retained.json')['batches']
        with mock.patch.object(runner, 'now', return_value=BASE + dt.timedelta(hours=1)):
            runner.retain_backlog(self.state)
        self.assertEqual(first, runner.read(self.state / 'retained.json')['batches'])

    def test_expired_backlog_is_not_resurrected_from_unchanged_pointer(self):
        self.publish(self.hot(backlog=True), 'recovery')
        runner.retain_backlog(self.state)
        with mock.patch.object(runner, 'now', return_value=BASE + dt.timedelta(hours=49)):
            result = runner.retain_backlog(self.state)
        self.assertEqual(0, result['retained_batches'])

    def test_multiple_backlog_batches_remain_until_individual_expiry(self):
        self.publish(self.hot(at=BASE - dt.timedelta(seconds=1), backlog=True), 'recovery')
        runner.retain_backlog(self.state)
        with mock.patch.object(runner, 'now', return_value=BASE + dt.timedelta(hours=1)):
            self.publish(self.hot(at=BASE + dt.timedelta(hours=1), backlog=True), 'recovery')
            runner.retain_backlog(self.state)
        self.assertEqual(2, len(runner.read(self.state / 'retained.json')['batches']))
        with mock.patch.object(runner, 'now', return_value=BASE + dt.timedelta(hours=48, minutes=30)):
            self.assertEqual(1, runner.retain_backlog(self.state)['retained_batches'])


class WorkerTests(unittest.TestCase):
    def test_anonymous_scope_allowlist_accepts_expected_public_urls(self):
        for url in ('https://api.github.com/repos/bryanedds/Nu',
                    'https://api.github.com/repos/asc-community/AngouriMath/issues?per_page=100',
                    'https://www.reddit.com/r/codex/new/.rss?limit=100',
                    'https://t.me/s/codex_resets',
                    'https://fxtwitter.com/thsottiaux/feed.xml',
                    'https://rsshub.chyi.org/twitter/user/thsottiaux',
                    'https://i.redd.it/synthetic.png'):
            with self.subTest(url=url):
                worker.validate_url(url)

    def test_allowlist_rejects_wrong_host_repo_protocol_userinfo_and_port(self):
        for url in ('http://api.github.com/repos/bryanedds/Nu',
                    'https://api.github.com.evil.test/repos/bryanedds/Nu',
                    'https://api.github.com/repos/other/private',
                    'https://api.github.com/user',
                    'https://www.reddit.com/r/other/new/.rss',
                    'https://t.me/s/other',
                    'https://synthetic:secret@api.github.com/repos/bryanedds/Nu',
                    'https://api.github.com:444/repos/bryanedds/Nu',
                    'file:///tmp/anything'):
            with self.subTest(url=url), self.assertRaises(ValueError):
                worker.validate_url(url)

    def test_public_url_cannot_include_authentication_query_parameters(self):
        for query in ('access_token=synthetic-secret', 'token=synthetic-secret', 'api_key=synthetic-secret'):
            with self.subTest(query=query), self.assertRaises(ValueError):
                worker.validate_url('https://api.github.com/repos/bryanedds/Nu?' + query)

    def test_redirect_cannot_escape_public_allowlist(self):
        request = urllib.request.Request('https://api.github.com/repos/bryanedds/Nu')
        with self.assertRaises(ValueError):
            worker.Redirects().redirect_request(request, None, 302, 'redirect', {}, 'https://evil.test/collect')

    def test_safe_environment_does_not_forward_tokens_or_cookies(self):
        fake = {'PATH': '/usr/bin', 'LANG': 'C.UTF-8', 'GH_TOKEN': 'synthetic-gh-secret',
                'GITHUB_TOKEN': 'synthetic-github-secret', 'OPENAI_API_KEY': 'synthetic-api-secret',
                'AUTHORIZATION': 'synthetic-auth-secret', 'COOKIE': 'synthetic-cookie-secret',
                'HOME': '/synthetic/with-auth', 'PYTHONPATH': '/synthetic/inject',
                'GIT_CONFIG_COUNT': '1', 'GIT_CONFIG_KEY_0': 'http.extraHeader',
                'GIT_CONFIG_VALUE_0': 'Authorization: synthetic-secret'}
        with mock.patch.dict(os.environ, fake, clear=True):
            env = runner.safe_env()
        self.assertEqual('/usr/bin', env['PATH'])
        self.assertEqual('/nonexistent', env['HOME'])
        self.assertFalse(any('secret' in value for value in env.values()))
        self.assertNotIn('PYTHONPATH', env)
        self.assertNotIn('GIT_CONFIG_COUNT', env)

    def run_worker(self, collector_callback, *, opener=None, max_requests=80):
        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary)
            fake_opener = opener or mock.Mock()
            fake_opener.open.side_effect = fake_opener.open.side_effect or AssertionError('Unexpected real transport')
            argv = ['worker.py', '/synthetic/collector.py', '--max-requests', str(max_requests)]
            with working_directory(path), mock.patch.object(worker.urllib.request, 'build_opener', return_value=fake_opener), \
                    mock.patch.object(worker.runpy, 'run_path', side_effect=lambda *a, **k: collector_callback()), \
                    mock.patch.object(worker.urllib.request, 'urlopen'), mock.patch('sys.argv', argv):
                worker.main()
            return json.loads((path / 'acquisition.json').read_text()), fake_opener

    def test_worker_rejects_auth_cookie_and_non_get_before_transport(self):
        for headers, method in (({'Authorization': 'Bearer synthetic-secret'}, 'GET'),
                                ({'Cookie': 'synthetic-secret'}, 'GET'),
                                ({'Proxy-Authorization': 'synthetic-secret'}, 'GET'), ({}, 'POST')):
            with self.subTest(headers=list(headers), method=method):
                def collector():
                    request = urllib.request.Request('https://api.github.com/repos/bryanedds/Nu', headers=headers, method=method)
                    with self.assertRaisesRegex(ValueError, 'anonymous'):
                        urllib.request.urlopen(request)
                log, opener = self.run_worker(collector)
                opener.open.assert_not_called()
                self.assertNotIn('synthetic-secret', json.dumps(log))

    def test_request_budget_is_bounded_and_does_not_reach_transport_after_cap(self):
        opener = mock.Mock()
        opener.open.side_effect = lambda *a, **k: worker.Response(b'{}', {})
        def collector():
            for _ in range(2):
                urllib.request.urlopen('https://api.github.com/repos/bryanedds/Nu')
            with self.assertRaisesRegex(RuntimeError, 'budget'):
                urllib.request.urlopen('https://api.github.com/repos/bryanedds/Nu')
        log, opener = self.run_worker(collector, opener=opener, max_requests=2)
        self.assertEqual(2, log['request_count'])
        self.assertEqual(2, opener.open.call_count)

    def test_empty_fixture_map_is_still_labeled_fixture(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary)
            (path / 'fixtures').mkdir()
            (path / 'fixtures/responses.json').write_text('{}')
            argv = ['worker.py', '/synthetic/collector.py', '--fixture-dir', str(path / 'fixtures')]
            with working_directory(path), mock.patch.object(worker.urllib.request, 'build_opener') as opener, \
                    mock.patch.object(worker.runpy, 'run_path'), mock.patch.object(worker.urllib.request, 'urlopen'), \
                    mock.patch('sys.argv', argv):
                worker.main()
                opener.return_value.open.assert_not_called()
            self.assertEqual('fixture', runner.read(path / 'acquisition.json')['mode'])

    def test_rate_limit_stops_same_host_and_records_cooldown(self):
        opener = mock.Mock()
        opener.open.side_effect = urllib.error.HTTPError('https://api.github.com/repos/bryanedds/Nu', 429, 'limited', {'Retry-After': '60'}, None)
        def collector():
            with self.assertRaises(urllib.error.HTTPError):
                urllib.request.urlopen('https://api.github.com/repos/bryanedds/Nu')
            with self.assertRaisesRegex(RuntimeError, 'cooldown'):
                urllib.request.urlopen('https://api.github.com/repos/asc-community/AngouriMath')
        log, opener = self.run_worker(collector, opener=opener)
        self.assertEqual(1, opener.open.call_count)
        self.assertIn('api.github.com', log['cooldowns'])
        self.assertEqual('error', log['requests'][0]['status'])


class ExtractionTests(unittest.TestCase):
    def test_every_collector_and_vendor_source_match_recorded_provenance(self):
        provenance = runner.read(ROOT / 'provenance.json')['collectors']
        self.assertEqual(set(runner.COLLECTORS), set(provenance))
        for key, entry in provenance.items():
            with self.subTest(collector=key):
                collector = (ROOT / 'collectors' / (key + '.py')).read_bytes()
                vendor = (ROOT / 'vendor/.github/workflows' / entry['workflow']).read_bytes()
                self.assertEqual(entry['collector_sha256'], hashlib.sha256(collector).hexdigest())
                self.assertEqual(entry['source_sha256'], hashlib.sha256(vendor).hexdigest())
                compile(collector, key, 'exec')


class RecoveryCollectorTests(unittest.TestCase):
    """Execute the actual extraction under an all-synthetic urllib transport."""

    def recovery(self, *, age_hours=6, partial_repo=False, fail_repo=False, malformed_feeds=False):
        current = dt.datetime.now(UTC)
        previous = runner.stamp(current - dt.timedelta(hours=age_hours))
        old_item = runner.stamp(current - dt.timedelta(days=8))
        recent_item = runner.stamp(current - dt.timedelta(minutes=30))
        requests = []

        def get(request, **kwargs):
            url = request.full_url if isinstance(request, urllib.request.Request) else request
            requests.append(url)
            parsed = urllib.parse.urlsplit(url)
            if parsed.hostname == 'api.github.com':
                if fail_repo and '/bryanedds/Nu' in parsed.path:
                    raise RuntimeError('synthetic source outage')
                if parsed.path.count('/') == 3:
                    value = {'default_branch': 'master'}
                elif parsed.path.endswith('/commits') and partial_repo and '/bryanedds/Nu' in parsed.path:
                    page = int(urllib.parse.parse_qs(parsed.query)['page'][0])
                    value = [{'sha': f'{page}-{i}', 'html_url': f'https://github.com/bryanedds/Nu/commit/{page}-{i}',
                              'commit': {'message': 'fixture', 'author': {'date': recent_item}}} for i in range(100)]
                elif parsed.path.endswith('/actions/runs'):
                    value = {'workflow_runs': []}
                else:
                    value = []
                body = json.dumps(value).encode()
            elif parsed.hostname == 'www.reddit.com':
                timestamp = 'invalid' if malformed_feeds else old_item
                body = f'<feed xmlns="http://www.w3.org/2005/Atom"><entry><id>old</id><published>{timestamp}</published><title>old</title></entry></feed>'.encode()
            elif parsed.hostname == 't.me':
                timestamp = 'invalid' if malformed_feeds else old_item
                body = f'<div class="tgme_widget_message" data-post="codex_resets/1"><time datetime="{timestamp}"></time><div class="tgme_widget_message_text">old</div></div>'.encode()
            else:
                timestamp = 'invalid' if malformed_feeds else old_item
                body = f'<rss><channel><item><title>old</title><pubDate>{timestamp}</pubDate><link>https://x.com/thsottiaux/status/1</link></item></channel></rss>'.encode()
            return worker.Response(body, {})

        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary)
            (path / 'hot/views').mkdir(parents=True)
            (path / 'previous').mkdir()
            state = {'schema_version': 1, 'sources': {key: {'last_success_at': previous}
                     for key in ('nu', 'angourimath', 'reddit', 'tibo', 'codex_resets')}}
            runner.atomic(path / 'previous/state.json', state)
            with working_directory(path), mock.patch.dict(os.environ, {}, clear=True), mock.patch('urllib.request.urlopen', side_effect=get):
                runpy.run_path(str(ROOT / 'collectors/recovery.py'), run_name='__main__')
            return (runner.read(path / 'hot/index.json'), runner.read(path / 'hot/state.json'), previous, requests)

    def test_complete_coverage_advances_cursor(self):
        index, state, previous, requests = self.recovery()
        self.assertTrue(index['has_backlog'])
        self.assertTrue(all(source['coverage_complete'] for source in index['sources']))
        for source in state['sources'].values():
            self.assertGreater(runner.parse(source['last_success_at']), runner.parse(previous))
        self.assertLess(len(requests), 30)

    def test_page_cap_preserves_incomplete_cursor_but_advances_other_sources(self):
        index, state, previous, requests = self.recovery(partial_repo=True)
        metadata = {source['key']: source for source in index['sources']}
        self.assertFalse(metadata['nu']['coverage_complete'])
        self.assertEqual(500, metadata['nu']['candidate_count'])
        self.assertEqual(previous, state['sources']['nu']['last_success_at'])
        self.assertGreater(runner.parse(state['sources']['angourimath']['last_success_at']), runner.parse(previous))
        commit_requests = [url for url in requests if '/bryanedds/Nu/commits?' in url]
        self.assertEqual(10, len(commit_requests))

    def test_failed_source_keeps_cursor_and_reports_unavailable(self):
        index, state, previous, requests = self.recovery(fail_repo=True)
        metadata = {source['key']: source for source in index['sources']}
        self.assertEqual('error', metadata['nu']['status'])
        self.assertFalse(metadata['nu']['coverage_complete'])
        self.assertEqual(previous, state['sources']['nu']['last_success_at'])

    def test_seven_day_lookback_cap_cannot_advance_cursor_past_missing_coverage(self):
        index, state, previous, requests = self.recovery(age_hours=9 * 24)
        for source in index['sources']:
            self.assertTrue(source['lookback_capped'])
            self.assertFalse(source['coverage_complete'])
            self.assertEqual(previous, state['sources'][source['key']]['last_success_at'])

    def test_malformed_feed_timestamps_cannot_establish_complete_coverage(self):
        index, state, previous, requests = self.recovery(malformed_feeds=True)
        metadata = {source['key']: source for source in index['sources']}
        for key in ('reddit', 'tibo', 'codex_resets'):
            with self.subTest(source=key):
                self.assertFalse(metadata[key]['coverage_complete'])
                self.assertEqual(previous, state['sources'][key]['last_success_at'])


if __name__ == '__main__':
    unittest.main()
