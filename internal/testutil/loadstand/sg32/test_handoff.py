"""Finite active-handoff controls; no Docker, Linux mount or DAC emulation."""
import contextlib
import copy
import errno
import json
import os
from pathlib import Path
import types
import unittest
from unittest import mock

HERE = Path(__file__).resolve().parent
N, CLI, PREP, SERVICE = 'a' * 32, 'b' * 64, '1' * 64, '2' * 64


def load(name):
    path = HERE / (name + '.py')
    module = types.ModuleType('handoff_pure_' + name)
    module.__file__ = str(path)
    exec(compile(path.read_bytes(), str(path), 'exec'), module.__dict__)
    return module


class PrefixReached(Exception):
    pass


class HostBoundary:
    """Only the finite helper's CLI/child boundary, not a Docker backend model."""
    def __init__(self, test, host, base, case):
        self.test, self.h, self.base, self.case = test, host, base, case
        self.record_reader = host.Child.records
        self.now, self.events, self.started, self.removed = 100., [], False, False
        self.names, self.ids = host.volume_names(N), [PREP]
        self.manifest = {'nonce': N, 'files': {'opaque': {'bytes': 1, 'sha256': CLI}}}
        self.prep = self.child('preparer', [{'kind': 'PREPARED', 'nonce': N, 'files': self.manifest['files']}])
        if case == 'prep-early-exit':
            self.prep.p.returncode = 0
        attached = {'kind': 'SECRET_ATTACHED', 'nonce': N}
        if case == 'foreign-attachment':
            attached['nonce'] = 'c' * 32
        rows = [attached, copy.deepcopy(attached)] if case == 'duplicate-attachment' else [attached]
        self.service = self.child('service', rows)

    def child(self, role, rows):
        owner = self
        class Child:
            def __init__(self):
                self.rows, self.err = rows, bytearray()
                self.p = types.SimpleNamespace(returncode=None)
                self.p.poll = lambda: self.p.returncode
                self.p.wait = self.wait
            def records(self, complete=False):
                if role == 'service' and owner.case == 'records-cross-deadline' and 'prep-removed' in owner.events:
                    owner.now = 116.
                raw = b''.join(json.dumps(row).encode() + b'\n' for row in self.rows)
                partial = (role == 'preparer' and owner.case == 'prepared-partial-tail') or (role == 'service' and (owner.case == 'attachment-partial-tail' or (owner.case == 'partial-tail-after-release' and 'prep-removed' in owner.events)))
                self.out = bytearray(raw + (b'{\"pending\":' if partial else b''))
                return owner.record_reader(self, complete)
            def done(self):
                return self.p.returncode is not None
            def wait(self, timeout):
                owner.events.append('prep-reaped')
                return None if owner.case == 'unconfirmed-reap' else self.p.returncode
            def finish_input(self):
                owner.events.append(role + '-input-closed')
            def send(self, raw):
                packet = json.loads(raw)
                owner.test.assertEqual(packet, {'kind': 'RELEASE' if role == 'preparer' else 'CONTINUE', 'nonce': N})
                owner.events.append(packet['kind'])
                if role == 'preparer':
                    self.p.returncode = 2 if owner.case == 'prep-nonzero' else 0
                    if owner.case == 'service-loss-after-release':
                        owner.service.p.returncode = 2
                    if owner.case == 'extra-prefix-after-release':
                        owner.service.rows.append({'kind': 'ERROR', 'nonce': N})
        return Child()

    def start(self, engine, args, role, interactive):
        self.test.assertIs(engine, self)
        self.test.assertEqual((args, role, interactive), (['start', '-a', '-i', SERVICE], 'service', True))
        self.events.append('service-started')
        self.started = True
        return self.service

    def pump_until(self, predicate, deadline):
        self.test.assertLessEqual(deadline, 115.)
        if not predicate():
            raise self.h.Fault('controlled-pump-no-accepted-record')

    def command(self, args, deadline, mutation=False):
        self.test.assertLessEqual(deadline, 115.)
        if args and args[0] == 'create':
            self.test.assertEqual(args, self.h.create_argv('service', N, self.names, cli_sha=CLI))
            self.test.assertTrue(mutation)
            self.events.append('service-created')
            if self.case == 'original-deadline':
                self.now = 116.
            return (SERVICE + '\n').encode()
        if args == ['rm', PREP]:
            self.test.assertTrue(mutation)
            self.events.append('prep-removed')
            self.removed = True
            return (PREP + '\n').encode()
        if args == ['ps', '-a', '--no-trunc', '--filter', 'id=' + PREP, '--format', '{{.ID}}']:
            self.events.append('prep-absence')
            return (PREP + '\n').encode() if self.case == 'absence-failed' else b''
        raise AssertionError('helper issued an unexpected command: ' + repr(args))

    def inspect(self, ident, deadline):
        self.test.assertLessEqual(deadline, 115.)
        if ident == PREP:
            code = self.prep.p.returncode
            return {'State': {'Running': code is None, 'ExitCode': code,
                              'OOMKilled': self.case == 'prep-oom' and code is not None}}
        self.test.assertEqual(ident, SERVICE)
        if not self.started:
            return self.base.container_meta('service', self.names)
        return {'State': {'Running': self.service.p.returncode is None, 'ExitCode': self.service.p.returncode,
                          'OOMKilled': self.case == 'service-oom'}}


class HandoffPureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base = load('test_sg32')
        cls.c, cls.clock, cls.s, cls.h = cls.base.subjects()

    def test_01_borrowed_control_fd_framing_original_deadline_and_mode_restore(self):
        s = self.s
        line = b'{"kind":"RELEASE","nonce":"' + N.encode() + b'"}\n'
        for case in ('valid', 'eof', 'extra-line', 'oversize', 'expired', 'read-race'):
            with self.subTest(control=case):
                readfd, writefd = os.pipe()
                try:
                    payload = b'' if case == 'eof' else line + line if case == 'extra-line' else b'x' * 33 + b'\n' if case == 'oversize' else line
                    os.write(writefd, payload)
                    os.close(writefd); writefd = None
                    stream = types.SimpleNamespace(fileno=lambda: readfd)
                    before = os.get_blocking(readfd)
                    fake_os = types.SimpleNamespace(**vars(os))
                    if case == 'read-race':
                        reader = os.read
                        attempts = []
                        def read(fd, count):
                            attempts.append(fd)
                            if len(attempts) == 1:
                                raise BlockingIOError(errno.EAGAIN, 'controlled pipe race')
                            return reader(fd, count)
                        fake_os.read = read
                    with mock.patch.object(s, 'os', fake_os), mock.patch.object(s, 'time', types.SimpleNamespace(monotonic=lambda: 100.)):
                        if case in ('valid', 'read-race'):
                            self.assertEqual(s.bounded_control_line(stream, 101.), line)
                        else:
                            with self.assertRaises(s.Fault):
                                s.bounded_control_line(stream, 100. if case == 'expired' else 101., 32 if case == 'oversize' else 4096)
                    self.assertEqual(os.get_blocking(readfd), before)
                    self.assertIsInstance(os.fstat(readfd), os.stat_result)  # Borrowed FD remains open.
                finally:
                    os.close(readfd)
                    if writefd is not None:
                        os.close(writefd)

    def test_02_actual_preparer_prepared_is_nonterminal_release_only_no_later_writes(self):
        s = self.s
        for case in ('release', 'foreign-nonce', 'extra-field', 'eof', 'late'):
            with self.subTest(preparer=case), self.base.PreparerFiles(s) as fixture:
                now, calls, after_prepared = [100.], [], []
                original = s.preparer_call
                def call(operation, path, function, *args):
                    if fixture.records:
                        after_prepared.append(operation)
                    return original(operation, path, function, *args)
                def control_line(stream, deadline, maximum=4096):
                    calls.append((deadline, maximum))
                    self.assertEqual(deadline, 120.)
                    if len(calls) == 1:
                        self.assertEqual(maximum, 8192)
                        self.assertFalse(fixture.records)
                        return json.dumps(fixture.config).encode() + b'\n'
                    self.assertEqual([r['kind'] for r in fixture.records], ['PREPARED'])
                    if case == 'eof':
                        raise s.Fault('handoff-control-eof')
                    if case == 'late':
                        now[0] = 121.
                    packet = {'kind': 'RELEASE', 'nonce': 'c' * 32 if case == 'foreign-nonce' else N}
                    if case == 'extra-field':
                        packet['extra'] = True
                    return json.dumps(packet).encode() + b'\n'
                with mock.patch.object(s, 'bounded_control_line', side_effect=control_line), mock.patch.object(s, 'preparer_call', side_effect=call), mock.patch.object(s, 'time', types.SimpleNamespace(monotonic=lambda: now[0])):
                    if case == 'release':
                        self.assertEqual(s.preparer(N), 0)
                    else:
                        with self.assertRaises(s.Fault):
                            s.preparer(N)
                self.assertEqual(calls, [(120., 8192), (120., 4096)])
                self.assertEqual(after_prepared, ['stdin-read'])
                self.assertFalse(fixture.live)

    def test_03_actual_keeper_attachment_continue_second_read_and_no_early_children(self):
        s = self.s
        for case in ('same', 'wrong-command', 'foreign-nonce', 'changed-secret', 'second-read-error', 'late', 'first-read-error'):
            with self.subTest(keeper=case):
                now, reads, records = [100.], [], []
                primary = PermissionError(errno.EACCES, 'private text', '/run/c1/auth.json')
                def secret():
                    reads.append('read')
                    if case == 'first-read-error' or (case == 'second-read-error' and len(reads) == 2):
                        raise primary
                    return {'username': 'test', 'password': 'different' if case == 'changed-secret' and len(reads) == 2 else 'private'}
                def continuation(stream, deadline):
                    self.assertEqual(deadline, 120.)
                    self.assertEqual(reads, ['read'])
                    self.assertEqual(records, [{'kind': 'SECRET_ATTACHED', 'nonce': N}])
                    if case == 'late':
                        now[0] = 121.
                    return json.dumps({'kind': 'RELEASE' if case == 'wrong-command' else 'CONTINUE',
                                       'nonce': 'c' * 32 if case == 'foreign-nonce' else N}).encode() + b'\n'
                group = mock.Mock(side_effect=PrefixReached())
                child = mock.Mock(side_effect=AssertionError('child before completed handoff'))
                fake_sys = types.SimpleNamespace(**vars(s.sys))
                fake_sys.stdin = types.SimpleNamespace(buffer=object())
                with contextlib.ExitStack() as stack:
                    replacements = {'os': types.SimpleNamespace(getpid=lambda: 1, getuid=lambda: 501, getgid=lambda: 20),
                                    'time': types.SimpleNamespace(monotonic=lambda: now[0]),
                                    'sys': fake_sys,
                                    'qualified_clock': lambda *args: {'contexts': {'thread_self': {'time': [5, 7]}}},
                                    'read_secret': secret, 'bounded_control_line': continuation,
                                    'emit': lambda value: records.append(copy.deepcopy(value)), 'Cgroup': group, 'OwnedChild': child}
                    for name, value in replacements.items():
                        stack.enter_context(mock.patch.object(s, name, value))
                    expected = PrefixReached if case == 'same' else PermissionError if case in ('second-read-error', 'first-read-error') else s.Fault
                    with self.assertRaises(expected) as caught:
                        s.keeper(N, CLI)
                child.assert_not_called()
                if case == 'same':
                    group.assert_called_once_with()
                    self.assertEqual(reads, ['read', 'read'])
                else:
                    group.assert_not_called()
                if case in ('second-read-error', 'first-read-error'):
                    self.assertIs(caught.exception, primary)
                self.assertEqual(len(records), 0 if case == 'first-read-error' else 1)

    def test_04_actual_host_handoff_order_and_causal_refusals_before_continue(self):
        h = self.h
        for case in ('success', 'prep-early-exit', 'foreign-attachment', 'duplicate-attachment',
                     'prep-nonzero', 'unconfirmed-reap', 'prep-oom', 'service-oom', 'service-loss-after-release',
                     'extra-prefix-after-release', 'absence-failed', 'original-deadline',
                     'release-encode-deadline', 'continue-encode-deadline', 'records-cross-deadline',
                     'prepared-partial-tail', 'attachment-partial-tail', 'partial-tail-after-release'):
            with self.subTest(host=case):
                boundary = HostBoundary(self, h, self.base, case)
                encode = h.encode
                def delayed_encode(packet, maximum=65536):
                    raw = encode(packet, maximum)
                    if (case == 'release-encode-deadline' and packet['kind'] == 'RELEASE') or (case == 'continue-encode-deadline' and packet['kind'] == 'CONTINUE'):
                        boundary.now = 116.
                    return raw
                with mock.patch.object(h, 'Child', side_effect=boundary.start), mock.patch.object(h, 'time', types.SimpleNamespace(monotonic=lambda: boundary.now)), mock.patch.object(h, 'encode', side_effect=delayed_encode):
                    if case == 'success':
                        result = h.handoff_secret(boundary, boundary.prep, PREP, N, boundary.manifest, boundary.names, boundary.ids, CLI, 280., 115.)
                        self.assertEqual(result, (SERVICE, boundary.service, 120.))
                    else:
                        with self.assertRaises(h.Fault):
                            h.handoff_secret(boundary, boundary.prep, PREP, N, boundary.manifest, boundary.names, boundary.ids, CLI, 280., 115.)
                if case != 'success':
                    self.assertNotIn('CONTINUE', boundary.events)
                if case == 'success':
                    ordered = ('service-created', 'service-started', 'RELEASE', 'prep-reaped', 'prep-removed', 'prep-absence', 'CONTINUE')
                    self.assertEqual([event for event in boundary.events if event in ordered], list(ordered))
                    self.assertEqual(boundary.ids, [PREP, SERVICE])

    def test_05_composed_service_reducer_requires_exact_first_attachment(self):
        records = self.base.complete_service_records(self.s)
        self.assertEqual(records[0], {'kind': 'SECRET_ATTACHED', 'nonce': N})
        result = self.c.verify_service(records, N, CLI)
        self.assertEqual(result['peak_bytes'], 2004)
        self.assertEqual(result['keeper_exit_tail'], 'UNKNOWN')
        for case in ('missing', 'duplicate', 'late', 'extra-field', 'initial-oom'):
            with self.subTest(reducer=case):
                bad = copy.deepcopy(records)
                if case == 'missing':
                    bad.pop(0)
                elif case == 'duplicate':
                    bad.insert(1, copy.deepcopy(bad[0]))
                elif case == 'late':
                    bad[0], bad[1] = bad[1], bad[0]
                elif case == 'extra-field':
                    bad[0]['unexpected'] = 'not allowed'
                else:
                    next(row for row in bad if row['kind'] == 'READY')['initial']['events_before']['oom'] = 1
                with self.assertRaises(self.c.Fault):
                    self.c.verify_service(bad, N, CLI)


if __name__ == '__main__':
    unittest.main()
