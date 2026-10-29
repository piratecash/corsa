"""Finite keeper startup labels; controlled call boundaries, no guest DAC model."""
import contextlib
import copy
import errno
from pathlib import Path
import sys
import types
import unittest
from unittest import mock

HERE = Path(__file__).resolve().parent
NONCE, CLI_SHA = 'a' * 32, 'b' * 64
SECRET, SECRET_PATH = 'private-argument-never-log', '/private/secret-filename'
STAGES = ('clock-before', 'read-secret', 'read-secret-after-handoff', 'cgroup-open', 'process-witness-open',
          'initial-cgroup-snapshot', 'selector-open', 'sampler-spawn')


def load_subject(name):
    path = HERE / (name + '.py')
    if name == 'clockprobe' and not path.exists():
        path = HERE.parent / 'clockprobe' / 'probe.py'
    module = types.ModuleType('sg32_keeper_diagnostic_subject_' + name)
    module.__file__ = str(path)
    exec(compile(path.read_bytes(), str(path), 'exec'), module.__dict__)
    return module


class PrefixReached(Exception):
    pass


class KeeperDiagnosticPureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        common, clock = load_subject('sg32_common'), load_subject('clockprobe')
        with mock.patch.dict(sys.modules, {'sg32_common': common, 'clockprobe': clock}):
            cls.service = load_subject('sg32_service')

    def test_01_call_passthrough_closed_label_strict_errno_and_primary_identity(self):
        s = self.service
        argument, value = object(), object()
        function = mock.Mock(return_value=value)
        self.assertIs(s.keeper_startup_call('read-secret', function, argument, named=SECRET), value)
        function.assert_called_once_with(argument, named=SECRET)
        function.reset_mock()
        with self.assertRaisesRegex(s.Fault, '^keeper-startup-diagnostic-label$'):
            s.keeper_startup_call(SECRET, function)
        function.assert_not_called()
        non_os = ValueError('ordinary controlled failure')
        with self.assertRaises(ValueError) as caught:
            s.keeper_startup_call('read-secret', mock.Mock(side_effect=non_os))
        self.assertIs(caught.exception, non_os)
        self.assertFalse(hasattr(non_os, 'keeper_diagnostic'))
        for number in (errno.EACCES, True, 4096):
            with self.subTest(errno=number):
                primary = PermissionError(errno.EACCES, SECRET, SECRET_PATH)
                primary.errno = number
                with self.assertRaises(PermissionError) as caught:
                    s.keeper_startup_call('read-secret', mock.Mock(side_effect=primary), SECRET)
                self.assertIs(caught.exception, primary)
                facts = primary.keeper_diagnostic
                expected = number if type(number) is int and number <= 4095 else None
                self.assertEqual(facts, {'scope': 'KEEPER_STARTUP_OPERATION', 'operation': 'read-secret', 'errno': expected, 'path_label': None})
                self.assertIs(type(facts['errno']), int if expected is not None else type(None))
                for private in (SECRET, SECRET_PATH):
                    self.assertNotIn(private.encode(), s.encode(facts, 1024))

        for case, filename, label in (
                ('known-metric', 'memory.peak', 'cgroup-metric:memory.peak'),
                ('known-auth', '/run/c1/auth.json', 'auth'),
                ('secret-filename', SECRET_PATH, None),
                ('unlisted-path', '/proc/1/unlisted', None),
                ('integer-fd', 23, None)):
            with self.subTest(path_label=case):
                primary = PermissionError(errno.EACCES, SECRET, filename)
                with self.assertRaises(PermissionError) as caught:
                    s.keeper_startup_call('cgroup-open', mock.Mock(side_effect=primary))
                self.assertIs(caught.exception, primary)
                self.assertEqual(primary.keeper_diagnostic,
                                 {'scope': 'KEEPER_STARTUP_OPERATION', 'operation': 'cgroup-open',
                                  'errno': errno.EACCES, 'path_label': label})
                encoded = s.encode(primary.keeper_diagnostic, 1024)
                self.assertNotIn(SECRET.encode(), encoded)
                self.assertNotIn(SECRET_PATH.encode(), encoded)
                self.assertNotIn(b'/proc/1/unlisted', encoded)
                self.assertEqual(set(primary.keeper_diagnostic), {'scope', 'operation', 'errno', 'path_label'})

    def test_02_actual_keeper_prefix_order_arguments_and_each_stage_failure(self):
        s = self.service
        for failed_stage in (*STAGES, None):
            with self.subTest(failed_stage=failed_stage):
                calls, entered, exited = [], [], []
                primary = PermissionError(errno.EACCES, SECRET, SECRET_PATH)
                domain, group_raw = [5, 7], b'0::/controlled-service-group\n'

                class Context:
                    def __init__(self, label):
                        self.label, self.raw = label, group_raw
                    def __enter__(self):
                        entered.append(self.label)
                        return self
                    def __exit__(self, *unused):
                        exited.append(self.label)
                    def snapshot(self, **kwargs):
                        return call('initial-cgroup-snapshot', {'members': [1]}, **kwargs)
                    def register(self, stream, events, data):
                        self_test.assertIs(stream, child.p.stdout)
                        raise PrefixReached()

                self_test = self
                child = types.SimpleNamespace(p=types.SimpleNamespace(stdout=object(), stderr=object()))

                def call(stage, value, *args, **kwargs):
                    calls.append((stage, args, kwargs))
                    if stage == failed_stage:
                        raise primary
                    return value

                secret_reads, attachment = [], []
                def secret():
                    stage = 'read-secret' if not secret_reads else 'read-secret-after-handoff'
                    secret_reads.append(stage)
                    return call(stage, {'username': 'user', 'password': SECRET})
                replacements = {
                    'os': types.SimpleNamespace(getpid=lambda: 1, getuid=lambda: 501, getgid=lambda: 20),
                    'time': types.SimpleNamespace(monotonic=lambda: 100),
                    'qualified_clock': lambda *args: call('clock-before', {'contexts': {'thread_self': {'time': domain}}}, *args),
                    'read_secret': secret,
                    'bounded_control_line': lambda *args: s.encode({'kind': 'CONTINUE', 'nonce': NONCE},4096),
                    'emit': lambda row: attachment.append(copy.deepcopy(row)),
                    'Cgroup': lambda: call('cgroup-open', Context('group')),
                    'ProcWitness': lambda *args: call('process-witness-open', Context('witness'), *args),
                    'selectors': types.SimpleNamespace(DefaultSelector=lambda: call('selector-open', Context('selector')), EVENT_READ=1),
                    # This seam returns a marker, not a real child or an owned-child cleanup proof.
                    'OwnedChild': lambda *args: call('sampler-spawn', child, *args),
                }
                with contextlib.ExitStack() as stack:
                    for name, value in replacements.items():
                        stack.enter_context(mock.patch.object(s, name, value))
                    with self.assertRaises(PermissionError if failed_stage else PrefixReached) as caught:
                        s.keeper(NONCE, CLI_SHA)
                count = STAGES.index(failed_stage) + 1 if failed_stage else len(STAGES)
                self.assertEqual([row[0] for row in calls], list(STAGES[:count]))
                self.assertEqual(calls[0][1:], (('keeper', 'sg32-keeper-before', NONCE), {}))
                self.assertEqual(attachment, [{"kind": "SECRET_ATTACHED", "nonce": NONCE}] if count >= 3 else [])
                if count >= 5:
                    self.assertEqual(calls[4][1:], ((1, group_raw, domain), {}))
                if count >= 6:
                    self.assertEqual(calls[5][1:], ((), {'initial': True}))
                if count == 8:
                    args = calls[-1][1]
                    self.assertEqual(args[0][:4], ['/usr/bin/python3', '-I', '-S', '-B'])
                    self.assertEqual(args[0][-4:], ['sampler', NONCE, CLI_SHA, '1'])
                    self.assertEqual(args[1:], ([], {'PATH': '/usr/bin:/bin', 'LC_ALL': 'C'}, True))
                self.assertEqual(exited, list(reversed(entered)))
                if failed_stage:
                    self.assertIs(caught.exception, primary)
                    self.assertEqual(primary.keeper_diagnostic,
                                     {'scope': 'KEEPER_STARTUP_OPERATION', 'operation': failed_stage, 'errno': errno.EACCES, 'path_label': None})
                else:
                    self.assertIsInstance(caught.exception, PrefixReached)

    def test_03_main_bounded_keeper_facts_and_existing_precedence(self):
        s = self.service
        for kind in ('keeper', 'preparer-priority', 'adverse-priority'):
            with self.subTest(routing=kind):
                primary = PermissionError(errno.EACCES, SECRET, SECRET_PATH)
                with self.assertRaises(PermissionError):
                    s.keeper_startup_call('cgroup-open', mock.Mock(side_effect=primary))
                expected = primary.keeper_diagnostic
                if kind == 'preparer-priority':
                    primary.preparer_diagnostic = {'scope': 'PREPARER_OPERATION', 'operation': 'open', 'errno': errno.EACCES}
                    expected = primary.preparer_diagnostic
                elif kind == 'adverse-priority':
                    primary = s.Adverse('controlled-adverse', {'scope': 'KNOWN_ADVERSE_PREFIX'})
                    primary.keeper_diagnostic = {'scope': 'MUST_NOT_REPLACE_ADVERSE'}
                    expected = primary.facts
                records = []
                with mock.patch.object(s, 'dispatch', side_effect=primary), mock.patch.object(s, 'emit', side_effect=lambda row: records.append(copy.deepcopy(row))):
                    self.assertEqual(s.main(['keeper', NONCE, CLI_SHA]), 2)
                self.assertEqual(len(records), 1)
                row = records[0]
                self.assertEqual((row['kind'], row['nonce'], row['status']),
                                 ('ERROR', NONCE, 'FAIL' if kind == 'adverse-priority' else 'UNKNOWN'))
                self.assertEqual(row['facts'], expected)
                self.assertEqual(row['reason'], 'controlled-adverse' if kind == 'adverse-priority' else 'PermissionError')
                self.assertFalse(row['cleanup_failed'])
                self.assertIsNone(row['observed_node_exit'])
                self.assertEqual(row['child_exits'], [])
                raw = s.encode(row, 2048)
                self.assertLessEqual(len(raw), 2048)
                for private in (SECRET, SECRET_PATH):
                    self.assertNotIn(private.encode(), raw)


if __name__ == '__main__':
    unittest.main()
