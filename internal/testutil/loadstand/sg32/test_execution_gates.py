"""Finite pure pre-import and last-moment budget controls; no real runtime."""
from pathlib import Path
import contextlib
import hashlib
import io
import json
import sys
import tempfile
import types
import unittest
from unittest import mock

HERE = Path(__file__).resolve().parent


def subject(name, filename):
    path = HERE / filename
    if not path.is_file():
        path = HERE.parent / 'sg32' / filename
    module = types.ModuleType(name)
    module.__file__ = str(path)
    exec(compile(path.read_bytes(), str(path), 'exec'), module.__dict__)
    return module


BOOT = subject('sg32_gate_bootstrap_subject', 'bootstrap.py')
with mock.patch.dict(sys.modules):
    sys.modules['sg32_common'] = subject('sg32_common', 'sg32_common.py')
    HOST = subject('sg32_gate_host_subject', 'sg32_host.py')


class Bundle:
    """Own ordinary-byte fixture; fake artifact bytes are not qualified ELF."""
    def __init__(self, changed_read=False):
        self.temp = tempfile.TemporaryDirectory(prefix='sg32-gate-pure-')
        self.root = Path(self.temp.name).resolve(strict=True)
        self.source = self.root / 'source'
        self.source.mkdir(mode=0o700)
        self.admission = self.root / 'admission.json'
        self.common = b"MARKER = 'captured-common-exact'\n"
        read_expr = "Path(path).read_bytes() + b'changed'" if changed_read else "Path(path).read_bytes()"
        self.host = ("import sg32_common\nfrom pathlib import Path\n"
                     "def read_admission(path):\n    return " + read_expr + ", {}\n"
                     "def main(args, started_at=None):\n    return 73 if sg32_common.MARKER == 'captured-common-exact' and started_at == 91.0 else 99\n").encode()
        self.write(self.source / 'bootstrap.py', Path(BOOT.__file__).read_bytes())
        self.write(self.source / 'sg32_common.py', self.common)
        self.write(self.source / 'sg32_host.py', self.host)
        files = {}
        for name in ('corsad', 'c2driver', 'sg32_common.py', 'sg32_service.py', 'clockprobe.py'):
            path = self.source / name
            if name != 'sg32_common.py':
                self.write(path, ('ordinary fixture for ' + name + '\n').encode())
            raw = path.read_bytes()
            files[name] = {'path': str(path), 'sha256': self.sha(raw), 'bytes': len(raw)}
        self.q = {'status': 'QUALIFIED_CURRENT_CLI_AND_REBOUND_DRIVER',
                  'source_commit': '8afa7f5b365a56d55f116c8a0dfb0ac581c79000',
                  'platform': 'linux-arm64-v8.0-cgo0', 'build_exit': 0,
                  'driver_source_reviewed': True, 'tester_accepted': True, 'reviewer_accepted': True,
                  'corsad_sha256': files['corsad']['sha256'], 'c2driver_sha256': files['c2driver']['sha256'],
                  'driver_compiled_cli_sha256': files['corsad']['sha256']}
        qp = self.root / 'qualification.json'
        self.write(qp, self.encode(self.q))
        self.cfg = {'profile': 'SG32-v1', 'owner_approved': True, 'allow_recovery_hold': True,
                    'one_attempt': True, 'inputs_ready': True, 'files': files,
                    'qualification_path': str(qp), 'qualification_sha256': self.sha(qp.read_bytes()),
                    'host_sha256': self.sha(self.host),
                    'bootstrap_sha256': self.sha((self.source / 'bootstrap.py').read_bytes())}
        self.save()

    @staticmethod
    def sha(raw):
        return hashlib.sha256(raw).hexdigest()

    @staticmethod
    def encode(value):
        return (json.dumps(value, sort_keys=True) + '\n').encode()

    @staticmethod
    def write(path, raw):
        path.write_bytes(raw)
        path.chmod(0o600)

    def save(self):
        self.write(self.admission, self.encode(self.cfg))

    @contextlib.contextmanager
    def bound(self):
        with mock.patch.object(BOOT, 'SOURCE_ROOT', self.source), \
             mock.patch.object(BOOT, '__file__', str(self.source / 'bootstrap.py')):
            yield self

    def close(self):
        self.temp.cleanup()


class ExecutionGatePureTests(unittest.TestCase):
    def bundle(self, **kwargs):
        value = Bundle(**kwargs)
        self.addCleanup(value.close)
        return value

    def test_01_owner_and_inputs_refuse_before_subject_loading(self):
        for gate in ('owner_approved', 'inputs_ready'):
            with self.subTest(gate=gate):
                b = self.bundle()
                b.cfg[gate] = False
                b.save()
                with b.bound(), mock.patch.object(BOOT, '_load_validated', side_effect=AssertionError('subject loaded')) as load, contextlib.redirect_stdout(io.StringIO()) as out:
                    self.assertEqual(BOOT.main([str(b.admission)]), 2)
                load.assert_not_called()
                self.assertEqual(json.loads(out.getvalue())['status'], 'NOT_STARTED_BOOTSTRAP_REFUSED')
                self.assertNotIn('sg32_host', sys.modules)

    def test_02_actual_path_and_hash_failures_precede_loader(self):
        for case in ('common-path', 'host-sha', 'bootstrap-sha', 'input-body', 'qualification-sha', 'entry-file-path'):
            with self.subTest(binding=case):
                b = self.bundle()
                file_override = None
                if case == 'common-path':
                    alternate = b.root / 'same-common.py'
                    b.write(alternate, b.common)
                    b.cfg['files']['sg32_common.py']['path'] = str(alternate)
                elif case == 'host-sha':
                    b.cfg['host_sha256'] = '0' * 64
                elif case == 'bootstrap-sha':
                    b.cfg['bootstrap_sha256'] = '0' * 64
                elif case == 'input-body':
                    b.write(b.source / 'corsad', b'changed ordinary bytes\n')
                elif case == 'qualification-sha':
                    b.cfg['qualification_sha256'] = '0' * 64
                else:
                    file_override = b.source / 'other-bootstrap.py'
                    b.write(file_override, (b.source / 'bootstrap.py').read_bytes())
                b.save()
                with b.bound(), mock.patch.object(BOOT, '_load_validated', side_effect=AssertionError('subject loaded')) as load, contextlib.ExitStack() as stack, contextlib.redirect_stdout(io.StringIO()) as out:
                    if file_override is not None:
                        stack.enter_context(mock.patch.object(BOOT, '__file__', str(file_override)))
                    self.assertEqual(BOOT.main([str(b.admission)]), 2)
                load.assert_not_called()
                self.assertEqual(json.loads(out.getvalue())['status'], 'NOT_STARTED_BOOTSTRAP_REFUSED')

    def test_03_stale_modules_and_partial_loader_ownership(self):
        for case in ('sg32_common', 'sg32_host', 'partial-syntax-error'):
            with self.subTest(module=case):
                b = self.bundle()
                with b.bound(), mock.patch.dict(sys.modules):
                    sys.modules.pop('sg32_common', None)
                    sys.modules.pop('sg32_host', None)
                    if case == 'partial-syntax-error':
                        with self.assertRaises(SyntaxError):
                            BOOT._load_validated(b.common, b'def invalid(\n')
                        self.assertNotIn('sg32_common', sys.modules)
                        self.assertNotIn('sg32_host', sys.modules)
                    else:
                        foreign = types.ModuleType(case)
                        foreign.keep = object()
                        sys.modules[case] = foreign
                        with self.assertRaisesRegex(BOOT.Fault, 'subject-already-loaded'):
                            BOOT._load_validated(b.common, b.host)
                        self.assertIs(sys.modules[case], foreign)
                        self.assertNotIn('sg32_host' if case == 'sg32_common' else 'sg32_common', sys.modules)

    def test_04_exact_captured_marker_delegation_and_admission_recheck(self):
        for case in ('normal', 'captured-bytes', 'admission-recheck'):
            with self.subTest(delegation=case):
                b = self.bundle(changed_read=case == 'admission-recheck')
                with b.bound(), mock.patch.dict(sys.modules), mock.patch.object(BOOT.time, 'monotonic', side_effect=[91.0, 92.0]) as entry_clock, contextlib.redirect_stdout(io.StringIO()) as out:
                    sys.modules.pop('sg32_common', None)
                    sys.modules.pop('sg32_host', None)
                    if case == 'captured-bytes':
                        _raw, _cfg, common, host = BOOT.preflight(b.admission)
                        b.write(b.source / 'sg32_common.py', b"MARKER = 'changed-on-disk'\n")
                        owned = BOOT._load_validated(common, host)
                        try:
                            self.assertEqual(owned[-1][1].main([str(b.admission)], started_at=91.0), 73)
                            self.assertEqual(owned[0][1].__file__, str(b.source / 'sg32_common.py'))
                            self.assertEqual(owned[-1][1].__file__, str(b.source / 'sg32_host.py'))
                            self.assertIs(sys.modules['sg32_common'], owned[0][1])
                        finally:
                            for name, value in reversed(owned):
                                if sys.modules.get(name) is value:
                                    del sys.modules[name]
                    else:
                        self.assertTrue(sys.flags.isolated and sys.flags.no_site and sys.flags.dont_write_bytecode)
                        self.assertEqual(BOOT.main([str(b.admission)]), 2 if case == 'admission-recheck' else 73)
                        self.assertEqual(entry_clock.call_args_list, [mock.call(), mock.call()])
                        if case == 'admission-recheck':
                            self.assertEqual(json.loads(out.getvalue())['status'], 'SUBJECT_ERROR')
                    self.assertNotIn('sg32_common', sys.modules)
                    self.assertNotIn('sg32_host', sys.modules)

    def test_05_fresh_activation_clock_enforces_exact_reserve(self):
        for remaining in (124.999, 125.0, 125.001):
            with self.subTest(remaining=remaining):
                driver = mock.Mock(spec=['send', 'finish_input'])
                # A prior successful reserve check does not entitle activation:
                # create/inspect/start can consume time before this fresh read.
                work = 200.0
                activation = {'ready': True, 'work_millis': 80000}
                now = {'value': work - 126.0}
                original_encode = HOST.encode
                def consuming_encode(value, limit):
                    now['value'] = work - remaining
                    return original_encode(value, limit)
                with mock.patch.object(HOST, 'encode', side_effect=consuming_encode) as encoder, mock.patch.object(HOST.time, 'monotonic', side_effect=lambda: now['value']) as clock:
                    if remaining < 125:
                        with self.assertRaisesRegex(HOST.Fault, 'insufficient-work-and-close-budget'):
                            HOST.activate_driver(driver, activation, work)
                        driver.send.assert_not_called()
                        driver.finish_input.assert_not_called()
                    else:
                        HOST.activate_driver(driver, activation, work)
                        driver.send.assert_called_once()
                        self.assertEqual(HOST.strict_json(driver.send.call_args.args[0]), activation)
                        driver.finish_input.assert_called_once_with()
                    encoder.assert_called_once_with(activation, 4096)
                    clock.assert_called_once_with()

        # Entry-origin propagation does not grant a new 180s after preflight.
        with mock.patch.object(HOST.time, 'monotonic', side_effect=[179.0, 180.0]), mock.patch.object(HOST, 'read_admission', return_value=(b'approved-fixture', {})) as read, mock.patch.object(HOST, 'Engine', side_effect=AssertionError('engine created')) as engine:
            with self.assertRaisesRegex(HOST.Fault, 'work-prefix-exhausted'):
                HOST.run(Path('/not-read-by-this-controlled-admission'), started_at=0.0)
            read.assert_called_once()
            engine.assert_not_called()
        with mock.patch.object(HOST.time, 'monotonic', return_value=179.0), mock.patch.object(HOST, 'read_admission', side_effect=AssertionError('admission read')) as read:
            with self.assertRaisesRegex(HOST.Fault, 'invalid-entry-origin'):
                HOST.run(Path('/not-read'), started_at=True)
            read.assert_not_called()
        expired_engine = HOST.Engine.__new__(HOST.Engine)
        with mock.patch.object(HOST.time, 'monotonic', return_value=10.0), mock.patch.object(HOST, 'Child', side_effect=AssertionError('child created')) as child:
            with self.assertRaisesRegex(HOST.Fault, 'host-deadline-before-command'):
                expired_engine.command(['info'], 10.0)
            child.assert_not_called()
