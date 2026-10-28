"""Small diagnostic-field controls; ordinary own files, no Linux DAC model."""
import copy
import errno
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest import mock

HERE = Path(__file__).resolve().parent
NONCE = 'a' * 32
SECRET = 'secret-error-text-must-never-be-logged'
FILENAME = '/private-secret-target-do-not-log'


def load_subject(name):
    path = HERE / (name + '.py')
    if name == 'clockprobe' and not path.exists():
        path = HERE.parent / 'clockprobe' / 'probe.py'
    module = types.ModuleType('sg32_diagnostic_subject_' + name)
    module.__file__ = str(path)
    exec(compile(path.read_bytes(), str(path), 'exec'), module.__dict__)
    return module


class PreparerDiagnosticPureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        common, clock = load_subject('sg32_common'), load_subject('clockprobe')
        with mock.patch.dict(sys.modules, {'sg32_common': common, 'clockprobe': clock}):
            cls.service = load_subject('sg32_service')

    def metadata_context(self):
        # These are actual ordinary host-user metadata, not guest root/capabilities.
        directory = tempfile.TemporaryDirectory(prefix='sg32-diagnostic-pure-')
        self.addCleanup(directory.cleanup)
        path = Path(directory.name).resolve(strict=True) / 'own-metadata'
        path.write_bytes(b'ordinary own bytes\n')
        path.chmod(0o600)
        info = path.lstat()
        actual = {'uid': info.st_uid, 'gid': info.st_gid, 'mode': info.st_mode, 'nlink': info.st_nlink}
        fake_os = types.SimpleNamespace(**vars(self.service.os))
        fake_os.lstat = mock.Mock(return_value=info)
        return fake_os, actual

    def test_01_success_passthrough_and_closed_labels_before_metadata(self):
        s = self.service
        fake_os, _metadata = self.metadata_context()
        value, argument = object(), object()
        function = mock.Mock(return_value=value)
        with mock.patch.object(s, 'os', fake_os):
            self.assertIs(s.preparer_call('manifest-read', '/inputs/manifest.json', function, argument), value)
        function.assert_called_once_with(argument)
        fake_os.lstat.assert_called_once_with('/inputs/manifest.json')
        for case in ('operation', 'path'):
            with self.subTest(label=case):
                fake_os, _metadata = self.metadata_context()
                fake_os.getuid = mock.Mock(side_effect=AssertionError('metadata before label validation'))
                function = mock.Mock(side_effect=AssertionError('operation before label validation'))
                operation = SECRET if case == 'operation' else 'open'
                path = FILENAME if case == 'path' else '/inputs/manifest.json'
                with mock.patch.object(s, 'os', fake_os), self.assertRaisesRegex(s.Fault, '^preparer-diagnostic-label$'):
                    s.preparer_call(operation, path, function)
                function.assert_not_called()
                fake_os.getuid.assert_not_called()
                fake_os.lstat.assert_not_called()

    def test_02_primary_permission_error_wins_over_metadata_failure(self):
        s = self.service
        for case in ('available', 'metadata-fails', 'actor-fails'):
            with self.subTest(metadata=case):
                fake_os, metadata = self.metadata_context()
                if case == 'metadata-fails':
                    fake_os.lstat.side_effect = OSError(errno.EIO, SECRET, FILENAME)
                elif case == 'actor-fails':
                    fake_os.getuid = mock.Mock(side_effect=OSError(errno.EIO, SECRET, FILENAME))
                primary = PermissionError(errno.EACCES, SECRET, FILENAME)
                function = mock.Mock(side_effect=primary)
                with mock.patch.object(s, 'os', fake_os), self.assertRaises(PermissionError) as caught:
                    s.preparer_call('open', '/inputs/manifest.json', function, b'secret-operation-argument')
                self.assertIs(caught.exception, primary)
                function.assert_called_once_with(b'secret-operation-argument')
                facts = primary.preparer_diagnostic
                self.assertEqual(set(facts), {'scope', 'operation', 'path_label', 'errno', 'actor', 'metadata', 'metadata_errno', 'metadata_unavailable'})
                self.assertEqual((facts['scope'], facts['operation'], facts['path_label'], facts['errno']),
                                 ('PREPARER_OPERATION', 'open', 'manifest', errno.EACCES))
                if case == 'available':
                    self.assertEqual(facts['metadata'], metadata)
                    self.assertFalse(facts['metadata_unavailable'])
                    self.assertIsNone(facts['metadata_errno'])
                else:
                    self.assertIsNone(facts['metadata'])
                    self.assertTrue(facts['metadata_unavailable'])
                    self.assertEqual(facts['metadata_errno'], errno.EIO)
                if case == 'actor-fails':
                    self.assertIsNone(facts['actor'])
                    fake_os.lstat.assert_not_called()
                else:
                    self.assertEqual(facts['actor'], {'uid': os.getuid(), 'gid': os.getgid(),
                                                     'euid': os.geteuid(), 'egid': os.getegid()})
                raw = s.encode(facts, 1024)
                self.assertLessEqual(len(raw), 1024)
                for hidden in (SECRET, FILENAME, 'secret-operation-argument', '/inputs/manifest.json'):
                    self.assertNotIn(hidden.encode(), raw)

    def test_03_main_routes_only_bounded_facts_and_strict_numeric_errno(self):
        s = self.service
        for case in ('valid', 'bool', 'unknown'):
            with self.subTest(errno=case):
                fake_os, _metadata = self.metadata_context()
                primary = PermissionError(errno.EACCES, SECRET, FILENAME)
                primary.errno = errno.EACCES if case == 'valid' else True if case == 'bool' else None
                operation = mock.Mock(side_effect=primary)
                with mock.patch.object(s, 'os', fake_os), self.assertRaises(PermissionError) as caught:
                    s.preparer_call('write', '/run/c1/auth.json', operation, b'secret-credential-body')
                self.assertIs(caught.exception, primary)
                self.assertEqual(primary.preparer_diagnostic['path_label'], 'auth')
                expected = errno.EACCES if case == 'valid' else None
                self.assertEqual(primary.preparer_diagnostic['errno'], expected)
                self.assertIs(type(primary.preparer_diagnostic['errno']), int if case == 'valid' else type(None))
                records = []
                with mock.patch.object(s, 'dispatch', side_effect=primary), mock.patch.object(s, 'emit', side_effect=lambda row: records.append(copy.deepcopy(row))):
                    self.assertEqual(s.main(['preparer', NONCE]), 2)
                self.assertEqual(len(records), 1)
                row = records[0]
                self.assertEqual((row['kind'], row['nonce'], row['status'], row['reason']),
                                 ('ERROR', NONCE, 'UNKNOWN', 'PermissionError'))
                self.assertEqual(row['facts'], primary.preparer_diagnostic)
                self.assertFalse(row['cleanup_failed'])
                self.assertIsNone(row['observed_node_exit'])
                self.assertEqual(row['child_exits'], [])
                raw = s.encode(row, 2048)
                self.assertLessEqual(len(raw), 2048)
                for hidden in (SECRET, FILENAME, 'secret-credential-body', '/run/c1/auth.json'):
                    self.assertNotIn(hidden.encode(), raw)
