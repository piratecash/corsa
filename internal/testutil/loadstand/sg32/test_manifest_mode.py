"""Owned host-FD mode controls; no Docker or Linux DAC emulation."""
import errno
import os
from pathlib import Path
import stat
import sys
import tempfile
import types
import unittest
from unittest import mock

HERE = Path(__file__).resolve().parent


def load_subject(name):
    path = HERE / (name + '.py')
    module = types.ModuleType('sg32_manifest_mode_subject_' + name)
    module.__file__ = str(path)
    exec(compile(path.read_bytes(), str(path), 'exec'), module.__dict__)
    return module


class ManifestModePureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        common = load_subject('sg32_common')
        with mock.patch.dict(sys.modules, {'sg32_common': common}):
            cls.host = load_subject('sg32_host')

    def engine(self):
        directory = tempfile.TemporaryDirectory(prefix='sg32-manifest-mode-pure-')
        self.addCleanup(directory.cleanup)
        root = Path(directory.name).resolve(strict=True)
        root.chmod(0o700)
        engine = self.host.Engine()
        self.addCleanup(engine.selector.close)
        engine.root = root
        return engine

    def test_01_nonsecret_manifest_exact_bytes_and_owned_fd_mode_under_umask(self):
        h, engine = self.host, self.engine()
        payload = {'nonce': 'a' * 32, 'files': {'corsad': {'sha256': 'b' * 64, 'bytes': 42}}}
        expected = (b'{"nonce":"' + b'a' * 32 + b'","files":{"corsad":{"sha256":"' +
                    b'b' * 64 + b'","bytes":42}}}\n')
        real_fchmod = os.fchmod
        changed = []

        def owned_fchmod(fd, mode):
            before = os.fstat(fd)
            self.assertTrue(stat.S_ISREG(before.st_mode))
            self.assertEqual((before.st_uid, before.st_nlink, before.st_size), (501, 1, len(expected)))
            self.assertEqual(stat.S_IMODE(before.st_mode), 0o600)
            changed.append((fd, before.st_dev, before.st_ino, mode))
            return real_fchmod(fd, mode)

        fake_os = types.SimpleNamespace(**vars(os))
        fake_os.fchmod = owned_fchmod
        previous = os.umask(0o077)
        try:
            with mock.patch.object(h, 'os', fake_os):
                engine.put('inputs-manifest.json', payload)
        finally:
            os.umask(previous)
        path = engine.root / 'inputs-manifest.json'
        after = path.lstat()
        self.assertEqual(path.read_bytes(), expected)
        self.assertEqual(len(changed), 1)
        fd, dev, ino, mode = changed[0]
        self.assertEqual((after.st_dev, after.st_ino, mode), (dev, ino, 0o644))
        self.assertEqual((after.st_uid, after.st_nlink, stat.S_IMODE(after.st_mode)), (501, 1, 0o644))
        self.assertEqual(stat.S_IMODE(engine.root.stat().st_mode), 0o700)
        self.assertEqual(engine.used['control'], len(expected))
        with self.assertRaises(OSError) as caught:
            os.fstat(fd)
        self.assertEqual(caught.exception.errno, errno.EBADF)

    def test_02_other_receipts_stay_private_and_exclusive_paths_never_overwrite(self):
        h = self.host
        for name in ('result.json', 'auth.json', 'inputs-manifest.json.tmp'):
            with self.subTest(private_name=name):
                engine = self.engine()
                fake_os = types.SimpleNamespace(**vars(os))
                fake_os.fchmod = mock.Mock(side_effect=AssertionError('private receipt chmod'))
                previous = os.umask(0)
                try:
                    with mock.patch.object(h, 'os', fake_os):
                        engine.put(name, {'private': 'keep-private'})
                finally:
                    os.umask(previous)
                path = engine.root / name
                self.assertEqual(path.read_bytes(), b'{"private":"keep-private"}\n')
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
                self.assertEqual(stat.S_IMODE(engine.root.stat().st_mode), 0o700)
                fake_os.fchmod.assert_not_called()
        for kind in ('existing-file', 'symlink'):
            with self.subTest(exclusive_path=kind):
                engine = self.engine()
                protected = engine.root / 'protected'
                protected.write_bytes(b'original-private-bytes')
                protected.chmod(0o600)
                target = engine.root / 'inputs-manifest.json'
                if kind == 'symlink':
                    target.symlink_to(protected)
                else:
                    target.write_bytes(b'existing-manifest-bytes')
                    target.chmod(0o600)
                with self.assertRaises(FileExistsError):
                    engine.put('inputs-manifest.json', {'replacement': True})
                self.assertEqual(protected.read_bytes(), b'original-private-bytes')
                self.assertEqual(stat.S_IMODE(protected.stat().st_mode), 0o600)
                if kind == 'symlink':
                    self.assertTrue(target.is_symlink())
                else:
                    self.assertEqual(target.read_bytes(), b'existing-manifest-bytes')

    def test_03_mode_or_identity_failure_refuses_and_closes_original_fd(self):
        h = self.host
        for kind in ('fchmod-error', 'mode-readback', 'identity-readback'):
            with self.subTest(failure=kind):
                engine = self.engine()
                fake_os = types.SimpleNamespace(**vars(os))
                real_fstat, real_fchmod, real_close = os.fstat, os.fchmod, os.close
                changed, closed = [], []
                primary = OSError(errno.EIO, 'injected-owned-fd-mode-failure')

                def chmod(fd, mode):
                    changed.append(fd)
                    if kind == 'fchmod-error':
                        raise primary
                    return real_fchmod(fd, mode)

                def fstat(fd):
                    value = real_fstat(fd)
                    if not changed:
                        return value
                    fields = {key: getattr(value, key) for key in dir(value) if key.startswith('st_')}
                    if kind == 'mode-readback':
                        fields['st_mode'] = stat.S_IFREG | 0o600
                    elif kind == 'identity-readback':
                        fields['st_ino'] += 1
                    return types.SimpleNamespace(**fields)

                def close(fd):
                    closed.append(fd)
                    return real_close(fd)

                fake_os.fchmod, fake_os.fstat, fake_os.close = chmod, fstat, close
                fake_os.fsync = mock.Mock(side_effect=AssertionError('fsync after refused mode'))
                with mock.patch.object(h, 'os', fake_os):
                    if kind == 'fchmod-error':
                        with self.assertRaises(OSError) as caught:
                            engine.put('inputs-manifest.json', {'nonce': 'c' * 32})
                        self.assertIs(caught.exception, primary)
                    else:
                        with self.assertRaisesRegex(h.Fault, '^upload-manifest-mode$'):
                            engine.put('inputs-manifest.json', {'nonce': 'c' * 32})
                self.assertEqual(len(changed), 1)
                self.assertEqual(closed, changed)
                fake_os.fsync.assert_not_called()
                with self.assertRaises(OSError) as caught:
                    real_fstat(changed[0])
                self.assertEqual(caught.exception.errno, errno.EBADF)
                remaining = engine.root / 'inputs-manifest.json'
                self.assertEqual(remaining.read_bytes(), b'{"nonce":"' + b'c' * 32 + b'"}\n')
                with self.assertRaises(FileExistsError):
                    engine.put('inputs-manifest.json', {'retry': True})


if __name__ == '__main__':
    unittest.main()
