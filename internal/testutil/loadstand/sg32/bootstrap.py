"""Closed SG32 entry: qualify bytes before loading either Subject module."""
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import time
import types

SOURCE_ROOT = Path('/Users/hades/Documents/repos/corsa/internal/testutil/loadstand/sg32')
INPUTS = {'corsad', 'c2driver', 'sg32_common.py', 'sg32_service.py', 'clockprobe.py'}


class Fault(Exception):
    pass


def need(value, reason):
    if not value:
        raise Fault(reason)


def decode(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            need(key not in result, 'duplicate-json-key')
            result[key] = value
        return result
    def invalid(_):
        raise Fault('nonfinite-json')
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid)


def checked(path, limit, capture=True):
    path = Path(path)
    need(path.is_absolute() and path.resolve(strict=True) == path, 'noncanonical-input-path')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        before = os.fstat(fd)
        need(stat.S_ISREG(before.st_mode) and before.st_uid == 501 and before.st_nlink == 1 and
             not before.st_mode & 0o022 and 0 < before.st_size <= limit, 'input-custody')
        stamp = lambda s: (s.st_dev,s.st_ino,s.st_mode,s.st_uid,s.st_gid,s.st_nlink,s.st_size,s.st_mtime_ns,s.st_ctime_ns)
        body, digest, size = bytearray(), hashlib.sha256(), 0
        while True:
            part = os.read(fd, 65536)
            if not part:
                break
            size += len(part); need(size <= limit, 'input-limit'); digest.update(part)
            if capture:
                body.extend(part)
        need(size == before.st_size and stamp(before) == stamp(os.fstat(fd)) == stamp(path.lstat()), 'input-changed')
        return bytes(body), digest.hexdigest(), size
    finally:
        os.close(fd)


def preflight(path):
    raw, _, _ = checked(path,65536)
    cfg = decode(raw)
    need(type(cfg) is dict and cfg.get('profile') == 'SG32-v1' and cfg.get('owner_approved') is True and
         cfg.get('allow_recovery_hold') is True and cfg.get('one_attempt') is True and cfg.get('inputs_ready') is True,
         'INERT-owner-or-inputs')
    need(type(cfg.get('files')) is dict and set(cfg['files']) == INPUTS, 'qualified-files-missing')
    qualification_raw, qualification_sha, _ = checked(cfg['qualification_path'],1048576)
    need(qualification_sha == cfg['qualification_sha256'], 'qualification-sha')
    q = decode(qualification_raw)
    need(type(q) is dict and q.get('status') == 'QUALIFIED_CURRENT_CLI_AND_REBOUND_DRIVER' and
         q.get('source_commit') == '8afa7f5b365a56d55f116c8a0dfb0ac581c79000' and
         q.get('platform') == 'linux-arm64-v8.0-cgo0' and type(q.get('build_exit')) is int and q['build_exit'] == 0 and
         all(q.get(k) is True for k in ('driver_source_reviewed','tester_accepted','reviewer_accepted')), 'inputs-unqualified')
    for name, entry in cfg['files'].items():
        need(type(entry) is dict and set(entry) == {'path','sha256','bytes'} and type(entry['sha256']) is str and
             re.fullmatch(r'[0-9a-f]{64}',entry['sha256']) and type(entry['bytes']) is int and
             0 < entry['bytes'] <= 128*1024**2, 'input-entry')
        _, digest, size = checked(entry['path'],128*1024**2,False)
        need(digest == entry['sha256'] and size == entry['bytes'], 'qualified-input-bytes')
        if name in ('corsad','c2driver'):
            need(q.get(name+'_sha256') == digest, 'qualified-artifact-sha')
    need(q.get('driver_compiled_cli_sha256') == cfg['files']['corsad']['sha256'], 'closed-driver-pin')
    need(SOURCE_ROOT.resolve(strict=True) == SOURCE_ROOT and
         Path(cfg['files']['sg32_common.py']['path']) == SOURCE_ROOT/'sg32_common.py', 'fixed-common-path')
    common, common_sha, _ = checked(SOURCE_ROOT/'sg32_common.py',131072)
    host, host_sha, _ = checked(SOURCE_ROOT/'sg32_host.py',131072)
    _, bootstrap_sha, _ = checked(SOURCE_ROOT/'bootstrap.py',65536,False)
    need(Path(__file__).resolve(strict=True) == SOURCE_ROOT/'bootstrap.py' and
         common_sha == cfg['files']['sg32_common.py']['sha256'] and host_sha == cfg['host_sha256'] and
         bootstrap_sha == cfg['bootstrap_sha256'], 'fixed-loaded-source-sha')
    need(checked(path,65536)[0] == raw, 'admission-changed-before-load')
    return raw, cfg, common, host


def _load_validated(common_raw, host_raw):
    need('sg32_common' not in sys.modules and 'sg32_host' not in sys.modules, 'subject-already-loaded')
    owned = []
    try:
        for name, body in (('sg32_common',common_raw),('sg32_host',host_raw)):
            module = types.ModuleType(name)
            module.__file__, module.__package__ = str(SOURCE_ROOT/(name+'.py')), ''
            sys.modules[name] = module; owned.append((name,module))
            exec(compile(body,module.__file__,'exec'),module.__dict__)
            need(sys.modules.get(name) is module, 'subject-module-replaced')
        need(all(sys.modules.get(name) is module for name,module in owned), 'subject-module-binding')
        return owned
    except BaseException:
        for name, module in reversed(owned):
            if sys.modules.get(name) is module:
                del sys.modules[name]
        raise


def main(argv=None):
    began = time.monotonic()
    args = sys.argv[1:] if argv is None else argv
    try:
        need(len(args) == 1, 'one-admission-path')
        raw, _cfg, common, host = preflight(Path(args[0]))
        need(sys.flags.isolated and sys.flags.no_site and sys.flags.dont_write_bytecode, 'isolated-I-S-B-required')
        need(time.monotonic() < began + 180, 'work-prefix-exhausted')
        owned = _load_validated(common,host)
    except (Fault,OSError,ValueError,TypeError,KeyError,SyntaxError) as error:
        print(json.dumps({'status':'NOT_STARTED_BOOTSTRAP_REFUSED','reason':str(error) if isinstance(error,Fault) else type(error).__name__}))
        return 2
    try:
        subject = owned[-1][1]
        need(subject.read_admission(Path(args[0]))[0] == raw, 'admission-changed-after-load')
        return subject.main([str(Path(args[0]))], started_at=began)
    except Exception as error:
        print(json.dumps({'status':'SUBJECT_ERROR','error_type':type(error).__name__}))
        return 2
    finally:
        for name, module in reversed(owned):
            if sys.modules.get(name) is module:
                del sys.modules[name]


if __name__ == '__main__':
    raise SystemExit(main())
