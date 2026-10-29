"""Finite preparer-controller controls; no Docker, guest DAC or target execution."""
import contextlib
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
import zlib
from unittest import mock

HERE = Path(__file__).resolve().parent
N, ID = 'a' * 32, '1' * 64
sha = lambda raw: hashlib.sha256(raw).hexdigest()


def load(name):
    path = HERE / (name + '.py')
    module = types.ModuleType('preparer_test_' + name)
    module.__file__ = str(path)
    exec(compile(path.read_bytes(), str(path), 'exec'), module.__dict__)
    return module


C = load('sg32_common')
with mock.patch.dict(sys.modules, {'sg32_common': C}):
    H = load('sg32_host')
    with mock.patch.dict(sys.modules, {'sg32_host': H}):
        P = load('preparer_only')
B, FULL, FIX = load('preparer_bootstrap'), load('bootstrap'), load('test_sg32')


class OwnFiles:
    def __init__(self):
        self.temp = tempfile.TemporaryDirectory(prefix='preparer-only-pure-')
        self.root = Path(self.temp.name).resolve(strict=True)
        self.runs = self.root / 'runs'; self.runs.mkdir(mode=0o700)
        self.path = self.root / 'admission.json'
        self.raw = b'controlled prequalified run fixture\n'
        self.write(self.path, self.raw)
        self.cfg = {'run_parent': str(self.runs), 'files': {}}
        for name in C.INPUT_NAMES:
            p = self.root / name; raw = ('opaque unit input ' + name).encode()
            self.write(p, raw)
            self.cfg['files'][name] = {'path': str(p), 'sha256': sha(raw), 'bytes': len(raw)}

    @staticmethod
    def write(path, raw):
        path.write_bytes(raw); path.chmod(0o600)

    def close(self): self.temp.cleanup()


class HoldObserved(BaseException): pass


class Transport:
    """Only the parent Docker CLI/completion boundary; real RestrictedEngine stays."""
    def __init__(self, files, outcome='prepared'):
        self.files, self.outcome = files, outcome
        self.volumes, self.present, self.started = set(), False, False
        self.calls, self.starts, self.engine, self.fail_copy = [], [], None, False
        self.bad_entry = False

    def metadata(self, engine):
        meta = FIX.container_meta('preparer', engine.names)
        meta['Id'] = ID
        meta['Config'].update(Entrypoint=['/usr/bin/python3'], Cmd=['-I', '-S', '-B', '-c', H.BOOTSTRAP, 'preparer', N])
        meta['HostConfig']['CapAdd'] = ['CAP_CHOWN']
        if self.bad_entry: meta['Config']['Cmd'][-2] = 'keeper'
        code = 0 if self.outcome == 'prepared' else 2
        running = self.started and self.outcome == 'timeout'
        meta['State'].update(ExitCode=0 if running else code, OOMKilled=False, Running=running)
        return meta

    def command(self, engine, args, deadline, mutation=False):
        self.engine = engine; self.calls.append(list(args)); out = b''
        if args[:2] == ['context', 'inspect']: out = json.dumps(H.ENDPOINT).encode()
        elif args[:1] == ['info']: out = b'"29.8.0"'
        elif args[:2] == ['image', 'inspect']: out = json.dumps({'Id': H.IMAGE, 'Os': 'linux', 'Architecture': 'arm64', 'Config': {'Volumes': None}}).encode()
        elif args[:2] == ['volume', 'create']: self.volumes.add(args[-1]); out = (args[-1] + '\n').encode()
        elif args[:2] == ['volume', 'inspect']:
            name = args[-1]; role = name.rsplit('-', 1)[1]
            options = {'type': 'tmpfs', 'device': 'tmpfs', 'o': 'size=1048576,mode=0700'} if role == 'secret' else None
            out = json.dumps({'Name': name, 'Driver': 'local', 'Options': options, 'Labels': {'corsa.sg32.owner': N, 'corsa.sg32.role': role}}).encode()
        elif args[:2] == ['volume', 'ls']:
            query = args[args.index('--filter') + 1]
            names = self.volumes if query.startswith('label=') else {query[5:]} & self.volumes
            out = ''.join(n + '\n' for n in sorted(names)).encode()
        elif args[:2] == ['volume', 'rm']: self.volumes.remove(args[-1]); out = (args[-1] + '\n').encode()
        elif args[:1] == ['create']:
            assert args == H.create_argv('preparer', N, engine.names)
            self.present = True; out = (ID + '\n').encode()
        elif args[:1] == ['inspect']: out = json.dumps(self.metadata(engine)).encode()
        elif args[:1] == ['ps']: out = (ID + '\n').encode() if self.present else b''
        elif args[:2] == ['rm', '-f']: assert args[-1] == ID; self.present = False; out = (ID + '\n').encode()
        elif args[:1] == ['cp']:
            if self.fail_copy: raise H.Fault('controlled failed copy')
        else: raise AssertionError('unexpected parent boundary argv')
        engine.commands.append({'argv': list(args), 'exit': 0, 'stdout': out.decode(), 'stderr': ''})
        return out

    def child(self, engine, args, role, interactive=False):
        assert args == ['start', '-a', '-i', ID] and role == 'control' and interactive is True
        self.starts.append(list(args)); self.started = True
        t = self
        class Child:
            def __init__(self):
                self.number, self.err, self.manifest = engine.attempt, bytearray(), None
                engine.attempt += 1
                code = None if t.outcome == 'timeout' else 0 if t.outcome == 'prepared' else 2
                self.p = types.SimpleNamespace(returncode=code, pid=12345)
                self.p.poll = lambda: self.p.returncode
                engine.live.append(self)
            def send(self, raw): self.manifest = C.strict_json(raw)
            def finish_input(self): pass
            def done(self): return self.p.returncode is not None
            def records(self, final=False):
                if t.outcome == 'malformed': raise ValueError('controlled malformed output')
                if t.outcome == 'prepared': return [{'kind': 'PREPARED', 'nonce': N, 'files': self.manifest['files']}]
                return [{'kind': 'ERROR', 'nonce': N, 'status': 'UNKNOWN', 'reason': 'PermissionError', 'facts': None}]
            def close(self, deadline):
                if t.outcome == 'unknown': return False
                if self.p.returncode is None: self.p.returncode = -15
                return True
        return Child()

    def pump(self, engine, predicate, deadline):
        if self.outcome == 'timeout': raise H.Fault('controlled preparer timeout')
        assert predicate()

    @contextlib.contextmanager
    def bound(self):
        with mock.patch.object(H.Engine, 'command', lambda e,a,d,mutation=False: self.command(e,a,d,mutation)), \
             mock.patch.object(H.Engine, 'pump_until', lambda e,p,d: self.pump(e,p,d)), \
             mock.patch.object(H, 'Child', side_effect=self.child):
            yield self

    def assert_no_runtime(self, test):
        for args in self.calls:
            test.assertNotIn(args[0], ('run', 'exec', 'start'))
            if args[0] == 'create': test.assertEqual(args, H.create_argv('preparer', N, H.volume_names(N)))
        test.assertLessEqual(len(self.starts), 1)
        for args in self.starts: test.assertEqual(args, ['start', '-a', '-i', ID])


def historical_service_fixture():
    """Closed historical Source DATA; never import or execute the decompressed bytes."""
    raw = (HERE / 'fixtures/preparer_service_4575.py.gz').read_bytes()
    assert len(raw) == 12134 and sha(raw) == 'f0521c0e10cd82dfbd240dfba0964ebae3520b36fb99d5739f8eefe8ec2f8465'
    decoder = zlib.decompressobj(31)
    restored = decoder.decompress(raw, 47526)
    assert decoder.eof and not decoder.unused_data and not decoder.unconsumed_tail
    assert len(restored) == 47525 and sha(restored) == P.DIAGNOSTIC_SHA
    return restored


class BootstrapFiles(OwnFiles):
    def __init__(self):
        super().__init__(); self.src = self.root / 'source'; self.src.mkdir(mode=0o700)
        common = b"MARKER='common'\n"; host = b"MARKER='host'\n"
        controller = b"import sg32_common,sg32_host\nfrom pathlib import Path\ndef admission(path): return Path(path).read_bytes(),{}\ndef main(args,started_at=None): return 73 if started_at==91 and sg32_common.MARKER=='common' and sg32_host.MARKER=='host' else 99\n"
        for name, raw in [('sg32_common.py', common), ('sg32_host.py', host), ('preparer_only.py', controller), ('preparer_bootstrap.py', Path(B.__file__).read_bytes())]: self.write(self.src/name, raw)
        self.cfg = {'profile': P.PROFILE, 'purpose': P.PURPOSE, 'owner_approved': True, 'one_attempt': True,
                    'allow_recovery_hold': True, 'inputs_ready': True, 'runtime_service': False, 'runtime_driver': False, 'files': {}}
        for name in C.INPUT_NAMES:
            path = self.src/name
            if name == 'sg32_service.py': self.write(path, historical_service_fixture())
            elif not path.exists(): self.write(path, b'ordinary opaque fixture\n')
            raw = path.read_bytes(); self.cfg['files'][name] = {'path': str(path), 'sha256': sha(raw), 'bytes': len(raw)}
        q = {'status': 'QUALIFIED_CURRENT_CLI_AND_REBOUND_DRIVER', 'source_commit': '8afa7f5b365a56d55f116c8a0dfb0ac581c79000',
             'platform': 'linux-arm64-v8.0-cgo0', 'build_exit': 0, 'driver_source_reviewed': True, 'tester_accepted': True,
             'reviewer_accepted': True, 'corsad_sha256': self.cfg['files']['corsad']['sha256'],
             'c2driver_sha256': self.cfg['files']['c2driver']['sha256'], 'driver_compiled_cli_sha256': self.cfg['files']['corsad']['sha256']}
        qp = self.root/'qualification.json'; self.write(qp, json.dumps(q).encode())
        self.cfg.update(qualification_path=str(qp), qualification_sha256=sha(qp.read_bytes()), host_sha256=sha(host),
                        preparer_only_sha256=sha(controller), bootstrap_sha256=sha((self.src/'preparer_bootstrap.py').read_bytes()))
        self.decision = {'profile': P.PROFILE, 'purpose': P.PURPOSE, 'owner_approved': True, 'one_attempt': True,
                         'runtime_service': False, 'runtime_driver': False}
        dp=self.root/'decision.json';self.write(dp,json.dumps(self.decision).encode());self.cfg.update(owner_decision_path=str(dp),owner_decision_sha256=sha(dp.read_bytes()))
        self.save()
    def save(self): self.write(self.path, json.dumps(self.cfg).encode())
    @contextlib.contextmanager
    def bound(self):
        with mock.patch.object(B, 'SOURCE_ROOT', self.src), mock.patch.object(B, '__file__', str(self.src/'preparer_bootstrap.py')): yield self


class PreparerOnlyPureTests(unittest.TestCase):
    def own(self, cls=OwnFiles):
        f=cls();self.addCleanup(f.close);return f

    def test_01_crossed_profiles_inert_and_decision_source_binding(self):
        for case in ('new-under-old','old-under-new','ownerfalse','decision-hash','numeric-decision','self-hash'):
            with self.subTest(admission=case):
                f=self.own()
                header={'profile': P.PROFILE if case!='old-under-new' else 'SG32-v1','owner_approved':case!='ownerfalse','allow_recovery_hold':True,'one_attempt':True}
                f.write(f.path,json.dumps(header).encode())
                with mock.patch.object(P,'RestrictedEngine',side_effect=AssertionError('engine before admission')), mock.patch.object(H,'Engine',side_effect=AssertionError('full engine before admission')):
                    if case=='new-under-old':
                        with self.assertRaisesRegex(C.Fault,'INERT-owner-admission'): H.main([str(f.path)])
                        with self.assertRaises(FULL.Fault): FULL.preflight(f.path)
                    elif case in ('old-under-new','ownerfalse'):
                        with self.assertRaisesRegex(C.Fault,'INERT-owner-admission'): P.main([str(f.path)])
                    else:
                        decision={'profile':P.PROFILE,'purpose':P.PURPOSE,'owner_approved':True,'one_attempt':True,'runtime_service':False,'runtime_driver':False}
                        if case=='numeric-decision': decision['owner_approved']=1
                        dp=f.root/'decision.json';f.write(dp,json.dumps(decision).encode())
                        cfg={'purpose':P.PURPOSE,'runtime_service':False,'runtime_driver':False,'owner_decision_path':str(dp),'owner_decision_sha256':sha(dp.read_bytes()),'preparer_only_sha256':sha(Path(P.__file__).read_bytes()),'files':{'sg32_service.py':{'sha256':P.DIAGNOSTIC_SHA}}}
                        if case=='decision-hash': cfg['owner_decision_sha256']='0'*64
                        if case=='self-hash': cfg['preparer_only_sha256']='0'*64
                        with mock.patch.object(H,'read_admission',return_value=(f.raw,cfg)) as read:
                            with self.assertRaisesRegex(C.Fault,'preparer-source-binding' if case=='self-hash' else 'new-preparer-owner-decision'): P.admission(f.path)
                        read.assert_called_once_with(f.path,expected_profile=P.PROFILE)

    def test_02_bootstrap_qualified_marker_and_owner_bytes_modules_refuse(self):
        for case in ('ownerfalse','controller-sha','stale-module','exact-marker','current-source'):
            with self.subTest(bootstrap=case):
                f=self.own(BootstrapFiles)
                if case=='ownerfalse': f.cfg['owner_approved']=False
                if case=='controller-sha': f.cfg['preparer_only_sha256']='0'*64
                if case=='current-source':
                    raw=(HERE/'sg32_service.py').read_bytes()
                    self.assertNotEqual(sha(raw),P.DIAGNOSTIC_SHA)
                    path=f.src/'sg32_service.py';f.write(path,raw)
                    f.cfg['files']['sg32_service.py']={'path':str(path),'sha256':sha(raw),'bytes':len(raw)}
                f.save()
                with f.bound(), mock.patch.dict(sys.modules), contextlib.redirect_stdout(io.StringIO()):
                    for name in ('sg32_common','sg32_host','preparer_only'):sys.modules.pop(name,None)
                    if case=='stale-module':
                        foreign=types.ModuleType('preparer_only');sys.modules['preparer_only']=foreign
                        with self.assertRaisesRegex(B.Fault,'subject-already-loaded'): B._load_validated(b'MARKER=1',b'MARKER=2',b'MARKER=3')
                        self.assertIs(sys.modules['preparer_only'],foreign)
                    elif case=='exact-marker':
                        self.assertEqual(f.cfg['files']['sg32_service.py']['sha256'],P.DIAGNOSTIC_SHA)
                        with mock.patch.object(Path,'read_bytes',return_value=b'corrupted historical fixture'):
                            with self.assertRaises(AssertionError): historical_service_fixture()
                        with mock.patch.object(B.time,'monotonic',side_effect=[91.,92.]):self.assertEqual(B.main([str(f.path)]),73)
                        self.assertTrue(all(name not in sys.modules for name in ('sg32_common','sg32_host','preparer_only')))
                    else:
                        with mock.patch.object(B,'_load_validated',side_effect=AssertionError('loaded unapproved bytes')) as load_subject:
                            if case=='current-source':
                                with self.assertRaisesRegex(B.Fault,'^diagnostic-source$'): B.preflight(f.path)
                            self.assertEqual(B.main([str(f.path)]),2)
                        load_subject.assert_not_called()

    def test_03_independent_command_guard_never_delegates_runtime_or_foreign(self):
        for case in ('service','driver','run','exec','start','foreign-cp','foreign-rm'):
            with self.subTest(command=case):
                f=self.own();t=Transport(f);e=P.RestrictedEngine(N,f.cfg);self.addCleanup(e.selector.close)
                e.prep_id=ID;e.verified.add(ID);e.volumes=set(e.names.values())
                args={'run':['run','--rm',H.IMAGE], 'exec':['exec',ID,'/inputs/corsad'], 'start':['start','-a','-i',ID],
                      'foreign-cp':['cp',next(iter(f.cfg['files'].values()))['path'],'2'*64+':/inputs/corsad'], 'foreign-rm':['rm','-f','2'*64]}.get(case)
                if case in ('service','driver'):args=H.create_argv(case,N,e.names,ID,'b'*64)
                with t.bound(), self.assertRaises(C.Fault):e.command(args,120.,mutation=True)
                self.assertEqual(t.calls,[]);self.assertEqual(t.starts,[])

    def test_04_copy_failure_entry_binding_and_one_start(self):
        for case in ('failed-copy','bad-entry','once'):
            with self.subTest(start=case):
                f=self.own();t=Transport(f);e=P.RestrictedEngine(N,f.cfg);self.addCleanup(e.selector.close)
                e.root=f.root;e.prep_id=ID;e.verified.add(ID);e.copied=set(f.cfg['files']);e.volumes=set(e.names.values());t.present=True
                with t.bound(), mock.patch.object(P.time,'monotonic',return_value=100.):
                    if case=='failed-copy':
                        t.fail_copy=True;args=['cp',str(e.root/'inputs-manifest.json'),ID+':/inputs/manifest.json']
                        with self.assertRaises(C.Fault):e.command(args,120.,mutation=True)
                        self.assertNotIn('manifest.json',e.copied)
                        with self.assertRaises(C.Fault):e.start_preparer(120.)
                        with self.assertRaises(C.Fault):e.command(args,120.,mutation=True)
                        self.assertEqual(t.starts,[]);self.assertEqual(len(t.calls),1)
                    else:
                        e.copied.add('manifest.json');t.bad_entry=case=='bad-entry'
                        if case=='bad-entry':
                            with self.assertRaisesRegex(C.Fault,'preparer-entry-command'):e.start_preparer(120.)
                            self.assertEqual(t.starts,[])
                        else:
                            child=e.start_preparer(120.)
                            with self.assertRaisesRegex(C.Fault,'closed-preparer-start'):e.start_preparer(120.)
                            self.assertEqual(len(t.starts),1);self.assertIs(e.prep_child,child)

    def run_flow(self,outcome,fail_receipt=False):
        f=self.own();t=Transport(f,outcome);original_read=P.read_owned
        def read(path,limit):
            if Path(path)==H.LOCK/'owner':return b'name=d1-sg32\npgid=77\n'
            return original_read(path,limit)
        fake_os=types.SimpleNamespace(**vars(P.os));fake_os.getuid=lambda:501;fake_os.getgid=lambda:20;fake_os.getpgrp=lambda:77
        fake_os.statvfs=lambda _:types.SimpleNamespace(f_bavail=32*1024**3,f_frsize=1)
        def hold(_):raise HoldObserved()
        clock=types.SimpleNamespace(monotonic=lambda:100.,sleep=hold)
        original_put=H.Engine.put
        def put(engine,name,value):
            if fail_receipt and name=='result.json':raise OSError('controlled result receipt failure')
            return original_put(engine,name,value)
        with t.bound(), mock.patch.object(P,'admission',return_value=(f.raw,f.cfg)), mock.patch.object(P,'read_owned',side_effect=read), mock.patch.object(P,'os',fake_os), mock.patch.object(P,'time',clock), mock.patch.object(P.secrets,'token_hex',return_value=N), mock.patch.object(H.Engine,'put',new=put), mock.patch.object(H,'run',side_effect=AssertionError('full SG32 continuation')), contextlib.redirect_stdout(io.StringIO()):
            if outcome=='unknown':
                with self.assertRaises(HoldObserved):P.run(f.path)
                code=None
            else:code=P.run(f.path)
        self.addCleanup(t.engine.selector.close)
        resultpath=f.runs/('preparer-'+N)/'result.json'
        result=json.loads(resultpath.read_bytes()) if resultpath.exists() else None
        self.assertTrue((f.root/'PREPARER-ONE-ATTEMPT-CLAIM.json').exists())
        t.assert_no_runtime(self);self.assertFalse(t.present);self.assertEqual(t.volumes,set());self.assertEqual(len(t.starts),1)
        return code,result,t

    def test_05_finite_success_error_malformed_timeout_all_cleanup_without_continuation(self):
        for outcome in ('prepared','error','malformed','timeout'):
            with self.subTest(outcome=outcome):
                code,result,t=self.run_flow(outcome)
                self.assertEqual(code,0 if outcome=='prepared' else 2)
                self.assertEqual(result['status'],{'prepared':'PREPARATION_COMPLETE','error':'PREPARER_ERROR_OBSERVED'}.get(outcome,'INCOMPLETE'))
                self.assertEqual(result['physical_closure'],'KNOWN');self.assertFalse(result['measurement_qualified'])
                self.assertEqual(result['historical_cause'],'UNKNOWN');self.assertIsNotNone(result['preparer'])
                self.assertEqual(result['preparer']['cli_exit'],None if outcome=='timeout' else 0 if outcome=='prepared' else 2)
                self.assertIsNotNone(result['preparer']['state'])
                self.assertTrue(any(args==['rm','-f',ID] for args in t.calls))
                self.assertEqual(len([args for args in t.calls if args[:2]==['volume','rm']]),3)

    def test_06_unknown_closure_holds_even_if_result_receipt_fails(self):
        code,result,t=self.run_flow('unknown',fail_receipt=True)
        self.assertIsNone(code);self.assertIsNone(result)
        self.assertFalse(t.engine.close_children(310.))
