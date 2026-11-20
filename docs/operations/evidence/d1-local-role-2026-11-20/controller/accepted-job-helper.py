"""Single offline Linux arm64 driver compilation; never executes the artifact."""
from pathlib import Path
import hashlib
import importlib.util
import json
import os
import shutil
import signal
import stat
import subprocess
import time
import uuid

HERE=Path(__file__).resolve().parent
REPO=HERE.parents[4]
OLD=Path('/private/tmp/corsa-d1-churn512-build-6d73ac4a404e42a78edee0c2c6b9f834')
OLD_MANIFEST_SHA='f929095e0f6f3694823ddf1d539b3854db71a9f66f17e22b73142c1315896699'
DRIVER='internal/core/node/find_node_churn512_external_linux_test.go'
BEFORE='279d05dbc8b6bdfed02d69c04edc1ea1ca5eea08eed578a3d599d4a37d026ba1'
GO='/usr/local/go/bin/go'
LOCK=Path('/private/tmp/corsa-heavy-run.lock')


def sha(path):
    h=hashlib.sha256()
    with path.open('rb') as stream:
        for part in iter(lambda:stream.read(1024*1024),b''):h.update(part)
    return h.hexdigest()


def stamp(path):
    s=path.lstat();return tuple(getattr(s,k) for k in ('st_dev','st_ino','st_uid','st_gid','st_mode','st_size','st_mtime_ns','st_ctime_ns'))


def inventory(path):return {str(p.relative_to(path)):sha(p) for p in sorted(path.rglob('*')) if p.is_file()}


def work_volume(root):
    """Phase accounting: only own ephemeral tmp/cache may vanish concurrently."""
    root=Path(root)
    def scan(directory,ephemeral=False):
        try:
            with os.scandir(directory) as iterator:
                entries=list(iterator)
        except FileNotFoundError:
            if ephemeral:return 0
            raise
        total=0
        for entry in entries:
            volatile=ephemeral or (directory==root and entry.name in ("tmp","cache"))
            try:info=entry.stat(follow_symlinks=False)
            except FileNotFoundError:
                if volatile:continue
                raise
            if stat.S_ISREG(info.st_mode):total+=info.st_size
            elif stat.S_ISDIR(info.st_mode):total+=scan(Path(entry.path),volatile)
            else:raise RuntimeError("unexpected own work entry type: "+entry.path)
        return total
    return scan(root)


def raw_bytes(root):
    return sum(p.stat().st_size for p in Path(root).glob('*.std*'))


def main():
    os.umask(0o077)
    if os.getpid()!=os.getpgrp():os.setsid()
    start=time.monotonic();deadline=start+600;cleanup_end=start+630
    manifest=json.loads((HERE/'MANIFEST.json').read_bytes())
    assert manifest['driver_after_sha256']==sha(HERE/'driver.go.txt') and manifest['build_controller_sha256']==sha(Path(__file__))
    attempt=uuid.uuid4().hex;work=Path('/private/tmp/corsa-d1-sg512-target-history-driver-'+attempt)
    assert shutil.disk_usage('/private/tmp').free>=10*1024**3
    work.mkdir(mode=0o700);LOCK.mkdir(mode=0o700)
    owner=f'name=d1-sg512-target-history-driver\npid={os.getpid()}\npgid={os.getpgrp()}\nattempt_id={attempt}\n'.encode();(LOCK/'owner').write_bytes(owner)
    custody=(stamp(LOCK),stamp(LOCK/'owner'));steps=[];child=None;known=True;error=None;artifact=None
    env={**os.environ,'GOROOT':'/usr/local/go','GOENV':'off','GOWORK':'off','GOVCS':'*:off','GODEBUG':'','GOTOOLCHAIN':'local','GOPROXY':'off','GOSUMDB':'off','GOTELEMETRY':'off','GOMAXPROCS':'2','GOMEMLIMIT':'768MiB','GOFLAGS':'-p=1','GOCACHE':str(work/'cache'),'GOMODCACHE':str(REPO/'.gomodcache'),'TMPDIR':str(work/'tmp'),'CGO_ENABLED':'0','GOOS':'linux','GOARCH':'arm64','GOARM64':'v8.0'}
    def volume():return work_volume(work)
    def group_absent():
        nonlocal known
        if not child:return True
        try:os.killpg(child.pid,0)
        except ProcessLookupError:return True
        known=False;return False
    def run(name,args,cwd):
        nonlocal child
        assert time.monotonic()<deadline and volume()<=6*1024**3
        policy='(version 1)(allow default)(deny network*)(deny file-write* (subpath '+json.dumps(str(work/'source'))+'))'
        row=dict(phase=name,argv=args,pid=None,exit=None,stdout=str(work/(name+'.stdout')),stderr=str(work/(name+'.stderr')))
        steps.append(row)
        with (work/(name+'.stdout')).open('xb') as out,(work/(name+'.stderr')).open('xb') as err:
            child=subprocess.Popen(['/usr/bin/sandbox-exec','-p',policy,*args],cwd=cwd,env=env,stdout=out,stderr=err,start_new_session=True)
            row['pid']=child.pid
            while child.poll() is None:
                assert time.monotonic()<deadline and raw_bytes(work)<=1024**2 and volume()<=6*1024**3,'deadline/RAW/file bound'
                time.sleep(.1)
        assert raw_bytes(work)<=1024**2,'aggregate RAW bound after command'
        row['exit']=child.returncode
        assert group_absent(),'owned command group remains'
        assert child.returncode==0,name+' failed'
    try:
        old_raw=(OLD/'SOURCE-MANIFEST.json').read_bytes();assert hashlib.sha256(old_raw).hexdigest()==OLD_MANIFEST_SHA
        frozen=json.loads(old_raw);assert frozen[DRIVER]==BEFORE and inventory(OLD/'source')==frozen
        src=work/'source';src.mkdir();(work/'tmp').mkdir();(work/'artifacts').mkdir()
        for relative,digest in frozen.items():
            parts=Path(relative).parts;assert parts and not Path(relative).is_absolute() and '..' not in parts
            original=OLD/'source'/relative;info=original.lstat();assert stat.S_ISREG(info.st_mode) and not original.is_symlink()
            copied=src/relative;copied.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(original,copied)
            assert sha(copied)==digest and time.monotonic()<deadline
        shutil.copyfile(HERE/'driver.go.txt',src/DRIVER)
        initial=inventory(src);expected=dict(frozen);expected[DRIVER]=manifest['driver_after_sha256'];assert initial==expected
        (work/'SOURCE-MANIFEST.json').write_text(json.dumps(initial,sort_keys=True,indent=2)+'\n')
        assert volume()<=6*1024**3
        run('go-version',[GO,'version'],src);assert (work/'go-version.stdout').read_text().strip()=='go version go1.27.4 darwin/arm64'
        run('modules',[GO,'mod','verify'],src)
        out=work/'artifacts/churn512-driver'
        run('driver-build',[GO,'test','-mod=readonly','-buildvcs=false','-p=1','-trimpath','-tags=d1_resource_probe','-c','-o',str(out),'./internal/core/node'],src)
        guard_source=HERE.parent/'d1-churn512-runtime-inert-v6/canonical/sg32_common.py';assert sha(guard_source)==manifest['static_guard_sha256']
        spec=importlib.util.spec_from_file_location('staticguard',guard_source);guard=importlib.util.module_from_spec(spec);spec.loader.exec_module(guard);guard.static_elf(out.read_bytes())
        run('driver-metadata',[GO,'version','-m',str(out)],src)
        metadata=(work/'driver-metadata.stdout').read_text();assert sha(OLD/'churn512-driver-metadata.stdout')==manifest['prior_metadata_sha256'];prior=(OLD/'churn512-driver-metadata.stdout').read_text()
        assert 'go1.27.4' in metadata and all(x in metadata for x in ('GOOS=linux','GOARCH=arm64','CGO_ENABLED=0','-trimpath=true','-tags=d1_resource_probe'))
        assert [x for x in metadata.splitlines() if x.startswith('\tdep\t')]==[x for x in prior.splitlines() if x.startswith('\tdep\t')]
        assert inventory(src)==initial and inventory(OLD/'source')==frozen and sha(HERE/'driver.go.txt')==manifest['driver_after_sha256']
        assert time.monotonic()<deadline and volume()<=6*1024**3
        artifact=dict(path=str(out),sha256=sha(out),bytes=out.stat().st_size,static_linux_arm64=True,executed=False)
    except BaseException as exc:
        error=str(exc)
        if child and child.poll() is None:
            os.killpg(child.pid,signal.SIGTERM)
            try:child.wait(timeout=max(.001,min(10,cleanup_end-time.monotonic())))
            except subprocess.TimeoutExpired:
                os.killpg(child.pid,signal.SIGKILL)
                try:child.wait(timeout=max(.001,cleanup_end-time.monotonic()))
                except subprocess.TimeoutExpired:known=False
        if child:
            group_absent()
            if steps and steps[-1]['pid']==child.pid:steps[-1]['exit']=child.poll()
    result=dict(status='DRIVER_BUILT' if error is None else 'STOP',attempt_id=attempt,work=str(work),error=error,cleanup_confirmed=known,steps=steps,artifact=artifact,ELF_executed=False,work_limit_s=600,cleanup_end_s=630)
    (work/'result.json').write_text(json.dumps(result,indent=2)+'\n');shutil.copyfile(work/'result.json',HERE/'RESULT.json')
    if known:
        assert custody==(stamp(LOCK),stamp(LOCK/'owner')) and (LOCK/'owner').read_bytes()==owner and set(x.name for x in LOCK.iterdir())=={'owner'}
        (LOCK/'owner').unlink();LOCK.rmdir()
    print(json.dumps(result))
    if error:raise SystemExit(1)


if __name__=='__main__':main()
