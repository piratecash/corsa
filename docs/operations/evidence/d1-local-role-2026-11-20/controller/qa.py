"""Bounded clean-snapshot CLOSED-role GoQA; Linux ELF compile/metadata only."""
from pathlib import Path
import hashlib
import importlib.util
import json
import os
import re
import shutil
import signal
import subprocess
import tarfile
import time
import uuid

HERE=Path(__file__).resolve().parent
REPO=HERE.parents[4]
ROLE=HERE.parent/'d1-directional-role-implementation-v3'
BASE='1b97da9a8c581f7a40664aa5b5eb78c59f104f3c'
ROLE_SHA='fdd45820d3af22e46101ba3b7e9212cfdf59cfcaa1232b5169e59aba6a90dd7e'
HELPER=HERE.parent/'d1-sg512-target-history-driver-build-v2/build_driver.py'
HELPER_SHA='1e703c9856b281979bb63f12df11a7c5f0d50152001ff8a557f1b44289a669cc'
COMMON=HERE.parent/'d1-churn512-runtime-inert-v7/canonical/sg32_common.py'
COMMON_SHA='2fa268e62081939cd4632b8a8deb59b2fe7d29e859b53769d59f552305a1ef33'
GO='/usr/local/go/bin/go'
LOCK=Path('/private/tmp/corsa-heavy-run.lock')
RAW_LIMIT=8*1024**2


def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def load_bound(name,path,digest):
    assert sha(path)==digest,'bound helper changed'
    spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def check_events(raw,expected):
    rows=[json.loads(line) for line in raw.splitlines()]
    assert rows and not any(row.get('Action') in ('fail','skip') for row in rows),'FAIL/SKIP'
    passed=[row['Test'] for row in rows if row.get('Action')=='pass' and row.get('Test') and '/' not in row['Test']]
    assert len(passed)==len(set(passed)) and set(passed)==set(expected),'missing/extra named tests'
    assert any(row.get('Action')=='pass' and not row.get('Test') for row in rows),'missing package PASS'
    return dict(passed=passed,child_passes=sum(row.get('Action')=='pass' and '/' in row.get('Test','') for row in rows))


def main():
    os.umask(0o077)
    if os.getpid()!=os.getpgrp():os.setsid()
    origin=time.monotonic();work_end=origin+900;cleanup_end=origin+930
    manifest=json.loads((HERE/'HANDOFF.json').read_bytes());tests=json.loads((HERE/'TESTS.json').read_bytes())
    assert manifest['controller_sha256']==sha(__file__) and manifest['tests_sha256']==sha(HERE/'TESTS.json')
    assert sha(ROLE/'MANIFEST.json')==ROLE_SHA
    role=json.loads((ROLE/'MANIFEST.json').read_bytes());assert role['base_commit']==BASE
    for name,row in role['files'].items():
        assert sha(ROLE/'posts'/name)==row['after_sha256']==sha(REPO/name),'role/current applied source drift'
    helper=load_bound('accepted_role_job_helper',HELPER,HELPER_SHA)
    common=load_bound('accepted_role_elf_guard',COMMON,COMMON_SHA)
    assert shutil.disk_usage('/private/tmp').free>=10*1024**3
    attempt=uuid.uuid4().hex;work=Path('/private/tmp/corsa-d1-closed-role-goqa-'+attempt);work.mkdir(mode=0o700)
    LOCK.mkdir(mode=0o700);owner=f'name=d1-closed-role-goqa-v3\npid={os.getpid()}\npgid={os.getpgrp()}\nattempt_id={attempt}\n'.encode();(LOCK/'owner').write_bytes(owner)
    custody=(helper.stamp(LOCK),helper.stamp(LOCK/'owner'));steps=[];artifacts={};child=None;known=True;error=None;initial=None
    env={**os.environ,'PATH':'/usr/local/go/bin:'+os.environ.get('PATH',''),'GO111MODULE':'on','GOROOT':'/usr/local/go','GOENV':'off','GOWORK':'off','GOVCS':'*:off','GODEBUG':'','GOTOOLCHAIN':'local','GOPROXY':'off','GOSUMDB':'off','GOTELEMETRY':'off','GOMAXPROCS':'2','GOMEMLIMIT':'768MiB','GOFLAGS':'-p=1','GOCACHE':str(work/'cache'),'GOMODCACHE':str(REPO/'.gomodcache'),'TMPDIR':str(work/'tmp'),'GOOS':'darwin','GOARCH':'arm64','CGO_ENABLED':'0','GOARM64':'v8.0'}
    policy='(version 1)(allow default)(deny network*)(allow network-inbound (local ip "localhost:*"))(allow network-outbound (remote ip "localhost:*"))(deny file-write* (subpath '+json.dumps(str(work/'source'))+'))'
    def raw_size():return sum(Path(row[key]).stat().st_size for row in steps for key in ('stdout','stderr'))
    def absent():
        nonlocal known
        if not child:return True
        try:os.killpg(child.pid,0)
        except ProcessLookupError:return True
        known=False;return False
    def phase_files():
        # Only after process-group absence: no live Go temp/cache rglob/stat.
        assert helper.work_volume(work)<=6*1024**3 and raw_size()<=RAW_LIMIT,'phase file/RAW bound'
    def run(name,args,cwd,extra=None,expected=None):
        nonlocal child
        assert time.monotonic()<work_end
        row=dict(phase=name,argv=args,pid=None,exit=None,stdout=str(work/(name+'.stdout')),stderr=str(work/(name+'.stderr')));steps.append(row)
        with Path(row['stdout']).open('xb') as out,Path(row['stderr']).open('xb') as err:
            child=subprocess.Popen(['/usr/bin/sandbox-exec','-p',policy,*args],cwd=cwd,env=env|(extra or {}),stdout=out,stderr=err,start_new_session=True);row['pid']=child.pid
            while child.poll() is None:
                assert time.monotonic()<work_end and raw_size()<=RAW_LIMIT,'work deadline/RAW bound'
                time.sleep(.1)
        row['exit']=child.returncode
        assert absent(),'owned command group remains'
        phase_files()
        assert child.returncode==0,name+' failed'
        if expected is not None:row['tests']=check_events(Path(row['stdout']).read_bytes(),expected)
    try:
        (work/'tmp').mkdir();(work/'artifacts').mkdir();src=work/'source';src.mkdir()
        run('archive',['/usr/bin/git','archive','--format=tar','--output='+str(work/'base.tar'),BASE],REPO)
        assert (work/'base.tar').stat().st_size<=1024**3
        with tarfile.open(work/'base.tar','r:') as archive:
            for member in archive:
                parts=Path(member.name).parts;assert parts and not Path(member.name).is_absolute() and '..' not in parts
                target=src/member.name
                if member.isdir():target.mkdir(parents=True,exist_ok=True)
                else:
                    assert member.isfile(),'archive links/special entries refused';target.parent.mkdir(parents=True,exist_ok=True)
                    with archive.extractfile(member) as inp,target.open('xb') as out:shutil.copyfileobj(inp,out)
                    target.chmod(member.mode&0o777)
                assert time.monotonic()<work_end
        for name,row in role['files'].items():
            target=src/name;assert (sha(target) if target.exists() else None)==row['before_sha256'],'clean base mismatch'
            target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(ROLE/'posts'/name,target);assert sha(target)==row['after_sha256']
        initial=helper.inventory(src);(work/'SOURCE-MANIFEST.json').write_text(json.dumps(initial,sort_keys=True,indent=2)+'\n')
        (work/'INPUTS.json').write_text(json.dumps(dict(base_commit=BASE,base_tar_sha256=sha(work/'base.tar'),role_manifest_sha256=ROLE_SHA,role=role,tests=tests,helper_sha256=HELPER_SHA,common_sha256=COMMON_SHA,seatbelt=policy),indent=2)+'\n');phase_files()
        run('go-version',[GO,'version'],src);assert (work/'go-version.stdout').read_text().strip()=='go version go1.27.4 darwin/arm64'
        run('modules',[GO,'mod','verify'],src)
        for name,group in tests.items():
            args=[GO,'test','-mod=readonly','-buildvcs=false','-p=1','-parallel=1','-count=1','-timeout=300s','-json']
            if group['tags']:args+=['-tags='+','.join(group['tags'])]
            args+=['-run=^('+'|'.join(re.escape(value) for value in group['tests'])+')$',group['package']]
            run(name,args,src,expected=group['tests'])
        sums={tuple(line.split()[:2]):line.split()[2] for line in (src/'go.sum').read_text().splitlines() if len(line.split())==3}
        for name,package in [('node','./internal/core/node'),('app','./internal/app/node')]:
            artifact=work/'artifacts'/(name+'.test')
            run(name+'-linux-compile',[GO,'test','-mod=readonly','-buildvcs=false','-p=1','-trimpath','-tags=d1_resource_probe','-c','-o',str(artifact),package],src,extra={'GOOS':'linux','GOARCH':'arm64','CGO_ENABLED':'0'})
            common.static_elf(artifact.read_bytes())
            run(name+'-metadata',[GO,'version','-m',str(artifact)],src)
            metadata=(work/(name+'-metadata.stdout')).read_text();assert all(x in metadata for x in ('go1.27.4','GOOS=linux','GOARCH=arm64','CGO_ENABLED=0','-tags=d1_resource_probe','-trimpath=true'))
            dependencies=[]
            for line in metadata.splitlines():
                if not line.startswith('\tdep\t'):continue
                fields=line.split();assert len(fields)==4 and sums.get((fields[1],fields[2]))==fields[3],'BuildInfo dependency mismatch';dependencies.append(fields[1:])
            assert dependencies and len(dependencies)==len({row[0] for row in dependencies})
            artifacts[name]=dict(path=str(artifact),sha256=sha(artifact),bytes=artifact.stat().st_size,static_linux_arm64=True,executed=False,dependencies=dependencies)
        assert helper.inventory(src)==initial,'source modified during QA/compile'
        assert sha(ROLE/'MANIFEST.json')==ROLE_SHA and sha(HELPER)==HELPER_SHA and sha(COMMON)==COMMON_SHA
        for name,row in role['files'].items():assert sha(ROLE/'posts'/name)==row['after_sha256']==sha(REPO/name)
        assert time.monotonic()<work_end;phase_files()
    except BaseException as exc:
        error=repr(exc)
        if child and child.poll() is None:
            os.killpg(child.pid,signal.SIGTERM)
            try:child.wait(timeout=max(.001,min(10,cleanup_end-time.monotonic())))
            except subprocess.TimeoutExpired:
                os.killpg(child.pid,signal.SIGKILL)
                try:child.wait(timeout=max(.001,cleanup_end-time.monotonic()))
                except subprocess.TimeoutExpired:known=False
        if child:
            absent()
            if steps and steps[-1]['pid']==child.pid:steps[-1]['exit']=child.poll()
    result=dict(status='CLOSED_ROLE_QA_AND_COMPILE_PASS' if error is None else 'STOP',error=error,attempt_id=attempt,work=str(work),base_commit=BASE,role_manifest_sha256=ROLE_SHA,steps=steps,artifacts=artifacts,cleanup_confirmed=known,linux_ELF_executed=False,work_limit_s=900,cleanup_end_s=930)
    (work/'result.json').write_text(json.dumps(result,indent=2)+'\n');shutil.copyfile(work/'result.json',HERE/'RESULT.json')
    if known:
        assert custody==(helper.stamp(LOCK),helper.stamp(LOCK/'owner')) and (LOCK/'owner').read_bytes()==owner and set(x.name for x in LOCK.iterdir())=={'owner'}
        (LOCK/'owner').unlink();LOCK.rmdir()
    print(json.dumps(dict(status=result['status'],error=error,attempt_id=attempt,work=str(work),cleanup_confirmed=known)))
    if error:raise SystemExit(1)


if __name__=='__main__':main()
