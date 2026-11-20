"""Fixed portable Hmix snapshotter; retained data only, no app or VM calls.

Old logs remain in the original console transport and as verified decoded bytes.
No ELF, image layer, downloaded package bulk or compiler cache is copied.
"""
from pathlib import Path, PurePosixPath
import hashlib
import importlib.util
import io
import json
import tarfile

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
DAY = REPO / "docs/_internal/d1-artifacts/2026-11-20"
RAW, SOURCE, ORIGINS = {}, {}, {}


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def read(path, bound=16 * 1024**2):
    path = Path(path)
    assert path.is_file() and not path.is_symlink() and 0 <= path.stat().st_size <= bound
    raw = path.read_bytes()
    assert len(raw) <= bound and not raw.startswith(b"\x7fELF")
    return raw


def add(table, name, path):
    p = PurePosixPath(name)
    assert name not in table and name == p.as_posix() and not p.is_absolute() and ".." not in p.parts
    table[name] = read(path)
    ORIGINS[('RAW/' if table is RAW else 'SOURCE/')+name] = {'path':str(Path(path).resolve()),
        'bytes':len(table[name]),'sha256':sha(table[name])}


def write_json(name, data):
    (HERE / name).write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def archive(name, files):
    assert len(files) <= 1024 and sum(map(len, files.values())) <= 64 * 1024**2
    path = HERE / (name + ".tar.xz")
    with tarfile.open(path, "w:xz", format=tarfile.USTAR_FORMAT, preset=6) as out:
        for n, raw in sorted(files.items()):
            member = tarfile.TarInfo(n)
            member.size = len(raw)
            member.mode = 0o644
            member.uid = member.gid = member.mtime = 0
            member.uname = member.gname = ""
            out.addfile(member, io.BytesIO(raw))
    inventory = {n: {"bytes": len(v), "sha256": sha(v)} for n, v in sorted(files.items())}
    write_json(name + "-MEMBERS.json", inventory)
    seen = set()
    with tarfile.open(path, "r:xz") as inp:
        for member in inp:
            assert member.isfile() and not member.pax_headers and member.name not in seen
            assert member.mode == 0o644 and member.uid == member.gid == member.mtime == 0
            assert inp.extractfile(member).read() == files[member.name]
            seen.add(member.name)
    assert seen == files.keys()
    return {"sha256": sha(read(path)), "members": len(files), "payload_bytes": sum(map(len, files.values()))}


def result_path(work):
    return work / ("result.json" if (work / "result.json").exists() else "RESULT.json")


def main():
    import base64
    import re
    import subprocess
    compile_work = Path('/private/tmp/corsa-d1-o1a-hmix-published31-goqa-2b1dcc00713d42b1bf26d9a9919f73a8')
    guest_data = Path('/private/tmp/corsa-d1-o1a-hmix-published31-guest-data-19c63a295cdc4280af8a08d6fbe00cc1')
    environment = {
      'prep1':Path('/private/tmp/corsa-d1-o1a-hmix-qemu-environment-6eb376658cf44478a29a40703f62f320'),
      'prep3':Path('/private/tmp/corsa-d1-o1a-hmix-qemu-environment-7ff585a82dc94891bb5b0f29534c3edb'),
      'prep4':Path('/private/tmp/corsa-d1-o1a-hmix-qemu-environment-bf7540eee8994db4adbca4c902adf518'),
      'qualification':Path('/private/tmp/corsa-d1-o1a-hmix-qemu-lo-probe-3ed22de52b924c42949c25997e8f7174'),
    }
    functional = Path('/private/tmp/d1-hmix-published31-linux-guest-v1-3abb1637f9541f5751438c0501c9406e')
    source = DAY/'d1-hmix-published31-linux-guest-source-v1'
    runtime = DAY/'d1-hmix-published31-linux-guest-source-v2'
    packages = {
      'goqa':'d1-hmix-published31-goqa-v1',
      'functional-source':'d1-hmix-published31-linux-guest-source-v1',
      'functional-runtime':'d1-hmix-published31-linux-guest-source-v2',
      'published-audit':'d1-hmix-published31-source-audit-v1',
      'guest-binding':'d1-hmix-published31-guest-binding-v1',
      'qualification-source-v1':'d1-hmix-qemu-lo-qualification-source-v1',
      'qualification-source-v2':'d1-hmix-qemu-lo-qualification-source-v2',
      **{'environment-source-v'+str(i):'d1-hmix-published31-qemu-environment-source-v'+str(i) for i in range(1,6)},
      'goqa-tester':'d1-hmix-published31-goqa-actual-tester-v1',
      'guest-tester':'d1-hmix-published31-guest-data-actual-tester-v1',
      'environment-tester':'d1-hmix-qemu-environment-prep4-actual-tester-v1',
      'qualification-architect':'d1-hmix-qemu-qualification-architect-data-v1',
      'runtime-tester':'d1-hmix-runtime-v2-tester-review-v1',
      'runtime-architect':'d1-hmix-published31-architect-data-v1',
    }
    for label, work in {'compile':compile_work,**environment}.items():
        result = json.loads(read(result_path(work)))
        paths = {Path(step[k]) for step in result['steps'] for k in ('stdout','stderr')}
        paths.update(work.glob('[0-9][0-9]-*.json'))
        for path in sorted(paths):
            add(RAW,label+'/'+path.relative_to(work).as_posix(),path)
        add(RAW,label+'/RESULT.json',result_path(work))
        for path in sorted(work.glob('*.json')):
            name = label+'/'+path.name
            if name not in RAW and path.name not in {'SOURCE-BEFORE.json','SOURCE-AFTER.json','SOURCE-EXPECTED.json'}:
                add(RAW,name,path)
    prep4 = environment['prep4']
    for path in sorted((prep4/'evidence').iterdir()):
        if path.name in {'APKINDEX-main.tar.gz','APKINDEX-community.tar.gz','alpine-devel@lists.alpinelinux.org-616ae350.rsa.pub'}:
            add(SOURCE,'authenticated-inputs/'+path.name,path)
        else:
            add(RAW,'prep4-inner/'+path.name,path)
    for path in sorted((prep4/'root-reconciliation').iterdir()):
        if path.is_file() and path.suffix in {'.json','.stdout','.stderr','.txt'}:
            add(RAW,'recovery/'+path.name,path)
    for path in sorted(functional.iterdir()):
        if path.is_file() and (path.suffix in {'.json','.out','.err','.log','.stdout','.stderr'}):
            add(RAW,'functional/'+path.name,path)
    for name in ('ASSEMBLY.json','RESULT.json'):
        add(RAW,'guest-data/'+name,guest_data/name)
    for label, package in packages.items():
        for path in sorted((DAY/package).iterdir()):
            if path.is_file() and (path.suffix in {'.py','.json','.md','.diff','.sh','.manifest'} or path.name.startswith('SHA256SUMS')):
                add(SOURCE,label+'/'+path.name,path)
    manifest = json.loads(read(source/'MANIFEST.json'))
    for name,row in manifest['files'].items():
        path = source/row['post']
        assert sha(read(path)) == row['after_sha256'] == sha(read(REPO/name))
        add(SOURCE,'tested-posts/'+name,path)
    for name in ('SOURCE-BEFORE.json','SOURCE-AFTER.json','SOURCE-EXPECTED.json'):
        add(SOURCE,'compile/'+name,compile_work/name)
    expected = json.loads(read(compile_work/'SOURCE-EXPECTED.json'))
    assert expected == json.loads(read(compile_work/'SOURCE-AFTER.json'))
    selected = {n:v for n,v in expected.items() if n.endswith('.go') or n in ('go.mod','go.sum')}
    tracked = set(subprocess.check_output(['/usr/bin/git','-C',str(REPO),'ls-files','-z','--','*.go','go.mod','go.sum']).decode().rstrip('\0').split('\0'))
    tracked.update(manifest['files'])
    extra = 'internal/core/node/loadstand_route_wire_full_send_mechanism_test.go'
    assert tracked-set(selected) == {extra} and set(selected) <= tracked
    assert sha(read(REPO/extra)) == 'fe1aa5a317538098266ba68cd9694886e7d0ff929f75a45729454010d373775f'
    assert all(sha(read(REPO/n)) == v for n,v in selected.items())
    checkpoint = {'compile_base_commit':'f258093a44aa15dd7ce79f6e6a13df9c0aae363c',
      'root_parent_before_test_apply':'cecc9b709e00fbecbbc91c0920f101f7e3b7729b',
      'only_two_test_posts':manifest['files'],'production_changes':0,
      'all_compiled_Go_and_module_files_equal_current_Root':len(selected),
      'additional_current_test_not_in_earlier_compile_subject':{
        'path':extra,'sha256':sha(read(REPO/extra)),
        'scope':'Already committed ac17 mechanism unit; only separate test/helper functions, no product change or Hmix call.',
        'prior_evidence':'docs/operations/evidence/d1-observer-full-send-2026-11-20/README.md'},
      'unrelated_untracked_files':'Not included or modified',
      'proc_parser_negative_function':'COMPILED_NOT_SELECTED_NOT_RUN'}
    write_json('SOURCE-CHECKPOINT.json',checkpoint)
    # Original helper lineage: exact finite copies, never a host/app execution.
    for origin in (
      REPO/'scripts/heavy-run.sh',
      DAY.parent/'2026-11-13/s6-amd64-lo-only-guest-v2/probe_profile.py',
      DAY.parent/'2026-11-13/s6-amd64-lo-only-guest-v2/run_preparation.py',
      DAY.parent/'2026-11-13/s6-amd64-lo-only-guest-v2/init.sh',
      DAY.parent/'2026-11-13/s6-amd64-lo-only-guest-v2/build_initramfs.py',
      DAY.parent/'2026-11-12/s6-baseline-restart-v4/controller.py',
      DAY.parent/'2026-11-12/s6-baseline-restart-v4/host_stage.py',
      DAY.parent/'2026-10-28/s6-common32-both31-runtime-source-v2/runtime/sg32_common.py',
      DAY.parent/'2026-10-28/s6-common32-both31-runtime-source-v2/runtime/sg32_host.py',
      DAY/'d1-o1a-capexec-seccomp-build-v1/common.py',
      DAY.parent/'2026-11-12/d1-full512-toolchain-index/verify_index.py'):
        label = 'accepted-lineage/'+origin.parent.name+'/'+origin.name
        add(SOURCE,label,origin)
    old = DAY.parent/'2026-11-12/s6-baseline-v2571-published-linux-amd64'
    add(SOURCE,'published-old/STATIC.json',old/'STATIC.json')
    add(SOURCE,'published-old/official-release-metadata.json',DAY.parent/'2026-10-28/s6-published-release-refresh-v1/v2.5.71.connector-decoded.json')
    # Full original old logs are recoverable from console.raw and also retained
    # decoded (only 64 KiB) for direct review, with every hash checked.
    console = read(functional/'console.log')
    logs = {}
    for line in console.decode().splitlines():
        if 'HMIX_OLD_RAW ' not in line:
            continue
        match = re.search(r'HMIX_OLD_RAW case=([^ ]+) name=([^ ]+) sha256=([0-9a-f]{64}) bytes=([0-9]+) payload=([A-Za-z0-9+/=]*)$',line)
        assert match
        case,name,digest,size,encoded = match.groups()
        body = base64.b64decode(encoded,validate=True)
        assert len(body) == int(size) and sha(body) == digest
        key = case+'/'+name
        assert key not in logs
        RAW['old-logs/'+key] = body
        logs[key] = {'bytes':len(body),'sha256':digest}
    assert len(logs) == 6 and sum(row['bytes'] for row in logs.values()) == 64170
    write_json('OLD-LOG-MEMBERS.json',logs)
    spec = importlib.util.spec_from_file_location('hmix_evidence_decoder',runtime/'decode.py')
    decoder = importlib.util.module_from_spec(spec);spec.loader.exec_module(decoder)
    functional_result = json.loads(read(functional/'result.json'))
    assert all(functional_result[k] == v for k,v in decoder.verify_console(console).items())
    summary = {
      'scope':'PUBLISHED31_OFF_VS_PRIVATE_R_FUNCTIONAL_ONLY_NOT_13B_512_K11_D1_G2_OR_RELEASE',
      'functional':{'result':'PASS','outer':1,'child':2,'seconds':7.09,
        'dial_direction':'old_to_current','DM_direction':'current_to_old',
        'current_subject':'genuine Service in static Node.test, ordinary Off and private immutable Responder',
        'old_subject':'fixed published v2.5.71/protocol31 ELF 06e26f9b…, not a rebuild',
        'decryption':'real old daemon inbox RPC + current test decoder; NOT old Desktop',
        'F1':'Pending1/Taken1/Answered0 during real inbox fetch/decryption; signed return and same opaque sessions',
        'receipts':'delivered/seen/seen_ack in both cases, after FN return in R case',
        'old_cleanup':'normal0 and group ESRCH in both exact Go cases'},
      'compile':{'source_guards_outer_PASS':7,'child_PASS':15,'commands_exit0':7,
        'artifact':'static Linux amd64/CGO0 Node.test','bytes':75315642,
        'sha256':'6b31bbdcab39095727e00389d94b75638433e3e5c1a84e796223f514bf0baf65',
        'toolchain':'Go1.27.4','BuildInfo_dependencies':26,'source_files':2727},
      'source_checkpoint':checkpoint,
      'environment':{'qualified_image':'sha256:45009926747bc2be5493d1d28f84f8a40b358f9bb600f5ff9c1724ff201913e1',
        'qualification':'x86_64/glibc2.36/exact {lo} in boot and distinct fresh netns/normal0',
        'prep1':'STOP before create/network: unrelated valid UTF8 certificate name',
        'prep3':'STOP before process: local log-driver compression/max-file1 mismatch',
        'prep4':'installation normal0; original controller STOP/HOLD preserved on commit banner/inherited Python entrypoint',
        'recovery':'same stopped CID metadata-only commit, no install/start; exact owned CID and obsolete4b60 removed, original controller/lock reconciled after three-role ACK',
        'signed_indexes_and_key':'Both fresh actual signed Alpine indexes and exact trusted public key retained.',
        'package_payloads':'54 actual apk verify records and receipts retained; downloaded .apk/deb bulk not duplicated.',
        'needed_image':'450 retained for this evidence/possible further old interop; no foreign image deleted',
        'future_prep_v5':'source-only metadata correction, NOT executed or used to relabel prep4'},
      'runtime':{'nonce':'3abb1637f9541f5751438c0501c9406e','Docker_commands_exit0':16,
        'complete_RAW_bytes':108224,'console_bytes':88507,'old_log_bytes':64170,
        'QEMU_exit':0,'OOM':False,'owned_container_removed':True,'owned_volumes_remaining':False,
        'historical_host_PID_PGID':'NOT_RECORDED; pinned wrapper terminal0/group-empty contract + fresh Root absence retained',
        'guest_old_PID_PGID':'genuine proc/identity/start checks + normal wait/group absence asserted in exact passing Go cases',
        'images_created_or_removed_by_functional_attempt':False},
      'profile':'QEMU group1CPU/1536MiB/64PID/noSwap; guest1CPU/768MiB/noNIC/disks/KVM; host180+30; postsetup120+10; Go90; one own readonly archive mount',
      'timing_or_memory_statistics':'NOT_MEASURED',
      'separate_512_evidence':'docs/operations/evidence/d1-churn512-current-2026-11-20/README.md; separate frozen subject, not replaced by Hmix',
      'raw':archive('RAW',RAW),'source':archive('SOURCE',SOURCE),
    }
    write_json('SUMMARY.json',summary)
    write_json('ORIGINS.json',ORIGINS)
    readable = {
      'FUNCTIONAL-RESULT.json':functional/'result.json','TESTER-DATA.json':functional/'TESTER-DATA.json',
      'REVIEWER-DATA.json':functional/'REVIEWER-DATA.json','ROOT-TERMINAL-REAP.json':functional/'ROOT-TERMINAL-REAP.json',
      'COMPILE-RESULT.json':compile_work/'RESULT.json','GUEST-ASSEMBLY.json':guest_data/'ASSEMBLY.json',
      'QUALIFICATION-RESULT.json':environment['qualification']/'RESULT.json',
      'ENVIRONMENT-ORIGINAL-STOP.json':prep4/'RESULT.json','ENVIRONMENT-METADATA-ACK.json':prep4/'ACK.json',
    }
    for name,origin in readable.items():
        (HERE/name).write_bytes(read(origin))
    files = sorted(p for p in HERE.iterdir() if p.is_file() and p.name != 'SHA256SUMS')
    (HERE/'SHA256SUMS').write_text(''.join(sha(read(p))+'  '+p.name+'\n' for p in files))
    print(json.dumps({'files':len(files)+1,'raw':summary['raw'],'source':summary['source'],
                     'sha256sums':sha(read(HERE/'SHA256SUMS'))}))


if __name__ == '__main__':
    main()
