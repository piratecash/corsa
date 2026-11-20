"""Fixed local complement packer. Reads retained data only; no applications or VM calls.

The complete console retains the entire 1351-file capsule. Expanded source bytes
are not duplicated; each capsule member is independently indexed and checked.
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
WORK = {k: Path(v) for k, v in {'qa': '/private/tmp/corsa-d1-o1a-full-send-leaf-goqa-cadf786a7245409d9a35e38ffcd775d1', 'four-build': '/private/tmp/corsa-o1a-full-send-leaf-elf-build-1f50c1e8b0cb41bb9fec0dd1a81fc4bf', 'source-data': '/private/tmp/corsa-d1-o1a-full-send-leaf-source-data-f1c34212489c4619a0215b3ea791a475', 'pid1-build': '/private/tmp/corsa-d1-o1a-full-send-leaf-pid1-66001737711341a4963500b3f2e8cbcb', 'capexec-build': '/private/tmp/corsa-d1-o1a-full-send-leaf-capexec-2155b0d9288247ffbc0450af1644af0d', 'pack': '/private/tmp/corsa-d1-o1a-initrd-data-assembly-8cc28d65f5b444c68c949a10523e7846', 'binding': '/private/tmp/corsa-d1-o1a-full-send-leaf-runtime-data-fdf5ecea5509467ea2e6fa86278e690a', 'native': '/private/tmp/corsa-d1-o1a-native-97a9c06a6fe700ed0cf2c98e8a19f152'}.items()}
DATA = Path("/private/tmp/corsa-o1a-full-send-leaf-inputs-29a5c76170dd42818facb32d2ec16e93")
PACKAGES = {'leaf-source': 'd1-o1a-full-send-leaf-source-v1', 'qa': 'd1-o1a-full-send-leaf-goqa-v1', 'four-build': 'd1-o1a-full-send-leaf-elf-build-v1', 'source-producer': 'd1-o1a-full-send-leaf-input-rebind-v1', 'pid1-build': 'd1-o1a-full-send-leaf-pid1-build-v1', 'capexec-build': 'd1-o1a-full-send-leaf-capexec-build-v1', 'pack': 'd1-o1a-full-send-leaf-initrd-v1', 'binding': 'd1-o1a-full-send-leaf-rebind-v1', 'runtime': 'd1-o1a-full-send-leaf-runtime-v1'}
RAW, SOURCE = {}, {}


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


def write_json(name, data):
    (HERE / name).write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def archive(name, files):
    assert len(files) <= 512 and sum(map(len, files.values())) <= 64 * 1024**2
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
    prior = REPO / 'docs/operations/evidence/d1-observer-full-send-2026-11-20'
    assert sha(read(prior/'SHA256SUMS')) == '71b386eafb8148a941aabc8e9a548bb7a8c64c0cf30bbfd83e8b8bcbf888375f'
    assert sha(read(prior/'prepare_evidence.py')) == '599519731d7546e4fef153420bdb955ba0882826fbaff5f078d9c07026528456'
    for label, work in WORK.items():
        if label == 'native':
            names = ('console.raw','native.jsonl','stderr.raw','controller-result.json','receipt.json',
                     'expected.json','worker-start.json','lock-binding.json','ROOT-DATA.json',
                     'ROOT-CAPSULE-DATA.json','TESTER-DATA.json','TESTER-CAPSULE-DATA.json',
                     'REVIEWER-DATA.json','REVIEWS.json','ACK.json')
            for name in names:
                add(RAW,label+'/'+name,work/name)
            continue
        result = json.loads(read(result_path(work)))
        if label == 'four-build':
            streams = sorted((work/'raw').iterdir())
            assert len(streams) == 51
        else:
            streams = [Path(step[k]) for step in result['steps'] for k in ('stdout','stderr')]
            streams += sorted(work.glob('[0-9][0-9]-*.json'))
        for path in streams:
            add(RAW,label+'/'+path.relative_to(work).as_posix(),path)
        add(RAW,label+'/RESULT.json',result_path(work))
        for name in ('ROOT-DATA.json','REVIEWER-DATA.json','POLICY-DATA.json','ROOT-PACK-DATA.json'):
            if (work/name).exists():
                add(RAW,label+'/'+name,work/name)
    for name in ('RESULT.json','ROOT-DATA.json','REVIEWER-DATA.json','SOURCE-ARCHIVE.json',
                 'EXPORTER-REUSE.json','component.json','pid1-source.diff','capexec-source.diff'):
        add(SOURCE,'prepared-data/'+name,DATA/name)
    data = json.loads(read(DATA/'RESULT.json'))
    for group, names in data['helper_changed_files'].items():
        for name in names:
            path = DATA/group/name
            expected = data['helper_sources'][group]['source_files'][name]
            assert {'bytes':path.stat().st_size,'sha256':sha(read(path))} == expected
            add(SOURCE,'helper-post/'+group+'/'+name,path)
    for label, package in PACKAGES.items():
        for path in sorted((DAY/package).iterdir()):
            if path.is_file() and (path.suffix in {'.py','.json','.md','.diff','.sh','.manifest'} or path.name.startswith('SHA256SUMS')):
                add(SOURCE,label+'/'+path.name,path)
    leaf = DAY/PACKAGES['leaf-source']
    manifest = json.loads(read(leaf/'MANIFEST.json'))
    for table, directory in (('files','posts'),('QA_only_files','qa-posts')):
        for name, row in manifest[table].items():
            path = leaf/directory/name
            assert sha(read(path)) == row.get('after_sha256',row.get('sha256'))
            add(SOURCE,directory+'/'+name,path)
    for name in ('QA-SOURCE-MANIFEST.json','ARTIFACT-SOURCE-MANIFEST.json'):
        add(SOURCE,'qa/'+name,WORK['qa']/name)
    qa = json.loads(read(result_path(WORK['qa'])))
    assert qa['error'] is None and qa['artifact_snapshot_files'] == 2450 and qa['QA_snapshot_files'] == 2455
    assert sum(len(row['passed']) for row in qa['tests'].values()) == 43
    assert sum(row['child_passes'] for row in qa['tests'].values()) == 274
    build = DAY/PACKAGES['four-build']
    add(SOURCE,'source-api-adapter/main.go',build/'source-api-module/cmd/source-api-readback/main.go')
    pins = {p.relative_to(build/'source-api-module').as_posix():sha(read(p))
            for p in sorted((build/'source-api-module').rglob('*.go'))
            if 'cmd' not in p.relative_to(build/'source-api-module').parts}
    SOURCE['source-api-adapter/ORIGINAL-API-PINS.json'] = (json.dumps(pins,indent=2,sort_keys=True)+'\n').encode()
    for name in ('source-before.json','source-after.json'):
        add(SOURCE,'four-build/'+name,WORK['four-build']/name)
    before = json.loads(read(WORK['four-build']/'source-before.json'))
    after = json.loads(read(WORK['four-build']/'source-after.json'))
    assert before == after and len(after) == 2450
    add(SOURCE,'pack/reconnect.assembly.json',WORK['pack']/'reconnect.assembly.json')
    for name in ('corsa-full-send-leaf-data.py','corsa-full-send-leaf-runtime-data.py'):
        add(SOURCE,'root-wrappers/'+name,Path('/private/tmp')/name)
    decoder_path = DAY/'d1-o1a-guest-preparation-v1/journal_capsule.py'
    assert sha(read(decoder_path)) == 'bd81ab5fd7ce88db408fbb9f7d3ea8bd59e89c5486081989154dd047499eadf4'
    # Same decoder is already retained by the linked ac17 evidence; no second
    # complete base-source or binary copy is added to this complement.
    spec = importlib.util.spec_from_file_location('portable_sender_capsule',decoder_path)
    decoder = importlib.util.module_from_spec(spec)
    import sys
    sys.modules[spec.name] = decoder
    spec.loader.exec_module(decoder)
    binding = {'workload':'reconnect','attempt_id':'97a9c06a6fe700ed0cf2c98e8a19f152',
               'profile_sha256':'f84856cfa6f3c3d39f4f5d69487c5d968106aeea77b16f8a3d7bf864266e7369',
               'source_commit':'4fcc2f7245095fc43d2225f0f0065f4ce3b26113'}
    capsule = decoder.decode(RAW['native/console.raw'],binding)
    assert len(capsule) == 1351
    go_count, failed = 0, []
    for name, raw in capsule.items():
        if name.endswith('.go'):
            assert name.startswith('sources/') and sha(raw) == after[name.split('/',2)[2]]
            go_count += 1
        elif name.endswith('/MANIFEST.sha256'):
            RAW['capsule/'+name] = raw
        else:
            assert name.endswith('.failed') and len(raw) <= 65536
            failed.append(name); RAW['capsule/'+name] = raw
    assert go_count == 1348 and len(failed) == 1 and len(capsule[failed[0]]) == 2906
    write_json('CAPSULE-MEMBERS.json',{n:{'bytes':len(v),'sha256':sha(v)} for n,v in sorted(capsule.items())})
    root = json.loads(read(WORK['native']/'ROOT-DATA.json'))
    assert root['observer_first_failure'] == 'observer_full_send_finish_sender_false'
    summary = {
      'scope':'DISPOSABLE_FULL_SEND_SENDER_DIAGNOSTIC_NOT_BASELINE_NOT_METRIC_NO512',
      'prior_committed_reference':{'commit':'ac17ba55c551ed1cb48d4c65482ec6d5175fc04e',
        'evidence':'docs/operations/evidence/d1-observer-full-send-2026-11-20',
        'sha256sums':sha(read(prior/'SHA256SUMS')),
        'lineage':'Same original 4fcc source, coherent fixture, decoder/library/worker/kernel/profile lineage. Prior c01/raw/mechanism evidence is linked, not duplicated.'},
      'attempt':binding['attempt_id'],'profile_sha256':binding['profile_sha256'],
      'Go':{'outcome':'FAIL','seconds':30.60,'observer_reason':31,'transport_code':95,
        'stage':'observer_full_send_finish_sender_false',
        'established':'The original sender returned sentAll=false at observer finish.',
        'underlying_build_dispatch_or_chunker_skip':'UNKNOWN'},
      'historical_c01_30s_and_480s_cause':'UNKNOWN; new diagnostics do not retroactively identify old conditions.',
      'capsule':{'files':1351,'Go_files':go_count,'manifests':2,'failed_records':1,'failed_body_bytes':0,
        'complete_transport':'RAW.tar.xz/native/console.raw','expanded_Go':'Not duplicated; every member hash verified against exact compiled source-after.json.'},
      'source_groups':data['source_groups'],
      'private_source':'Exactly three diagnostic posts and five QA-only posts retained; none applied to the product checkout.',
      'QA':{'outer_PASS':43,'child_PASS':274,'commands_exit0':5,'artifact_files':2450,'QA_files':2455},
      'builds':'17 commands for four Linux test/stand ELF artifacts, including fresh original Go source API and static/BuildInfo metadata; ten PID1 QA outer/five child and four helper commands; eight C commands including pure policy emission. No test/stand ELF artifact executed by these builds.',
      'exporter':'Same accepted 7ef3 binary reused by exact compiled dependency closure.',
      'native':{'worker_exit':0,'test_exit':1,'export_exit':0,'native_events':4,'all_guest_children_reaped':True,
        'intervention':False,'physical_XPC':'UNKNOWN','stopped_only_ACK':'COMPLETE; functional FAIL preserved'},
      'original_periodic_reconnect_metric_acceptance':'NOT_ESTABLISHED','D1_acceptance':'NOT_ESTABLISHED',
      'raw':archive('RAW',RAW),'source':archive('SOURCE',SOURCE),
    }
    write_json('SUMMARY.json',summary)
    for label in WORK:
        if label != 'native':
            (HERE/(label.upper()+'-RESULT.json')).write_bytes(read(result_path(WORK[label])))
    for name, origin in (('STOP-ACK.json','ACK.json'),('STOP-REVIEWER.json','REVIEWER-DATA.json')):
        (HERE/name).write_bytes(read(WORK['native']/origin))
    files = sorted(p for p in HERE.iterdir() if p.is_file() and p.name != 'SHA256SUMS')
    (HERE/'SHA256SUMS').write_text(''.join(sha(read(p))+'  '+p.name+'\n' for p in files))
    print(json.dumps({'files':len(files)+1,'raw':summary['raw'],'source':summary['source'],
                     'capsule_files':1351,'sha256sums':sha(read(HERE/'SHA256SUMS'))}))


if __name__ == '__main__':
    main()
