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
WORK = {
 "unit": Path("/private/tmp/corsa-d1-o1a-full-send-mechanism-unit-8e84a3f375944ddfbace94eeb70d3f8b"),
 "four-build": Path("/private/tmp/corsa-o1a-observer-coherent-snapshot-elf-build-eb04d5740b2c47eaa3a233b784f64af6"),
 "source-data": Path("/private/tmp/corsa-d1-o1a-observer-coherent-source-data-8378df0ad72d45f2ab58afa7911f37d3"),
 "pid1-build": Path("/private/tmp/corsa-d1-o1a-observer-coherent-pid1-fbeea1be18b14b7982baeae851cb2de8"),
 "capexec-build": Path("/private/tmp/corsa-d1-o1a-observer-coherent-capexec-39d4468aa7e44b2cbb72c911f170b3b4"),
 "pack": Path("/private/tmp/corsa-d1-o1a-initrd-data-assembly-130ff1c9cf9f49e58e2446f64be506b6"),
 "binding": Path("/private/tmp/corsa-d1-o1a-observer-coherent-runtime-data-bf2e5e2e404048c68d9f5e6b336370c7"),
 "native": Path("/private/tmp/corsa-d1-o1a-native-c01f0e036e3ea01e020dee5d88dd8564"),
}
DATA = Path("/private/tmp/corsa-o1a-observer-coherent-inputs-9273f9098aa84d5b8f6002cd68484bb8")
PACKAGES = {
 "unit-source": "d1-o1a-full-send-mechanism-unit-v1",
 "four-build": "d1-o1a-observer-coherent-snapshot-elf-build-v1",
 "source-producer": "d1-o1a-observer-coherent-snapshot-input-rebind-v1",
 "pid1-build": "d1-o1a-observer-coherent-pid1-build-v1",
 "capexec-build": "d1-o1a-observer-coherent-capexec-build-v1",
 "pack": "d1-o1a-observer-coherent-initrd-v1",
 "binding": "d1-o1a-observer-coherent-rebind-v1",
 "runtime": "d1-o1a-observer-coherent-runtime-v1",
}
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
    prior = REPO / "docs/operations/evidence/d1-observer-first-failure-2026-11-20"
    assert sha(read(prior / "SHA256SUMS")) == "413f23257c2295c54587e57a3662c436c71e8c5ce04569e64b3db3ad6ff97576"
    for label, work in WORK.items():
        if label == "native":
            names = ("console.raw", "native.jsonl", "stderr.raw", "controller-result.json", "receipt.json", "expected.json", "worker-start.json", "lock-binding.json", "ROOT-DATA.json", "ROOT-CAPSULE-DATA.json", "TESTER-DATA.json", "TESTER-CAPSULE-DATA.json", "REVIEWER-DATA.json", "REVIEWS.json", "ACK.json")
            for name in names:
                add(RAW, label + "/" + name, work / name)
            continue
        result = json.loads(read(result_path(work)))
        if label == "four-build":
            streams = sorted((work / "raw").iterdir())
            assert len(streams) == 51
        else:
            streams = [Path(step[k]) for step in result["steps"] for k in ("stdout", "stderr")]
            streams += sorted(work.glob("[0-9][0-9]-*.json"))
        for path in streams:
            add(RAW, label + "/" + path.relative_to(work).as_posix(), path)
        add(RAW, label + "/RESULT.json", result_path(work))
        for name in ("ROOT-DATA.json", "REVIEWER-DATA.json", "POLICY-DATA.json", "ROOT-PACK-DATA.json"):
            if (work / name).exists():
                add(RAW, label + "/" + name, work / name)
    for name in ("RESULT.json", "ROOT-DATA.json", "REVIEWER-DATA.json", "SOURCE-ARCHIVE.json", "EXPORTER-REUSE.json", "component.json", "pid1-source.diff", "capexec-source.diff"):
        add(SOURCE, "prepared-data/" + name, DATA / name)
    for label, package in PACKAGES.items():
        for path in sorted((DAY / package).iterdir()):
            if path.is_file() and (path.suffix in {".py", ".json", ".md", ".diff", ".sh", ".manifest"} or path.name.startswith("SHA256SUMS")):
                add(SOURCE, label + "/" + path.name, path)
    unit = DAY / PACKAGES["unit-source"]
    unit_manifest = json.loads(read(unit / "MANIFEST.json"))
    for name, row in unit_manifest["files"].items():
        path = unit / "posts" / name
        assert sha(read(path)) == row["after_sha256"]
        add(SOURCE, "unit-post/" + name, path)
    for name in ("SOURCE-BEFORE.json", "SOURCE-AFTER.json", "overlay.json"):
        add(SOURCE, "unit/" + name, WORK["unit"] / name)
    assert json.loads(read(WORK["unit"] / "SOURCE-BEFORE.json")) == json.loads(read(WORK["unit"] / "SOURCE-AFTER.json"))
    assert len(json.loads(read(WORK["unit"] / "SOURCE-AFTER.json"))) == 2105
    build = DAY / PACKAGES["four-build"]
    add(SOURCE, "source-api-adapter/main.go", build / "source-api-module/cmd/source-api-readback/main.go")
    pins = {p.relative_to(build / "source-api-module").as_posix(): sha(read(p)) for p in sorted((build / "source-api-module").rglob("*.go")) if "cmd" not in p.relative_to(build / "source-api-module").parts}
    SOURCE["source-api-adapter/ORIGINAL-API-PINS.json"] = (json.dumps(pins, indent=2, sort_keys=True) + "\n").encode()
    for name in ("source-before.json", "source-after.json"):
        add(SOURCE, "four-build/" + name, WORK["four-build"] / name)
    add(SOURCE, "pack/reconnect.assembly.json", WORK["pack"] / "reconnect.assembly.json")
    for name in ("corsa-observer-coherent-data.py", "corsa-observer-coherent-runtime-data.py", "corsa-full-send-mechanism-unit.py"):
        add(SOURCE, "root-wrappers/" + name, Path("/private/tmp") / name)
    # Import only the pinned data decoder. Complete capsule transport stays in console.raw.
    decoder_path = DAY / "d1-o1a-guest-preparation-v1/journal_capsule.py"
    assert sha(read(decoder_path)) == "bd81ab5fd7ce88db408fbb9f7d3ea8bd59e89c5486081989154dd047499eadf4"
    add(SOURCE, "capsule-decoder/journal_capsule.py", decoder_path)
    spec = importlib.util.spec_from_file_location("portable_capsule", decoder_path)
    decoder = importlib.util.module_from_spec(spec)
    import sys
    sys.modules[spec.name] = decoder
    spec.loader.exec_module(decoder)
    binding = {"workload": "reconnect", "attempt_id": "c01f0e036e3ea01e020dee5d88dd8564", "profile_sha256": "f84856cfa6f3c3d39f4f5d69487c5d968106aeea77b16f8a3d7bf864266e7369", "source_commit": "4fcc2f7245095fc43d2225f0f0065f4ce3b26113"}
    capsule = decoder.decode(RAW["native/console.raw"], binding)
    assert len(capsule) == 1351
    source_map = json.loads(read(WORK["four-build"] / "source-after.json"))
    go_count = 0
    failed = []
    for name, raw in capsule.items():
        if name.endswith(".go"):
            assert name.startswith("sources/") and sha(raw) == source_map[name.split("/", 2)[2]]
            go_count += 1
        elif name.endswith("/MANIFEST.sha256"):
            add_name = "capsule/" + name
            assert add_name not in RAW
            RAW[add_name] = raw
        else:
            assert name.endswith(".failed") and len(raw) <= 65536
            failed.append(name)
            RAW["capsule/" + name] = raw
    assert go_count == 1348 and len(failed) == 1 and len(capsule[failed[0]]) == 2906
    write_json("CAPSULE-MEMBERS.json", {n: {"bytes": len(v), "sha256": sha(v)} for n, v in sorted(capsule.items())})
    summary = {
      "scope": "DISPOSABLE_OBSERVER_FIRST_FAILURE_DIAGNOSTIC_NOT_BASELINE_NOT_METRIC_NO512",
      "prior_committed_reference": {"commit": "f258093a44aa15dd7ce79f6e6a13df9c0aae363c", "evidence": "docs/operations/evidence/d1-observer-first-failure-2026-11-20", "sha256sums": sha(read(prior / "SHA256SUMS")), "unchanged_sources": "Original observer instrumentation, three coherent namespace fixture posts, original API/helper/controller lineage; no unchanged base tree or ELF duplicated."},
      "attempt": binding["attempt_id"], "profile_sha256": binding["profile_sha256"],
      "Go": {"outcome": "FAIL", "seconds": 30.63, "stage": "observer_full_send", "first_failure_transport_code": 72, "specific_sender_site_or_compound_predicate": "UNKNOWN"},
      "setup": "Advanced beyond the preceding initial multicast guard; this is not a general environment qualification.",
      "capsule": {"files": len(capsule), "Go_files": go_count, "manifests": 2, "failed_records": 1, "failed_body_bytes": 0, "complete_transport": "RAW.tar.xz/native/console.raw", "expanded_Go": "Not duplicated; exact member hashes checked against compiled source-after.json."},
      "source_groups": {"Core_files": 640, "Core_sha256": "9fb7bba6dd77a128a634f0d6d26e67485b07f1178b9361166a754e0fb7d1268e", "stand_files": 708, "stand_sha256": "5aad60ae1d638ae27ebcafd518a31aab4f8c83bb926284e26a22a5117c0f8267"},
      "builds": "17 four-artifact commands; ten PID1 QA outer/five child cases; four Go-helper commands; eight C commands. Pure policy dumper only, not capexec/application execution during builds.",
      "exporter": "Unchanged 7ef3 binary reused by exact compiled closure; not rebuilt.",
      "native": {"worker_exit": 0, "test_exit": 1, "export_exit": 0, "native_events": 4, "all_guest_children_reaped": True, "intervention": False, "physical_XPC": "UNKNOWN", "stopped_only_ACK": "COMPLETE; failed functional outcome retained"},
      "unit_mechanism": {"outer_PASS": 4, "child_PASS": 12, "commands_exit0": 1, "RAW_bytes": 17207, "source_before_after_records": 2105, "scope": "Original sender and real queue; controlled pre-window-to-inside crossing and same-connection projection changes. These possible mechanisms do not establish the actual c01 site or condition.", "review_sha256": sha(read(WORK["unit"] / "REVIEWER-DATA.json"))},
      "old_30s_and_480s_inner_cause": "UNKNOWN; code72 does not establish equivalence.",
      "original_periodic_reconnect_metric_acceptance": "NOT_ESTABLISHED", "D1_acceptance": "NOT_ESTABLISHED",
      "raw": archive("RAW", RAW), "source": archive("SOURCE", SOURCE),
    }
    write_json("SUMMARY.json", summary)
    for label in WORK:
        if label != "native":
            (HERE / (label.upper() + "-RESULT.json")).write_bytes(read(result_path(WORK[label])))
    for name, origin in (("STOP-ACK.json", "ACK.json"), ("STOP-REVIEWER.json", "REVIEWER-DATA.json")):
        (HERE / name).write_bytes(read(WORK["native"] / origin))
    files = sorted(p for p in HERE.iterdir() if p.is_file() and p.name != "SHA256SUMS")
    (HERE / "SHA256SUMS").write_text("".join(sha(read(p)) + "  " + p.name + "\n" for p in files))
    print(json.dumps({"files": len(files) + 1, "raw": summary["raw"], "source": summary["source"], "capsule_files": len(capsule), "sha256sums": sha(read(HERE / "SHA256SUMS"))}))


if __name__ == "__main__":
    main()
