"""Fixed retained-data snapshot; no Go, compiler, application, VM or Git commands."""
from pathlib import Path, PurePosixPath
import hashlib
import importlib.util
import io
import json
import sys
import tarfile

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
DAY = REPO / "docs/_internal/d1-artifacts/2026-11-20"
DATA = Path("/private/tmp/corsa-o1a-zero-offer-inputs-58d83a83fb1047ee80b323ca5b4a958f")
WORK = {
    "qa": Path("/private/tmp/corsa-d1-o1a-zero-offer-private-goqa-9a21e3e8c3f84d278810991efd0497e4"),
    "four-build": Path("/private/tmp/corsa-o1a-zero-offer-private-elf-build-4ffb25a64e9d40fbad9eb0ec77f7e618"),
    "source-data": Path("/private/tmp/corsa-d1-o1a-zero-offer-source-data-bc6c4bc531dd43ffaf054a6419b37a6e"),
    "pid1-build": Path("/private/tmp/corsa-d1-o1a-zero-offer-pid1-b36d6869ef424c418ed85bcb3bb26ffa"),
    "capexec-build": Path("/private/tmp/corsa-d1-o1a-zero-offer-capexec-40bb2671bf2747c7857498a17708597c"),
    "pack": Path("/private/tmp/corsa-d1-o1a-initrd-data-assembly-a79ac1f8f1e544a1881267ef19562c12"),
    "binding": Path("/private/tmp/corsa-d1-o1a-zero-offer-runtime-data-f5853c3f826249efaf09e6bd209f7a9d"),
    "native": Path("/private/tmp/corsa-d1-o1a-native-2c75180d2872646aa2f7201b240cb175"),
}
PACKAGES = {
    "private-subject-v1": "d1-o1a-zero-offer-private-source-v1",
    "private-subject-v2": "d1-o1a-zero-offer-private-source-v2",
    "qa": "d1-o1a-zero-offer-private-goqa-v1",
    "four-build": "d1-o1a-zero-offer-private-elf-build-v1",
    "source-producer": "d1-o1a-zero-offer-input-rebind-v1",
    "pid1-build": "d1-o1a-zero-offer-pid1-build-v1",
    "capexec-build": "d1-o1a-zero-offer-capexec-build-v1",
    "pack": "d1-o1a-zero-offer-initrd-v1",
    "binding": "d1-o1a-zero-offer-rebind-v1",
    "runtime": "d1-o1a-zero-offer-runtime-v1",
    "qa-architect": "d1-o1a-zero-offer-private-goqa-architect-data-v1",
    "build-architect": "d1-o1a-zero-offer-four-elf-architect-data-v1",
    "data-architect": "d1-o1a-zero-offer-inputs-architect-data-v1",
    "data-tester": "d1-o1a-zero-offer-inputs-actual-tester-v1",
    "helpers-architect": "d1-o1a-zero-offer-helper-architect-data-v1",
    "pack-architect": "d1-o1a-zero-offer-pack-architect-data-v1",
    "pack-tester": "d1-o1a-zero-offer-initrd-actual-tester-v1",
    "runtime-architect": "d1-o1a-zero-offer-runtime-architect-data-v1",
    "native-tester": "d1-o1a-zero-offer-native-actual-tester-v1",
}
RAW, SOURCE, ORIGINS = {}, {}, {}


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def read(path):
    path = Path(path)
    assert path.is_file() and not path.is_symlink() and 0 <= path.stat().st_size <= 16 * 1024**2
    raw = path.read_bytes()
    assert len(raw) <= 16 * 1024**2 and not raw.startswith(b"\x7fELF")
    return raw


def add(table, name, path):
    n = PurePosixPath(name)
    assert name not in table and n.as_posix() == name and not n.is_absolute() and ".." not in n.parts
    table[name] = read(path)
    ORIGINS[("RAW/" if table is RAW else "SOURCE/") + name] = {
        "path": str(Path(path).resolve()), "bytes": len(table[name]), "sha256": sha(table[name])}


def write_json(name, data):
    (HERE / name).write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def archive(name, files):
    assert len(files) <= 512 and sum(map(len, files.values())) <= 64 * 1024**2
    path = HERE / (name + ".tar.xz")
    with tarfile.open(path, "w:xz", format=tarfile.USTAR_FORMAT, preset=6) as out:
        for n, raw in sorted(files.items()):
            member = tarfile.TarInfo(n)
            member.size, member.mode = len(raw), 0o644
            member.uid = member.gid = member.mtime = 0
            member.uname = member.gname = ""
            out.addfile(member, io.BytesIO(raw))
    write_json(name + "-MEMBERS.json", {
        n: {"bytes": len(v), "sha256": sha(v)} for n, v in sorted(files.items())})
    seen = set()
    with tarfile.open(path, "r:xz") as inp:
        for member in inp:
            assert member.isfile() and not member.pax_headers and member.name not in seen
            assert member.uid == member.gid == member.mtime == 0 and member.mode == 0o644
            assert inp.extractfile(member).read() == files[member.name]
            seen.add(member.name)
    assert seen == files.keys()
    return {"sha256": sha(read(path)), "members": len(files), "payload_bytes": sum(map(len, files.values()))}


def result_path(work):
    return work / ("result.json" if (work / "result.json").exists() else "RESULT.json")


def main():
    for label, work in WORK.items():
        if label == "native":
            for name in ("console.raw", "native.jsonl", "stderr.raw", "controller-result.json",
                         "receipt.json", "expected.json", "worker-start.json", "lock-binding.json",
                         "ROOT-DATA.json", "ROOT-CAPSULE-DATA.json", "REVIEWER-DATA.json",
                         "REVIEWER-CAPSULE-INVENTORY.json", "REVIEWS.json", "ACK.json"):
                add(RAW, label + "/" + name, work / name)
            continue
        result = json.loads(read(result_path(work)))
        paths = sorted((work / "raw").iterdir()) if label == "four-build" else [
            Path(step[k]) for step in result["steps"] for k in ("stdout", "stderr")]
        if label != "four-build":
            paths += sorted(work.glob("[0-9][0-9]-*.json"))
        for path in paths:
            add(RAW, label + "/" + path.relative_to(work).as_posix(), path)
        add(RAW, label + "/RESULT.json", result_path(work))
        for path in sorted(work.glob("*DATA.json")):
            add(RAW, label + "/" + path.name, path)
    for label, package in PACKAGES.items():
        folder = DAY / package
        for path in sorted(folder.iterdir()):
            if path.is_file() and path.suffix != ".xz":
                add(SOURCE, label + "/" + path.name, path)
    subject = DAY / PACKAGES["private-subject-v2"]
    manifest = json.loads(read(subject / "MANIFEST.json"))
    for field, directory in (("files", "posts"), ("QA_only_files", "qa-posts")):
        for name, row in manifest[field].items():
            path = subject / directory / name
            assert sha(read(path)) == row.get("after_sha256", row.get("sha256"))
            add(SOURCE, directory + "/" + name, path)
    # The two earlier diagnostic posts are unchanged; preserve their actual bytes
    # without confusing them with the ordinary Root observation implementation.
    for name in ("internal/core/node/loadstand_route_wire_full_receipt.go",
                 "internal/testutil/loadstand/censusdiag/diagnostic.go"):
        add(SOURCE, "unchanged-private-posts/" + name, WORK["four-build"] / "source" / name)
    for name in ("RESULT.json", "ROOT-DATA.json", "REVIEWER-DATA.json", "SOURCE-ARCHIVE.json",
                 "EXPORTER-REUSE.json", "component.json", "pid1-source.diff", "capexec-source.diff",
                 "guest-sources.tar.gz"):
        add(SOURCE, "prepared-data/" + name, DATA / name)
    data = json.loads(read(DATA / "RESULT.json"))
    for group, row in data["helper_sources"].items():
        for name, wanted in row["source_files"].items():
            path = DATA / group / name
            assert wanted == {"bytes": path.stat().st_size, "sha256": sha(read(path))}
            add(SOURCE, "actual-helper-source/" + group + "/" + name, path)
    for name in ("QA-SOURCE-MANIFEST.json", "ARTIFACT-SOURCE-MANIFEST.json"):
        add(SOURCE, "qa/" + name, WORK["qa"] / name)
    for name in ("source-before.json", "source-after.json"):
        add(SOURCE, "four-build/" + name, WORK["four-build"] / name)
    assert read(WORK["four-build"] / "source-before.json") == read(WORK["four-build"] / "source-after.json")
    after = json.loads(read(WORK["four-build"] / "source-after.json"))
    assert len(after) == 2450 and sha(read(WORK["four-build"] / "source-after.json")) == "29950a43880d803751a6e5fd15b79e4eac785bcce9d08e8935b90d0a1388fb0f"
    qa = json.loads(read(result_path(WORK["qa"])))
    assert qa["error"] is None and qa["artifact_snapshot_files"] == 2450 and qa["QA_snapshot_files"] == 2457
    assert sum(len(x["passed"]) for x in qa["tests"].values()) == 46
    assert sum(x["child_passes"] for x in qa["tests"].values()) == 311
    api = DAY / PACKAGES["four-build"] / "source-api-module"
    for path in sorted(api.rglob("*")):
        if path.is_file():
            add(SOURCE, "original-Go-API-module/" + path.relative_to(api).as_posix(), path)
    add(SOURCE, "pack/reconnect.assembly.json", WORK["pack"] / "reconnect.assembly.json")
    for name in ("corsa-zero-offer-source-data.py", "corsa-zero-offer-runtime-data.py",
                 "corsa-root-zero-offer-stop-audit.py", "corsa-zero-offer-native-data-review.py",
                 "corsa-zero-offer-input-data-review.py", "corsa-zero-offer-cpio-data-review.py"):
        add(SOURCE, "root-and-tester-readbacks/" + name, Path("/private/tmp") / name)
    for origin in (
        DAY / "d1-o1a-guest-preparation-v1/journal_capsule.py",
        DAY.parent / "2026-11-18/d1-o1a-guest-source-inputs/prepare_sources.py",
        DAY / "d1-full512-devnull-fix-v1/package_initrd.py",
        DAY / "d1-o1a-helper-build-v1/common.py",
    ):
        add(SOURCE, "accepted-lineage/" + origin.name, origin)
    decoder_path = DAY / "d1-o1a-guest-preparation-v1/journal_capsule.py"
    spec = importlib.util.spec_from_file_location("portable_zero_capsule", decoder_path)
    decoder = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = decoder
    spec.loader.exec_module(decoder)
    binding = {"workload": "reconnect", "attempt_id": "2c75180d2872646aa2f7201b240cb175",
               "profile_sha256": "f84856cfa6f3c3d39f4f5d69487c5d968106aeea77b16f8a3d7bf864266e7369",
               "source_commit": "4fcc2f7245095fc43d2225f0f0065f4ce3b26113"}
    capsule = decoder.decode(RAW["native/console.raw"], binding)
    assert len(capsule) == 1351
    failed, go_count = [], 0
    for name, raw in capsule.items():
        if name.endswith(".go"):
            assert sha(raw) == after[name.split("/", 2)[2]]
            go_count += 1
        else:
            assert name.endswith("/MANIFEST.sha256") or name.endswith(".failed")
            key = "capsule/" + name
            RAW[key] = raw
            ORIGINS["RAW/" + key] = {"decoded_from": "RAW/native/console.raw",
                                   "bytes": len(raw), "sha256": sha(raw), "binding": binding}
            if name.endswith(".failed"):
                failed.append(name)
    assert go_count == 1348 and len(failed) == 1
    write_json("CAPSULE-MEMBERS.json", {
        n: {"bytes": len(v), "sha256": sha(v)} for n, v in sorted(capsule.items())})
    for label in WORK:
        if label != "native":
            (HERE / (label.upper() + "-RESULT.json")).write_bytes(read(result_path(WORK[label])))
    for name, origin in (
        ("NATIVE-RECEIPT.json", WORK["native"] / "receipt.json"),
        ("STOP-ACK.json", WORK["native"] / "ACK.json"),
        ("ROOT-DATA.json", WORK["native"] / "ROOT-DATA.json"),
        ("TESTER-DATA.json", DAY / "d1-o1a-zero-offer-native-actual-tester-v1/REVIEW.json"),
        ("REVIEWER-DATA.json", WORK["native"] / "REVIEWER-DATA.json"),
    ):
        (HERE / name).write_bytes(read(origin))
    anchors = {
        "ordinary_zero_offer_fix": {"commit": "13e4d4f9195c333ee3307b1d55e387b33aa4c677",
            "evidence": "../d1-zero-offer-accounting-2026-11-20/README.md"},
        "separate_interop_test_lint": {"commit": "2c97bf91f1bc1afca14b4a55a00dbd1639aec1b1"},
        "earlier_disposable_sender_false": {"commit": "cecc9b709e00fbecbbc91c0920f101f7e3b7729b",
            "evidence": "../d1-full-send-sender-2026-11-20/README.md"},
    }
    write_json("SOURCE-CHECKPOINT.json", {
        "anchors": anchors,
        "this_native_subject": "Old actual leaf 2450-file source plus ONE full_send finish post, retaining existing private diagnostics; not current ordinary Root binaries.",
        "ordinary_Root_finish_post": "1cb353cc46fabd04243df40b222db144d1a8d3f43c040af861553e5142d43433",
        "disposable_private_finish_post": "638ba3f314ee362f08c57bf31a428c5b0752fd68870a03344064c86a4caee823",
        "actual_artifact_source_manifest_sha256": sha(read(WORK["four-build"] / "source-after.json")),
        "source_groups": data["source_groups"],
        "QA_only_files": 7, "QA_only_files_excluded_from_ELF_and_guest_sources": True,
        "guest_archive": data["source_archive"], "production_checkout_private_diagnostics_applied": False,
        "linuxhelper_payload_unchanged": True, "new_member_paths": False,
        "actual_CPIO_changed_members": 7, "actual_boot_changed_rows": 5,
    })
    summary = {
        "scope": "DISPOSABLE_ZERO_OFFER_CORRECTED_SHORT_FAILURE_NOT_METRIC_NOT_BASELINE_NO512",
        "capsule_binding": binding,
        "attempt": binding["attempt_id"], "Go": {"outcome": "FAIL", "seconds": 30.62,
            "observer_reason": 31, "transport_code": 95,
            "stage": "observer_full_send_finish_sender_false",
            "source_conjunction": "sentAll=false with p.send!=nil or p.next!=0: some frame was supplied to observation, not proven enqueued or written.",
            "underlying_producer_return_site": "UNKNOWN",
            "failed_journal_record": "Generic failed outcome; no deeper refusal field serialized."},
        "ordinary_unit_fix": "Valid, committed 13e4d4f; does not resolve this actual supplied-frame refusal.",
        "private_QA": {"outer_PASS": 46, "child_PASS": 311, "artifact_files": 2450, "QA_files": 2457},
        "build_scope": "Four static Linux test/stand ELF artifacts and new PID1/capexec. Exporter reused by exact dependency proof. Builds did not execute these ELF artifacts.",
        "native": {"worker_exit": 0, "test_exit": 1, "export_exit": 0,
            "native_events": 4, "stop_complete_ns": 32125790000,
            "all_children_reaped": True, "TERM_or_KILL": False,
            "physical_XPC_resources": "UNKNOWN", "stopped_only_ACK": "COMPLETE"},
        "capsule": {"files": 1351, "Go_files": go_count, "manifests": 2, "failed_records": 1,
            "transport": "RAW.tar.xz/native/console.raw", "selected_source_archive": "SOURCE.tar.xz/prepared-data/guest-sources.tar.gz"},
        "historical97a9_and_480s_inner_causes": "UNKNOWN; not retroactively attributed.",
        "O1a_metric_or_D1_or_activation_or_release_PASS": False,
        "anchors": anchors,
        "raw": archive("RAW", RAW), "source": archive("SOURCE", SOURCE),
    }
    write_json("ORIGINS.json", ORIGINS)
    write_json("SUMMARY.json", summary)
    files = sorted(p for p in HERE.iterdir() if p.is_file() and p.name != "SHA256SUMS")
    (HERE / "SHA256SUMS").write_text("".join(sha(read(p)) + "  " + p.name + "\n" for p in files))
    print(json.dumps({"files": len(files) + 1, "raw": summary["raw"], "source": summary["source"],
                      "sha256sums": sha(read(HERE / "SHA256SUMS"))}, indent=2))


if __name__ == "__main__":
    main()
