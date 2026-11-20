"""Reproduce this bounded local evidence bundle; no builds or application execution."""
from pathlib import Path, PurePosixPath
import hashlib
import io
import json
import tarfile

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
DAY = REPO / "docs/_internal/d1-artifacts/2026-11-20"
WORK = {
    "qa": Path("/private/tmp/corsa-d1-census-diagnostic-goqa-67b5e77b36a348158c4e3c0c046974b5"),
    "build": Path("/private/tmp/corsa-o1a-census-diagnostic-elf-build-1ad46baec0ab43c5b702dad5e8c7ead3"),
    "data": Path("/private/tmp/corsa-d1-o1a-census-component-source-data-d5bbc9a0b3af4df59468aec60ee32487"),
    "go": Path("/private/tmp/corsa-d1-o1a-census-component-go-helpers-248b094982094786a05f30c6ae505f70"),
    "c": Path("/private/tmp/corsa-d1-o1a-census-component-capexec-f4ca49c286e349fda030fc95c0b8ff64"),
    "pack": Path("/private/tmp/corsa-d1-o1a-initrd-data-assembly-c70e760ff5b1490d9abe2564b650b69b"),
    "native": Path("/private/tmp/corsa-d1-o1a-native-0a66a114a62a5cbcbd6dd4a671288ee4"),
    "unit": Path("/private/tmp/corsa-d1-o1a-full-receipt-refill-unit-69df6a2f07774901843a77d1b3e40b74"),
    "guards-partial": Path("/private/tmp/corsa-d1-o1a-full-receipt-source-guards-d5e71e2487904b8bb441a96c25db2740"),
    "guards-clean": Path("/private/tmp/corsa-d1-o1a-full-receipt-clean-findnode-guards-0216a464d806404aa07a2d8cc5acb633"),
}
DATA = Path("/private/tmp/corsa-o1a-census-component-inputs-8eba82dda57843b99ff3999bd4ee91ee")
RAW = {}
SOURCE = {}


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def read(path, limit=16 * 1024**2):
    path = Path(path)
    assert path.is_file() and not path.is_symlink() and path.stat().st_size <= limit
    raw = path.read_bytes()
    assert len(raw) <= limit and not raw.startswith(b"\x7fELF")
    return raw


def add(table, name, path):
    assert name not in table and not PurePosixPath(name).is_absolute() and ".." not in PurePosixPath(name).parts
    table[name] = read(path)


def write_json(name, value):
    (HERE / name).write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")


def result_path(work):
    return work / ("result.json" if (work / "result.json").exists() else "RESULT.json")


def archive(name, files):
    assert len(files) <= 512 and sum(map(len, files.values())) <= 64 * 1024**2
    # USTAR supports these bounded paths without PAX records or extraction.
    output = HERE / (name + ".tar.xz")
    with tarfile.open(output, "w:xz", format=tarfile.USTAR_FORMAT, preset=6) as tar:
        for path, raw in sorted(files.items()):
            member = tarfile.TarInfo(path)
            member.size = len(raw)
            member.mode = 0o644
            member.uid = member.gid = member.mtime = 0
            member.uname = member.gname = ""
            tar.addfile(member, io.BytesIO(raw))
    inventory = {path: {"bytes": len(raw), "sha256": sha(raw)} for path, raw in sorted(files.items())}
    write_json(name + "-MEMBERS.json", inventory)
    seen = set()
    with tarfile.open(output, "r:xz") as tar:
        for member in tar:
            assert member.isfile() and not member.pax_headers and member.name not in seen
            assert member.uid == member.gid == member.mtime == 0 and member.mode == 0o644
            assert tar.extractfile(member).read() == files[member.name]
            seen.add(member.name)
    assert seen == files.keys()
    return {"members": len(files), "payload_bytes": sum(map(len, files.values())), "sha256": sha(read(output))}


def main():
    # Complete command streams and their persisted metadata, including nested
    # graph streams in the four-artifact builder. Never walk caches or binaries.
    for label, work in WORK.items():
        if label == "native":
            names = ("console.raw", "native.jsonl", "stderr.raw", "controller-result.json", "receipt.json", "expected.json", "worker-start.json", "lock-binding.json", "ROOT-REVIEW.json", "REVIEWER-DATA.json", "REVIEWS.json", "ACK.json")
            for name in names:
                add(RAW, label + "/" + name, work / name)
            continue
        result = json.loads(read(result_path(work)))
        if label == "build":
            stream_paths = sorted((work / "raw").glob("*"))
            assert len(stream_paths) == 48
        else:
            stream_paths = []
            for step in result["steps"]:
                stream_paths.extend(Path(step[key]) for key in ("stdout", "stderr"))
            stream_paths.extend(sorted(work.glob("[0-9][0-9]-*.json")))
        for path in stream_paths:
            add(RAW, label + "/" + str(path.relative_to(work)), path)
        add(RAW, label + "/result.json", result_path(work))
        for name in ("ROOT-DATA.json", "REVIEWER-DATA.json", "ROOT-PACK-DATA.json", "REVIEWER-RUNTIME-DATA.json", "POLICY-DATA.json"):
            if (work / name).exists():
                add(RAW, label + "/" + name, work / name)
    failed = list((WORK["native"] / "decoded-journal").glob("*.failed"))
    assert len(failed) == 1
    add(RAW, "journal/" + failed[0].name, failed[0])

    packages = {
        "diagnostic-source": "d1-o1a-census-stage-diagnostic-v1",
        "diagnostic-qa": "d1-o1a-census-diagnostic-goqa-v1",
        "four-elf-build": "d1-o1a-census-diagnostic-elf-build-v1",
        "component-profile": "d1-o1a-census-component-profile-v1",
        "input-producer": "d1-o1a-census-component-input-rebind-v1",
        "go-helper-controller": "d1-o1a-census-component-go-helper-build-v2",
        "c-helper-controller": "d1-o1a-census-component-capexec-build-v1",
        "initrd-controller": "d1-o1a-census-component-initrd-v1",
        "runtime-binder": "d1-o1a-census-component-rebind-v1",
        "runtime": "d1-o1a-census-component-runtime-v1",
        "refill-unit": "d1-o1a-full-receipt-refill-unit-v1",
        "refill-unit-formatted": "d1-o1a-full-receipt-refill-unit-v2",
    }
    # Fixed manifest-driven posts; top-level source, exact binding and profile
    # records only. No recursive archive of ignored artifact trees.
    extensions = {".py", ".json", ".md", ".diff", ".sh", ".txt", ".manifest"}
    diagnostic_names = set(json.loads(read(DAY / packages["diagnostic-source"] / "MANIFEST.json"))["files"])
    for label, package in packages.items():
        folder = DAY / package
        for path in sorted(folder.iterdir()):
            if path.is_file() and (path.suffix in extensions or path.name == "SHA256SUMS"):
                add(SOURCE, label + "/" + path.name, path)
        manifest = folder / "MANIFEST.json"
        if manifest.exists() and (folder / "posts").is_dir():
            for name in json.loads(read(manifest))["files"]:
                if label == "diagnostic-qa" and name in diagnostic_names:
                    continue  # Same nine bytes are retained under diagnostic-source.
                add(SOURCE, label + "/posts/" + name, folder / "posts" / name)
    # The new three finalizer posts are already in the accepted baseline
    # bundle, but the exact twelve-file combined compiler subject is retained.
    for kind, package in (("helper-go", "d1-o1a-census-component-go-helper-build-v2"), ("helper-c", "d1-o1a-census-component-capexec-build-v1")):
        folder = DAY / package
        frozen = json.loads(read(folder / "FROZEN.json"))
        for name in frozen["source_files"]:
            add(SOURCE, kind + "/" + name, folder / "source" / name)
        add(SOURCE, kind + "/SHA256SUMS", folder / "source/SHA256SUMS")
    for label, work in (("tested", WORK["qa"]), ("compiled", WORK["build"])):
        names = ("SOURCE-MANIFEST.json", "INPUTS.json") if label == "tested" else ("source-before.json", "source-after.json")
        for name in names:
            add(SOURCE, label + "/" + name, work / name)
    for name in ("RESULT.json", "SOURCE-ARCHIVE.json", "pid1-source.diff", "capexec-source.diff", "component.json"):
        add(SOURCE, "prepared-inputs/" + name, DATA / name)
    for name in ("SOURCE-BEFORE.json", "SOURCE-AFTER.json", "overlay.json"):
        add(SOURCE, "refill-unit-actual/" + name, WORK["unit"] / name)
    add(RAW, "unit/TESTER-DATA.json", DAY / "d1-o1a-full-receipt-refill-unit-actual-tester-v1/REVIEW.json")
    for label, names in {
        "guards-partial": ("SOURCE-BEFORE.json", "overlay.json"),
        "guards-clean": ("SOURCE-MANIFEST.json", "GO-SOURCE-AFTER.json"),
    }.items():
        for name in names:
            add(SOURCE, label + "/" + name, WORK[label] / name)
    for name in ("corsa-full-receipt-final-source-guards.py", "corsa-full-receipt-clean-findnode-guards.py"):
        add(SOURCE, "guard-controllers/" + name, Path("/private/tmp") / name)
    add(SOURCE, "policy/factory.bpf", WORK["c"] / "output/policy.bpf")
    for name, path in {
        "capture_worker.py": DAY / "d1-o1a-host-capture-v1/capture_worker.py",
        "journal_capsule.py": DAY / "d1-o1a-guest-preparation-v1/journal_capsule.py",
    }.items():
        add(SOURCE, "runtime-dependencies/" + name, path)
    add(SOURCE, "evidence/prepare_evidence.py", Path(__file__))

    # Small primary receipts remain directly readable; full prior stages stay
    # inside archives rather than duplicating every metadata object.
    readable = {
        "QA-RESULT.json": result_path(WORK["qa"]),
        "FOUR-ELF-RESULT.json": result_path(WORK["build"]),
        "INPUT-RESULT.json": DATA / "RESULT.json",
        "GO-HELPER-RESULT.json": result_path(WORK["go"]),
        "C-HELPER-RESULT.json": result_path(WORK["c"]),
        "PACK-RESULT.json": result_path(WORK["pack"]),
        "NATIVE-EXPECTED.json": WORK["native"] / "expected.json",
        "NATIVE-RECEIPT.json": WORK["native"] / "receipt.json",
        "STOP-REVIEWS.json": WORK["native"] / "REVIEWS.json",
        "STOP-ACK.json": WORK["native"] / "ACK.json",
        "REFILL-UNIT-RESULT.json": result_path(WORK["unit"]),
        "REFILL-UNIT-REVIEWER.json": WORK["unit"] / "REVIEWER-DATA.json",
        "SOURCE-GUARDS-PARTIAL-RESULT.json": result_path(WORK["guards-partial"]),
        "FINDNODE-GUARDS-CLEAN-RESULT.json": result_path(WORK["guards-clean"]),
        "FINAL-GUARDS-REVIEWER.json": WORK["guards-clean"] / "REVIEWER-DATA.json",
    }
    for name, path in readable.items():
        (HERE / name).write_bytes(read(path))
    qa = json.loads(read(result_path(WORK["qa"])))
    assert sum(len(step.get("tests", {}).get("passed", [])) for step in qa["steps"]) == 21
    assert sum(step.get("tests", {}).get("child_passes", 0) for step in qa["steps"]) == 98
    expected = json.loads(read(WORK["native"] / "expected.json"))
    receipt = json.loads(read(WORK["native"] / "receipt.json"))
    assert expected["purpose"] == "COMPONENT_CENSUS_DIAGNOSTIC" and receipt["worker_exit"] == 0
    unit = json.loads(read(result_path(WORK["unit"])))
    assert unit["status"] == "REFILL_UNIT_MECHANISM_PASS" and unit["tests"] == {"outer": 4, "child": 11}
    unit_before = json.loads(read(WORK["unit"] / "SOURCE-BEFORE.json"))
    assert unit_before == json.loads(read(WORK["unit"] / "SOURCE-AFTER.json")) and len(unit_before) == 2102
    partial = json.loads(read(result_path(WORK["guards-partial"])))
    clean = json.loads(read(result_path(WORK["guards-clean"])))
    assert partial["status"] == "FAILED" and partial["tests"] == {"outer": 20, "child": 105}
    assert clean["status"] == "CLEAN_FINDNODE_SOURCE_GUARDS_PASS" and clean["tests"] == {"outer": 5, "child": 2}
    final_go = json.loads(read(WORK["guards-clean"] / "GO-SOURCE-AFTER.json"))
    assert final_go == json.loads(read(WORK["guards-partial"] / "SOURCE-BEFORE.json")) and len(final_go) == 2103
    summary = {
        "scope": "COMPONENT_CENSUS_DIAGNOSTIC_ONLY_NO_METRIC_ADMISSION",
        "attempt_id": expected["attempt_id"],
        "status": "ORIGINAL_GO_FAIL_30_65_SECONDS_STAGE_FULL_RECEIPT",
        "first_specific_stage": "full_receipt",
        "exact_inner_predicate": "UNKNOWN",
        "historical_480_second_failure_inner_cause": "UNKNOWN_NOT_PROVEN_SAME",
        "O1a_rates": "UNKNOWN_NOT_ACCEPTED",
        "VM": "NORMAL_STOPPED_WORKER_EXIT0_ALL_GUEST_CHILDREN_REAPED",
        "physical_XPC_resources": "UNKNOWN",
        "identical_retry": False,
        "original_periodic_or_reconnect_repeated": False,
        "source_QA": {"outer_PASS": 21, "child_PASS": 98, "scope": "targeted source tests and compile only"},
        "four_ELF_source": "4fcc2f7245095fc43d2225f0f0065f4ce3b26113 plus three finalizer and nine census diagnostic posts",
        "source_groups": json.loads(read(DATA / "RESULT.json"))["source_groups"],
        "component_profile": {"purpose": expected["purpose"], "transport_workload": "reconnect", "startup_lead_seconds": 10, "duration_seconds": 20, "begin_seconds": 2, "disconnect_seconds": 6, "end_seconds": 15, "Go_timeout_seconds": 90, "C_timeout_seconds": 120},
        "capsule": {"files": 1351, "source_files": 1348, "manifests": 2, "failed_records": 1, "failed_record_bytes": 2906, "failed_record_body_bytes": 0},
        "captures": receipt["captures"],
        "accepted_only": ["source QA", "compile metadata", "data assembly", "normal VM stop and exact-own-lock ACK"],
        "not_accepted": ["O1a metrics", "standalone 512MB", "D1", "G2", "public activation", "release"],
        "refill_unit": {
            "status": "MECHANISM_ONLY_NOT_SAVED_RUNTIME_CAUSE_PROOF",
            "outer_PASS": 4,
            "child_PASS": 11,
            "single_test_source_sha256": unit["overlay_sha256"],
            "source_snapshot_records": 2102,
            "source_snapshot_scope": "2100 tracked Go files plus go.mod/go.sum; not a whole-tree 2642-file snapshot",
            "relevant_files_byte_equal_actual_diagnostic_subject": 35,
            "contrast": {"16s": {"wire_frames": 100, "wire_claims": 10000, "accepted": 3200, "strict_full_receipt": False}, "50s": {"wire_frames": 100, "wire_claims": 10000, "accepted": 10000, "strict_full_receipt": True}},
            "controls": "original handler/identity-keyed limiter/withdrawal; valid newer replacement sequence; complete observer; manual clock; no limiter reset or oracle relaxation",
            "product_change": False,
            "VM_or_ServiceRun": False,
            "old_30s_or_480s_inner_cause": "UNKNOWN_NOT_PROVEN_BY_THIS_UNIT",
        },
        "formatted_regression_final_guards": {
            "formatted_test_sha256": clean["new_test_sha256"],
            "original_43d_unit_result_preserved": True,
            "format_proof": "v2 FORMAT.json: gofmt(original43d) equals formatted3d619; no production changes",
            "partial_runner_status": "FAILED_PRESERVED",
            "partial_node_tests": {"outer_PASS": 20, "child_PASS": 105},
            "partial_findnode_failure": "TestCandidateInactiveCallGraphAndInventories sees ignored docs/_internal production copies; original scanner unchanged",
            "partial_RAW_bytes": partial["raw_bytes"],
            "isolated_findnode_status": "PASS_IN_SEPARATE_LATER_INVOCATION",
            "isolated_findnode_tests": {"outer_PASS": 5, "child_PASS": 2},
            "isolated_RAW_bytes": clean["raw_bytes"],
            "isolated_tracked_source_files_plus_new_test": 2659,
            "partial_before_equals_clean_after_Go_source_records": 2103,
            "Go_snapshot_scope": "2100 previously tracked Go files plus formatted new test and go.mod/go.sum",
            "not_one_combined_green_run": True,
        },
        "archives": {"RAW": archive("RAW", RAW), "SOURCE": archive("SOURCE", SOURCE)},
    }
    write_json("SUMMARY.json", summary)
    # Docs and README are authored separately and included in this checksum.
    paths = sorted(path for path in HERE.iterdir() if path.is_file() and path.name != "SHA256SUMS")
    (HERE / "SHA256SUMS").write_text("".join(sha(read(path)) + "  " + path.name + "\n" for path in paths))
    print(json.dumps({"files": len(paths) + 1, "bytes": sum(path.stat().st_size for path in HERE.iterdir() if path.is_file()), "manifest_sha256": sha(read(HERE / "SHA256SUMS")), "archives": summary["archives"]}, sort_keys=True))


if __name__ == "__main__":
    main()
