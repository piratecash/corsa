"""Fixed local evidence packer; no tests, compilers, applications or VM calls.

The observer chain failed. A separate deterministic fixture regression passed;
its result does not replace the failed VM or establish its exact kernel timing.
"""
from pathlib import Path, PurePosixPath
import hashlib
import io
import json
import tarfile

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
DAY = REPO / "docs/_internal/d1-artifacts/2026-11-20"
WORK = {
    "qa-failed": Path("/private/tmp/corsa-d1-o1a-observer-first-failure-goqa-01c7ed4e7ca545db9632580f8bfecd4e"),
    "qa-passed": Path("/private/tmp/corsa-d1-o1a-observer-first-failure-goqa-d2b17d5e44a04f03bfd954bd9aad7e74"),
    "four-build": Path("/private/tmp/corsa-o1a-observer-first-failure-elf-build-3e7567a7e3df45319f3abf139a6bdc4f"),
    "source-data": Path("/private/tmp/corsa-d1-o1a-observer-first-failure-source-data-daaa826a353c4c8a99109b268e857473"),
    "pid1-build": Path("/private/tmp/corsa-d1-o1a-observer-first-failure-pid1-6832d573cb304d07a411fcaa5e474c41"),
    "capexec-build": Path("/private/tmp/corsa-d1-o1a-observer-first-failure-capexec-ad1fc6b857e04ce68cc0519102511f5e"),
    "pack": Path("/private/tmp/corsa-d1-o1a-initrd-data-assembly-37a04197d0da40f79f2d8cb2a7eb06d8"),
    "binding": Path("/private/tmp/corsa-d1-o1a-observer-first-failure-runtime-data-957bbe9c21ce4509a3dbc538a648c927"),
    "native": Path("/private/tmp/corsa-d1-o1a-native-9b75ce742acfaaa725eb667c3521e520"),
    "fixture-build": Path("/private/tmp/corsa-d1-o1a-namespace-coherent-snapshot-test-build-a8cc7165e43f4030ac1b4ee72602ff11"),
    "fixture-unit": Path("/private/tmp/corsa-d1-o1a-namespace-coherent-snapshot-unit-4f0f6314cfab4b4093902ab8bb0f5213"),
    "fixture-lint": Path("/private/tmp/corsa-d1-o1a-namespace-coherent-snapshot-linux-lint-e194074c63784c57adf4bd6d2ff6150f"),
}
DATA = Path("/private/tmp/corsa-o1a-observer-inputs-736d02cb6b2a40dcb8ca947b3384e13e")
PACKAGES = {
    "subject-v1": "d1-o1a-observer-first-failure-source-v1",
    "subject-v2": "d1-o1a-observer-first-failure-source-v2",
    "qa-v1": "d1-o1a-observer-first-failure-goqa-v1",
    "qa-v2": "d1-o1a-observer-first-failure-goqa-v2",
    "four-build": "d1-o1a-observer-first-failure-elf-build-v1",
    "source-producer": "d1-o1a-observer-first-failure-input-rebind-v1",
    "pid1-build": "d1-o1a-observer-first-failure-pid1-build-v1",
    "capexec-build": "d1-o1a-observer-first-failure-capexec-build-v1",
    "pack": "d1-o1a-observer-first-failure-initrd-v1",
    "binding": "d1-o1a-observer-first-failure-rebind-v1",
    "runtime": "d1-o1a-observer-first-failure-runtime-v1",
    "fixture-source": "d1-o1a-initial-route-snapshot-source-v1",
    "fixture-build": "d1-namespace-coherent-snapshot-qa-v1",
    "fixture-unit": "d1-namespace-coherent-snapshot-unit-v1",
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
    for label, work in WORK.items():
        if label == "native":
            for name in ("console.raw", "native.jsonl", "stderr.raw", "controller-result.json", "receipt.json", "expected.json", "worker-start.json", "lock-binding.json", "ROOT-DATA.json", "TESTER-DATA.json", "REVIEWER-DATA.json", "REVIEWS.json", "ACK.json"):
                add(RAW, label + "/" + name, work / name)
            continue
        result = json.loads(read(result_path(work)))
        if label == "four-build":
            streams = sorted((work / "raw").iterdir())
            assert len(streams) == 51  # 17 complete stdout/stderr/process records.
        else:
            streams = [Path(step[k]) for step in result["steps"] for k in ("stdout", "stderr")]
            streams += sorted(work.glob("[0-9][0-9]-*.json"))
        for path in streams:
            add(RAW, label + "/" + path.relative_to(work).as_posix(), path)
        add(RAW, label + "/RESULT.json", result_path(work))
        for name in ("ROOT-DATA.json", "REVIEWER-DATA.json", "POLICY-DATA.json", "ROOT-PACK-DATA.json"):
            if (work / name).exists():
                add(RAW, label + "/" + name, work / name)
    for name in ("RESULT.json", "REVIEWER-DATA.json", "SOURCE-ARCHIVE.json", "EXPORTER-REUSE.json", "pid1-source.diff", "capexec-source.diff"):
        add(SOURCE, "prepared-data/" + name, DATA / name)
    # Keep all new observer posts once. The v1/v2 production posts are identical;
    # only the selected-probe QA stimulus differs and both QA versions survive.
    subject = DAY / PACKAGES["subject-v2"]
    manifest = json.loads(read(subject / "MANIFEST.json"))
    for name in manifest["files"]:
        add(SOURCE, "observer-posts/" + name, subject / "posts" / name)
    for label in ("subject-v1", "subject-v2"):
        folder = DAY / PACKAGES[label]
        rows = json.loads(read(folder / "MANIFEST.json"))["QA_only_files"]
        for name in rows:
            add(SOURCE, label + "/qa-posts/" + name, folder / "qa-posts" / name)
    fixture = DAY / PACKAGES["fixture-source"]
    for name, row in json.loads(read(fixture / "MANIFEST.json"))["files"].items():
        path = fixture / "posts" / name
        assert sha(read(path)) == row["after_sha256"]
        add(SOURCE, "fixture-posts/" + name, path)
    for label, package in PACKAGES.items():
        folder = DAY / package
        for path in sorted(folder.iterdir()):
            if path.is_file() and (path.suffix in {".py", ".json", ".md", ".diff", ".sh", ".manifest"} or path.name == "SHA256SUMS"):
                add(SOURCE, label + "/" + path.name, path)
    build = DAY / PACKAGES["four-build"]
    add(SOURCE, "source-api-adapter/main.go", build / "source-api-module/cmd/source-api-readback/main.go")
    original_api = {p.relative_to(build / "source-api-module").as_posix(): sha(read(p)) for p in sorted((build / "source-api-module").rglob("*.go")) if "cmd" not in p.relative_to(build / "source-api-module").parts}
    SOURCE["source-api-adapter/ORIGINAL-API-PINS.json"] = (json.dumps(original_api, indent=2, sort_keys=True) + "\n").encode()
    for label in ("qa-failed", "qa-passed"):
        for name in ("ARTIFACT-SOURCE-MANIFEST.json", "QA-SOURCE-MANIFEST.json"):
            add(SOURCE, label + "/" + name, WORK[label] / name)
    for name in ("source-before.json", "source-after.json"):
        add(SOURCE, "four-build/" + name, WORK["four-build"] / name)
    add(SOURCE, "fixture-build/SOURCE-MANIFEST.json", WORK["fixture-build"] / "SOURCE-MANIFEST.json")
    for name in ("reconnect.assembly.json",):
        add(SOURCE, "pack/" + name, WORK["pack"] / name)
    for name in ("corsa-observer-first-failure-data.py", "corsa-observer-first-failure-runtime-data.py", "corsa-coherent-snapshot-linux-lint.py"):
        add(SOURCE, "root-wrappers/" + name, Path("/private/tmp") / name)
    summary = {
        "scope": "DISPOSABLE_OBSERVER_DIAGNOSTIC_EARLY_SETUP_FAILURE_NOT_BASELINE_NOT_METRIC_NO512",
        "prior_committed_reference": {"commit": "a6a9fdc90d02134f5995df412a304f9e49201aa5", "evidence": "docs/operations/evidence/d1-census-diagnostic-2026-11-20", "reason": "Original 4fcc+3finalizer+9census source/helper/controller lineage retained there; unchanged base trees and old binaries are not duplicated."},
        "production_worktree_modified_by_observer": False,
        "changed_private_Core": {"files": 640, "sha256": "9fb7bba6dd77a128a634f0d6d26e67485b07f1178b9361166a754e0fb7d1268e"},
        "artifact_stand": {"files": 708, "sha256": "982764418fc1cf30f59ff1b9bb71399763a91295bfd0c30b941e48f6cf1813b2"},
        "QA_failed_v1": "14 outer PASS; selected-probe QA stimulus used unselected route_probe; legacy groups NOT_STARTED",
        "QA_passed_v2": {"outer": 36, "child": 147, "production_delta_from_v1": 0},
        "four_ELFs": "Compile-only, 17 normal commands including fresh original Source API; 2450 artifact paths exclude two QA-only files",
        "exporter": "Exact 7ef3 binary reused by unchanged compiled dependency closure; not rebuilt",
        "native": {"attempt": "9b75ce742acfaaa725eb667c3521e520", "Go": "FAIL0.13s before journal", "stage": "setup-apply full-000 plannedInNamespace guard4 ff00::/8 configure=true", "observer_first_failure_family": "NOT_REACHED", "test_exit": 1, "worker_exit": 0, "physical_XPC": "UNKNOWN", "stopped_only_ACK": "COMPLETE; functional failure unchanged"},
        "old_30s_and_480s_observer_inner_cause": "UNKNOWN",
        "fixture_repair": {"subject": "e4b51cc plus three fixture production files and one new pure test; Core/App unchanged", "source_manifest_sha256": sha(read(fixture / "MANIFEST.json")), "compile_source_files": 2704, "unit_inputs_including_test_ELF": 2705, "Linux_unit_outer": 17, "Linux_unit_child": 84, "unit_result_sha256": sha(read(result_path(WORK["fixture-unit"]))), "lint_status": json.loads(read(result_path(WORK["fixture-lint"])))["status"], "actual_fixed_VM": "NOT_RUN", "exact_kernel_event_cause": "NOT_PROVEN"},
        "original_periodic_reconnect_metric_acceptance": "NOT_ESTABLISHED",
        "raw": archive("RAW", RAW), "source": archive("SOURCE", SOURCE),
    }
    write_json("SUMMARY.json", summary)
    for label in ("qa-failed", "qa-passed", "four-build", "pid1-build", "capexec-build", "pack", "binding", "fixture-build", "fixture-unit", "fixture-lint"):
        (HERE / (label.upper() + "-RESULT.json")).write_bytes(read(result_path(WORK[label])))
    (HERE / "STOP-ACK.json").write_bytes(read(WORK["native"] / "ACK.json"))
    (HERE / "STOP-REVIEWER.json").write_bytes(read(WORK["native"] / "REVIEWER-DATA.json"))
    files = sorted(p for p in HERE.iterdir() if p.is_file() and p.name != "SHA256SUMS")
    (HERE / "SHA256SUMS").write_text("".join(sha(read(p)) + "  " + p.name + "\n" for p in files))
    print(json.dumps({"files": len(files) + 1, "raw": summary["raw"], "source": summary["source"], "sha256sums": sha(read(HERE / "SHA256SUMS"))}))


if __name__ == "__main__":
    main()
