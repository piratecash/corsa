"""Fixed zero-offer accounting evidence snapshot; data only, no Go/app/VM calls."""
from pathlib import Path, PurePosixPath
import difflib
import hashlib
import io
import json
import subprocess
import tarfile

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
DAY = REPO / "docs/_internal/d1-artifacts/2026-11-20"
RAW, SOURCE, ORIGINS = {}, {}, {}
WORKS = {
    "original": Path("/private/tmp/corsa-d1-o1a-sender-refusal-unit-c51403d5f58347e39c104d5851c604f1"),
    "fixed": Path("/private/tmp/corsa-d1-o1a-zero-offer-fix-unit-6875c1480f4f4fc6b8fb5937b6494564"),
    "linux-compile": Path("/private/tmp/corsa-d1-o1a-zero-offer-linux-compile-dc7706294f834840b5d1a43f63d642dc"),
}
BASE = "22aaabc2f8035b8d287ce4181a997d1725efa722"
LINT_FIXED = Path("/private/tmp/corsa-d1-o1a-zero-offer-linux-lint-fixed-05ae1e457b4f4309bbfb274382c81821")
LINT_FAILED = Path("/private/tmp/corsa-d1-o1a-zero-offer-linux-lint-5fabecfd98794fc7b4d8213b08236e06")


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def read(path):
    path = Path(path)
    assert path.is_file() and not path.is_symlink() and path.stat().st_size <= 16 * 1024**2
    raw = path.read_bytes()
    assert len(raw) <= 16 * 1024**2 and not raw.startswith(b"\x7fELF")
    return raw


def add(table, name, path):
    p = PurePosixPath(name)
    assert name not in table and name == p.as_posix() and not p.is_absolute() and ".." not in p.parts
    raw = read(path)
    table[name] = raw
    ORIGINS[("RAW/" if table is RAW else "SOURCE/") + name] = {
        "path": str(Path(path).resolve()), "bytes": len(raw), "sha256": sha(raw)}


def write_json(name, value):
    (HERE / name).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def archive(name, files):
    assert len(files) <= 256 and sum(map(len, files.values())) <= 16 * 1024**2
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
            assert member.mode == 0o644 and member.uid == member.gid == member.mtime == 0
            assert inp.extractfile(member).read() == files[member.name]
            seen.add(member.name)
    assert seen == files.keys()
    return {"sha256": sha(read(path)), "members": len(files), "payload_bytes": sum(map(len, files.values()))}


def main():
    package = DAY / "d1-o1a-zero-offer-fix-v1"
    manifest = json.loads(read(package / "MANIFEST.json"))
    assert sha(read(package / "MANIFEST.json")) == "2ede9161978aabd3739a8948edf515aeead0754851a48e5350b99552abf78883"
    results = {}
    for label, work in WORKS.items():
        result = json.loads(read(work / "RESULT.json"))
        assert result["error"] is None and not result["unknown"] and not result["lock_retained"]
        results[label] = result
        (HERE / (label.upper() + "-RESULT.json")).write_bytes(read(work / "RESULT.json"))
        paths = {Path(step[k]) for step in result["steps"] for k in ("stdout", "stderr")}
        paths.update(work.glob("[0-9][0-9]-*.json"))
        for path in sorted(paths):
            add(RAW, label + "/" + path.name, path)
        for name in ("SOURCE-BEFORE.json", "SOURCE-AFTER.json", "overlay.json"):
            if (work / name).exists():
                add(SOURCE, label + "/" + name, work / name)
    failed_lint = json.loads(read(LINT_FAILED / "RESULT.json"))
    assert failed_lint["status"] == "FAILED" and not failed_lint["unknown"] and not failed_lint["lock_retained"]
    (HERE / "LINT-ORIGINAL-FAILED.json").write_bytes(read(LINT_FAILED / "RESULT.json"))
    for path in sorted(LINT_FAILED.glob("01-linux-lint.*")):
        add(RAW, "lint-original-failed/" + path.name, path)
    add(SOURCE, "controllers/linux-lint-original.py", "/private/tmp/corsa-zero-offer-linux-lint.py")
    (HERE / "REVIEWER-LINT-FAILURE.json").write_bytes(read(LINT_FAILED / "REVIEWER-DATA.json"))
    for path in sorted((DAY / "d1-hmix-interop-lint-fix-v1").rglob("*")):
        if path.is_file() and path.suffix in {".go", ".json", ".md", ".diff", ".txt"}:
            add(SOURCE, "later-interop-lint-fix/" + path.relative_to(DAY / "d1-hmix-interop-lint-fix-v1").as_posix(), path)
    fixed_lint = json.loads(read(LINT_FIXED / "RESULT.json"))
    assert fixed_lint["status"] == "LINUX_FIXTURE_LINT_PASS" and not fixed_lint["unknown"] and not fixed_lint["lock_retained"]
    (HERE / "LINT-FIXED-RESULT.json").write_bytes(read(LINT_FIXED / "RESULT.json"))
    (HERE / "REVIEWER-LINT-DATA.json").write_bytes(read(LINT_FIXED / "REVIEWER-DATA.json"))
    for path in sorted(LINT_FIXED.glob("01-linux-lint.*")):
        add(RAW, "lint-fixed/" + path.name, path)
    for name in ("SOURCE-BEFORE.json", "SOURCE-AFTER.json"):
        add(SOURCE, "lint-fixed/" + name, LINT_FIXED / name)
    add(SOURCE, "controllers/linux-lint-fixed.py", "/private/tmp/corsa-zero-offer-linux-lint-fixed.py")
    (HERE / "TESTER-LINT-DATA.json").write_bytes(read(DAY / "d1-o1a-zero-offer-actual-tester-v1/LINT-REVIEW.json"))
    for label, path in {
        "original.py": "/private/tmp/corsa-sender-refusal-unit.py",
        "fixed.py": "/private/tmp/corsa-zero-offer-fix-unit.py",
        "linux-compile.py": "/private/tmp/corsa-zero-offer-linux-compile.py",
        "tester-readback.py": "/private/tmp/corsa-zero-offer-data-readback.py",
    }.items():
        add(SOURCE, "controllers/" + label, path)
    add(SOURCE, "controllers/common.py", DAY / "d1-o1a-capexec-seccomp-build-v1/common.py")
    for label, package_name in {
        "original": "d1-o1a-sender-refusal-unit-v1",
        "fixed": "d1-o1a-zero-offer-fix-v1",
    }.items():
        folder = DAY / package_name
        add(SOURCE, label + "/MANIFEST.json", folder / "MANIFEST.json")
        for path in sorted((folder / "posts").rglob("*.go")):
            add(SOURCE, label + "/posts/" + path.relative_to(folder / "posts").as_posix(), path)
    for label, origin in {
        "TESTER-DATA.json": DAY / "d1-o1a-zero-offer-actual-tester-v1/REVIEW.json",
        "ARCHITECT-CODE.json": DAY / "d1-o1a-zero-offer-fix-architect-review-v1/REVIEW.json",
        "ARCHITECT-DATA.json": DAY / "d1-o1a-zero-offer-fix-architect-data-v1/REVIEW.json",
        "REVIEWER-UNIT-DATA.json": WORKS["fixed"] / "REVIEWER-DATA.json",
    }.items():
        (HERE / label).write_bytes(read(origin))
    source_a = json.loads(read(WORKS["original"] / "SOURCE-BEFORE.json"))
    source_b = json.loads(read(WORKS["fixed"] / "SOURCE-BEFORE.json"))
    source_c = json.loads(read(WORKS["linux-compile"] / "SOURCE-BEFORE.json"))
    assert source_a == source_b
    changes = {n: {"before": source_a.get(n), "after": source_c.get(n)}
               for n in sorted(source_a.keys() | source_c.keys()) if source_a.get(n) != source_c.get(n)}
    assert changes == {n: {"before": row["before_sha256"], "after": row["after_sha256"]}
                       for n, row in manifest["files"].items()}
    assert all(sha(read(package / "posts" / n)) == row["after_sha256"]
               for n, row in manifest["files"].items())
    selected = [
        "internal/core/node/routing_announce.go",
        "internal/core/node/peer_management.go",
        "internal/core/node/loadstand_route_wire.go",
        "internal/core/node/loadstand_route_wire_full_receipt.go",
        "internal/core/node/loadstand_route_wire_full_send_test.go",
        "internal/core/node/loadstand_route_wire_full_send_mechanism_test.go",
        "internal/core/node/loadstand_executor_route_wire_pairs_test.go",
        "go.mod", "go.sum",
    ]
    for name in selected:
        assert sha(read(REPO / name)) == source_a[name]
        add(SOURCE, "unchanged/" + name, REPO / name)
    for name, row in manifest["files"].items():
        after = read(package / "posts" / name)
        assert sha(after) == row["after_sha256"]
        before = subprocess.check_output(["/usr/bin/git", "-C", str(REPO), "show", BASE + ":" + name]) if row["before_sha256"] else b""
        if before:
            assert sha(before) == row["before_sha256"]
            SOURCE["before/" + name] = before
            ORIGINS["SOURCE/before/" + name] = {"git_commit": BASE, "git_path": name, "bytes": len(before), "sha256": sha(before)}
        diff = "".join(difflib.unified_diff(before.decode().splitlines(True), after.decode().splitlines(True),
                    fromfile="a/" + name, tofile="b/" + name)).encode()
        SOURCE["patches/" + Path(name).name + ".diff"] = diff
        ORIGINS["SOURCE/patches/" + Path(name).name + ".diff"] = {
            "derived_from_before_after": name, "bytes": len(diff), "sha256": sha(diff)}
    lint_source = json.loads(read(LINT_FIXED / "SOURCE-BEFORE.json"))
    assert lint_source == json.loads(read(LINT_FIXED / "SOURCE-AFTER.json"))
    interop_manifest = json.loads(read(DAY / "d1-hmix-interop-lint-fix-v1/MANIFEST.json"))
    lint_delta = {n: {"before": source_c.get(n), "after": lint_source.get(n)}
                  for n in sorted(source_c.keys() | lint_source.keys()) if source_c.get(n) != lint_source.get(n)}
    assert lint_delta == {n: {"before": v["before_sha256"], "after": v["after_sha256"]}
                          for n, v in interop_manifest["files"].items()}
    write_json("SOURCE-CHECKPOINT.json", {
        "manifest_declared_base": manifest["base_commit"],
        "all_2107_unit_source_module_payloads_also_exact_git_commit": BASE,
        "proof": "TESTER-DATA checked every git blob, not inferred from stale controller base label.",
        "unit_virtual_overlay": "Original one test post; fixed three exact posts, each separately hashed.",
        "final_compile_subject": BASE + " plus exact three posts",
        "compile_source_module_records": len(source_c), "changed_records": changes,
        "new_product_state_counters_clocks_schema_or_transport_policy": False,
        "production_changed_files": manifest["production_changes"],
        "later_lint_subject": "same compile subject plus two separately reviewed interop-test fixes",
        "later_lint_delta": lint_delta,
        "later_lint_source_records_before_after_equal": len(lint_source),
    })
    write_json("SUMMARY.json", {
        "scope": "ZERO_SUPPLIED_FRAME_ACCOUNTING_ONLY_NOT_NATIVE_CAUSE_OR_O1A_METRIC_PASS",
        "original_unit": {"outer_PASS": 3, "child_PASS": 34, "RAW_bytes": 37809},
        "fixed_unit": {"outer_PASS": 8, "child_PASS": 62, "RAW_bytes": 71889},
        "linux_compile": {"commands_exit0": 4, "RAW_bytes": 5287, "ELF_execution": False,
                          "artifacts": results["linux-compile"]["artifacts"]},
        "genuine_test_scope": "Original sender/chunker/queue and manual clock; writer accounting callbacks, not real network/native I/O.",
        "positive": "Missing session and missing V3 supply no frame, create no full pass, and no longer poison an otherwise valid observation; later real 101-destination/two-frame queued pass retains exact writer proof.",
        "negative": "Disconnected/full queue, any supplied or partial frame, prior poison, bounds/clock/bad metadata/frame/empty success remain failures.",
        "original_native_97a9_cause": "UNKNOWN_NOT_RETROACTIVELY_FIXED",
        "native_VM_rerun": "NOT_RUN", "O1a_metric_PASS": False,
        "Linux_lint": {"original": "FAILED: 8 interop-test findings; preserved",
                       "fixed": "PASS: 0 issues, 10 B RAW, one command exit 0",
                       "subject": "compile subject plus exact two later interop-test fixes; 2109 maps unchanged"},
        "prior_evidence": "../d1-full-send-sender-2026-11-20/README.md",
        "new_D1_gate_or_approval": False,
        "later_interop_lint_fix": "Separate two-test-file subject; original failed lint retained, no global source maps recorded for that attempt. Original compile is not relabelled.",
    })
    archives = {"RAW": archive("RAW", RAW), "SOURCE": archive("SOURCE", SOURCE)}
    write_json("ORIGINS.json", ORIGINS)
    write_json("ARCHIVES.json", archives)
    paths = [p for p in HERE.iterdir() if p.is_file() and p.name != "SHA256SUMS"]
    (HERE / "SHA256SUMS").write_text("".join(sha(read(p)) + "  " + p.name + "\n" for p in sorted(paths)))
    print(json.dumps(archives, indent=2))


if __name__ == "__main__":
    main()
