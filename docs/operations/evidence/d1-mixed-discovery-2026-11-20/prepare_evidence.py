"""Package fixed local DATA and source references; no tests or applications run."""
from pathlib import Path, PurePosixPath
import hashlib
import io
import json
import tarfile

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
DAY = REPO / "docs/_internal/d1-artifacts/2026-11-20"
BASE = "a6a9fdc90d02134f5995df412a304f9e49201aa5"
RUNS = {
    "hmix": Path("/private/tmp/corsa-d1-o1a-hmix-existing-four-goqa-849357d5ee1349928e2ff52fc2d6c676"),
    "t7-v1": Path("/private/tmp/corsa-d1-o1a-t7-identity-presence-goqa-8fcf13814b7949949d96e6c9a91b1f45"),
    "t7-v2": Path("/private/tmp/corsa-d1-o1a-t7-resolver-diagnostic-goqa-8f4cfc7f33bc4298961617ae7f465c7c"),
    "t7-v3": Path("/private/tmp/corsa-d1-o1a-t7-producer-readiness-goqa-4f2cc32456334c1b93c99742414f27ba"),
    "final-guards": Path("/private/tmp/corsa-d1-o1a-t7-final-findnode-guards-994fb5b3e2b6439c9417497dde5f5de0"),
}
CONTROLLERS = {
    "hmix": Path("/private/tmp/corsa-hmix-existing-four-goqa.py"),
    "t7-v1": Path("/private/tmp/corsa-t7-identity-presence-goqa.py"),
    "t7-v2": Path("/private/tmp/corsa-t7-resolver-diagnostic-goqa.py"),
    "t7-v3": Path("/private/tmp/corsa-t7-producer-readiness-goqa.py"),
    "final-guards": Path("/private/tmp/corsa-t7-final-findnode-guards.py"),
}
PACKAGES = {
    "t7-v1": "d1-o1a-t7-identity-presence-source-v1",
    "t7-v2": "d1-o1a-t7-identity-presence-source-v2",
    "t7-v3": "d1-o1a-t7-identity-presence-source-v3",
}
REFERENCES = (
    "internal/core/node/find_node_owned_answer_test.go",
    "internal/core/node/find_node_mixed_peer_exchange_test.go",
    "internal/core/node/datagram_e2e_test.go",
    "internal/core/node/connection_manager_test.go",
    "internal/core/node/identity_resolver.go",
    "internal/core/node/identity_discovery.go",
    "internal/core/node/presence_prober.go",
    "internal/core/node/presence_service.go",
    "internal/core/node/routing_snapshot.go",
    "internal/core/node/routing_provider.go",
    "internal/core/node/hot_reads_refresh.go",
    "internal/core/datagram/metrics.go",
)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def read(path):
    path = Path(path)
    assert path.is_file() and not path.is_symlink() and path.stat().st_size <= 2 * 1024**2
    raw = path.read_bytes()
    assert len(raw) <= 2 * 1024**2 and not raw.startswith(b"\x7fELF")
    return raw


def add(files, name, path):
    parts = PurePosixPath(name)
    assert name not in files and not parts.is_absolute() and ".." not in parts.parts
    files[name] = read(path)


def js(name, value):
    (HERE / name).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def archive(name, files):
    assert len(files) <= 128 and sum(map(len, files.values())) <= 8 * 1024**2
    target = HERE / (name + ".tar.xz")
    with tarfile.open(target, "w:xz", format=tarfile.USTAR_FORMAT, preset=6) as out:
        for path, raw in sorted(files.items()):
            member = tarfile.TarInfo(path)
            member.size = len(raw)
            member.mode = 0o644
            member.uid = member.gid = member.mtime = 0
            member.uname = member.gname = ""
            out.addfile(member, io.BytesIO(raw))
    inventory = {n: {"bytes": len(v), "sha256": sha(v)} for n, v in sorted(files.items())}
    js(name + "-MEMBERS.json", inventory)
    seen = set()
    with tarfile.open(target, "r:xz") as inp:
        for member in inp:
            assert member.isfile() and member.name not in seen and not member.pax_headers
            assert member.mode == 0o644 and member.uid == member.gid == member.mtime == 0
            assert inp.extractfile(member).read() == files[member.name]
            seen.add(member.name)
    assert seen == files.keys()
    return {"members": len(files), "payload_bytes": sum(map(len, files.values())), "sha256": sha(read(target))}


def main():
    raw, source, runs = {}, {}, {}
    for label, work in RUNS.items():
        result = json.loads(read(work / "RESULT.json"))
        assert len(result["steps"]) == 1
        if label == "final-guards":
            assert result["T7_manifest_sha256"] == "c3aea5015be9871c7578f620dc7ec48c88e6e16bc66748ec3a3ed080038016e8"
        else:
            assert result["base_code_commit"] == BASE
        step = result["steps"][0]
        for key in ("stdout", "stderr"):
            path = Path(step[key])
            assert path.parent == work
            add(raw, label + "/" + path.name, path)
        for path in sorted(work.glob("[0-9][0-9]-*.json")):
            add(raw, label + "/" + path.name, path)
        for name in ("RESULT.json", "REVIEWER-DATA.json", "ROOT-DATA.json"):
            if (work / name).exists():
                add(raw, label + "/" + name, work / name)
        counter = "GO-SOURCE.json" if label in {"hmix", "final-guards"} else "SOURCE-MANIFEST.json"
        add(source, "source-hashes/" + label + "-" + counter, work / counter)
        add(source, "controllers/" + label + ".py", CONTROLLERS[label])
        events = [json.loads(x) for x in read(Path(step["stdout"])).splitlines()]
        outer = [x["Test"] for x in events if x.get("Action") == "pass" and x.get("Test") and "/" not in x["Test"]]
        child = sum(x.get("Action") == "pass" and "/" in x.get("Test", "") for x in events)
        runs[label] = {"attempt_id": result["attempt_id"], "status": result["status"], "outer_pass": outer,
                       "child_pass": child, "command_exit": step["exit"], "raw_bytes": result["raw_bytes"],
                       "result_sha256": sha(read(work / "RESULT.json")), "review_sha256": sha(read(work / "REVIEWER-DATA.json"))}
        js(label.upper() + "-RESULT.json", result)
    assert [(len(runs[n]["outer_pass"]), runs[n]["child_pass"]) for n in RUNS] == [(4, 3), (6, 15), (2, 12), (3, 12), (5, 2)]
    for label, package in PACKAGES.items():
        folder = DAY / package
        for path in sorted(folder.iterdir()):
            if path.is_file() and path.suffix in {".json", ".diff", ".md"}:
                add(source, label + "/" + path.name, path)
        manifest = json.loads(read(folder / "MANIFEST.json"))
        assert len(manifest["files"]) == 2 and manifest["production_files_changed"] == 0
        for row in manifest["files"]:
            path = folder / row["post"]
            assert sha(read(path)) == row["after_sha256"]
            add(source, label + "/" + row["post"], path)
    common = DAY / "d1-o1a-capexec-seccomp-build-v1/common.py"
    add(source, "controllers/common.py", common)
    base = json.loads(read(RUNS["hmix"] / "GO-SOURCE.json"))
    assert len(base) == 2103
    clean = Path("/private/tmp/corsa-d1-o1a-full-receipt-clean-findnode-guards-0216a464d806404aa07a2d8cc5acb633/source")
    pins = {}
    for name in REFERENCES:
        path = clean / name
        assert sha(read(path)) == base[name]
        add(source, "base-references/" + name, path)
        pins[name] = base[name]
    add(source, "base-references/internal/core/node/find_node_bare_v2_pilot_test.go", clean / "internal/core/node/find_node_bare_v2_pilot_test.go")
    pins["internal/core/node/find_node_bare_v2_pilot_test.go"] = base["internal/core/node/find_node_bare_v2_pilot_test.go"]
    source["REFERENCE-PINS.json"] = (json.dumps(pins, indent=2, sort_keys=True) + "\n").encode()
    summary = {"scope": "CURRENT_PROTOCOL31_FUNCTIONAL_HMIX_AND_T7_SLICE_ONLY", "base_code_commit": BASE,
               "runs": runs, "raw": archive("RAW", raw), "source": archive("SOURCE", source),
               "final_source_manifest_sha256": sha(read(DAY / PACKAGES["t7-v3"] / "MANIFEST.json")),
               "production_delta": 0, "final_test_posts": 2, "Hmix_and_T7_are_separate_invocations": True,
               "v1_failure_cause": "UNKNOWN; generic resolver wait, no retained state",
               "v2_diagnostic": "Initial canAttempt=false/noRoute=true/gen0; deferred success/authoritative gen1>0 after original wait failed; Pending1/refusals0",
               "v3_fix": "Await natural resolver cached-route readiness during setup before held FIND_NODE; proof/intake budgets unchanged",
               "released_old_protocol31_binary_delivery": "NOT_RUN", "13b_load_latency_resource_matrix": "NOT_MEASURED",
               "D1_admission": "NOT_ESTABLISHED", "public_activation": "NONE"}
    js("SUMMARY.json", summary)
    (HERE / "REVIEWER-DATA.json").write_bytes(read(RUNS["t7-v3"] / "REVIEWER-DATA.json"))
    (HERE / "GUARDS-REVIEWER-DATA.json").write_bytes(read(RUNS["final-guards"] / "REVIEWER-DATA.json"))
    files = sorted(p for p in HERE.iterdir() if p.is_file() and p.name != "SHA256SUMS")
    (HERE / "SHA256SUMS").write_text("".join(sha(read(p)) + "  " + p.name + "\n" for p in files))
    print(json.dumps({"files": len(files) + 1, "bytes": sum(p.stat().st_size for p in files) + (HERE / "SHA256SUMS").stat().st_size,
                      "SHA256SUMS": sha(read(HERE / "SHA256SUMS")), "raw": summary["raw"], "source": summary["source"]}))


if __name__ == "__main__":
    main()
