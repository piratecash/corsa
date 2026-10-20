#!/usr/bin/env python3
"""Owned bounded fake-Docker controls. No command contacts a real daemon.
Usage: test-loadstand-linux-owner-test.py ABSOLUTE_SCRIPT NEW_OUTPUT_DIR [CASES...]
"""
import hashlib
import json
import os
import pathlib
import signal
import socket
import subprocess
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent
SCRIPT = pathlib.Path(sys.argv[1]).resolve()
OUTPUT = pathlib.Path(sys.argv[2])
OUTPUT.mkdir(mode=0o700)
SOURCE = SCRIPT.read_text()
SOURCE_HASH = hashlib.sha256(SOURCE.encode()).hexdigest()
ALL_CASES = ["forged", "stale", "pipe", "pregroup", "exited", "failed_test",
             "running", "attach_running", "start_unknown", "mismatch", "wait_fail",
             "inspect_fail", "volume_before", "volume_after", "create_before",
             "create_after", "qualification_once", "qualification_unknown",
             "owner_mismatch", "owner_read_fail", "stop_kill_fail", "rm_fail",
             "volume_rm_fail", "absence_fail", "receipt_removed", "receipt_retained",
             "membership_unknown", "image_fail", "invalid_scenario", "empty_scenario", "select_restore",
             "select_pdeath", "select_fds", "select_blocked", "select_bootstrap", "select_runtime"]
ACTIVE = {}  # one owned fixture only; exception cleanup never signals another job
CASES = sys.argv[3:] or ALL_CASES
assert set(CASES) <= set(ALL_CASES)

FAKE = r'''#!/usr/bin/env python3
import os, sys, json, time, pathlib
path = pathlib.Path(os.environ["FAKE_DOCKER_STATE"])
state = json.loads(path.read_text())
case = os.environ["FAKE_SCENARIO"]
args = sys.argv[1:]
image = "sha256:33bee74c45f307e3268adc2010c0f55c48e7a6041e12cd12432bb1a46e498e43"
def save():
    temporary = path.with_name(path.name + "." + str(os.getpid()))
    temporary.write_text(json.dumps(state))
    temporary.replace(path)
def block_diagnostic():
    directory = pathlib.Path(os.environ["FAKE_BINARY_DIR"])
    (directory / ("outcome-" + state["owner"] + ".json")).mkdir(exist_ok=True)
outcome = pathlib.Path(os.environ["FAKE_BINARY_DIR"]) / ("outcome-" + str(state.get("owner")) + ".json")
with pathlib.Path(os.environ["FAKE_DOCKER_TRACE"]).open("a") as trace:
    trace.write(json.dumps({"args":args,"running":state.get("running",False),
                           "membership_unknown":state.get("membership_unknown",False),
                           "outcome_file_present":outcome.is_file()}) + "\n")
if args and args[0] == "context":
    print("unix://" + state["socket"])
elif args and args[0] == "ps":
    if state.get("membership_unknown"):
        sys.exit(1)
    if case == "absence_fail" and "--all" in args:
        sys.exit(1)
    if state.get("container") and ("--all" in args or state.get("running")):
        print(state["container"] if "--all" in args else "0123456789abcdef")
elif args[:2] == ["image", "inspect"]:
    if case == "image_fail":
        sys.exit(1)
    if case == "pipe":
        child = os.fork()
        if child == 0:
            pathlib.Path(os.environ["FAKE_PIPE_PID"]).write_text(str(os.getpid()))
            time.sleep(60)
            os._exit(0)
        time.sleep(60)
    print(image)
elif args[:2] == ["volume", "create"]:
    state["owner"] = args[-1].removeprefix("corsa-")
    if case == "volume_before":
        save()
        sys.exit(1)
    state["volume"] = args[-1]
    save()
    if case == "volume_after":
        sys.exit(1)
    print(args[-1])
elif args[:2] == ["volume", "inspect"]:
    state["volume_inspects"] = state.get("volume_inspects",0) + 1
    save()
    if not state.get("volume") or case == "qualification_unknown" or (case == "qualification_once" and state["volume_inspects"] == 1):
        sys.exit(1)
    print("local|" + state["owner"])
elif args[:2] == ["volume", "ls"]:
    if state.get("volume"):
        print(state["volume"])
elif args[:2] == ["volume", "rm"]:
    if case in ["volume_rm_fail", "receipt_retained"]:
        if case == "receipt_retained":
            block_diagnostic()
        sys.exit(1)
    state["volume"] = None
    save()
    if case == "receipt_removed":
        block_diagnostic()
elif args and args[0] == "create":
    if case == "create_before":
        sys.exit(1)
    state["container"] = args[args.index("--name") + 1]
    state["running"] = False
    state["phase"] = "created"
    state["actual_exit"] = 0
    save()
    if case == "create_after":
        sys.exit(1)
    print("0123456789abcdef")
elif args and args[0] == "start":
    state["running"] = case in ["running", "attach_running", "stop_kill_fail", "membership_unknown"]
    state["phase"] = "running" if state["running"] else "exited"
    state["actual_exit"] = 1 if case == "failed_test" else 0
    save()
    if case in ["failed_test", "attach_running", "mismatch", "stop_kill_fail", "membership_unknown"]:
        sys.exit(1)
    if case in ["start_unknown", "receipt_removed"]:
        sys.exit(124)
elif args and args[0] == "wait":
    if case in ["wait_fail", "attach_running", "stop_kill_fail", "membership_unknown"]:
        sys.exit(124 if case != "wait_fail" else 1)
    print(state.get("actual_exit",0))
elif args and args[0] == "inspect":
    if not state.get("container"):
        sys.exit(1)
    template = args[args.index("--format") + 1]
    if ".State." in template:
        state["outcome_collected"] = True
        if case == "membership_unknown":
            state["membership_unknown"] = True
        save()
        if case == "inspect_fail":
            sys.exit(1)
        print(state["phase"] + "|" + ("true" if state["running"] else "false") + "|false|false|" + str(state.get("actual_exit",0)) + "|" + state["owner"] + "|" + image)
    else:
        if case in ["owner_read_fail", "membership_unknown"] and state.get("outcome_collected"):
            sys.exit(1)
        owner = "foreign" if case == "owner_mismatch" else state["owner"]
        print(owner + "|" + image)
elif args and args[0] in ["stop", "kill"]:
    if case == "stop_kill_fail":
        sys.exit(1)
    state["running"] = False
    state["phase"] = "exited"
    state["actual_exit"] = 137
    save()
elif args and args[0] == "rm":
    if case == "rm_fail":
        sys.exit(1)
    state["container"] = None
    save()
else:
    sys.exit(2)
'''


def store_state(path, value):
    temporary = path.with_name(path.name + ".actor")
    temporary.write_text(json.dumps(value))
    temporary.replace(path)


def trace_rows(path):
    raw = path.read_text()
    lines = raw.splitlines()
    if raw and not raw.endswith("\n"):
        lines = lines[:-1]  # an append in progress is not yet a complete event
    return [json.loads(line) for line in lines if line]


def stop_owned_fixture(process, lock):
    owner = lock / "owner"
    if owner.exists():
        fields = dict(line.split("=", 1) for line in owner.read_text().splitlines() if "=" in line)
        if fields.get("pid") == str(process.pid) and fields.get("pgid", "").isdigit():
            try:
                os.killpg(int(fields["pgid"]), signal.SIGKILL)
            except ProcessLookupError:
                pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    return process.wait(timeout=3)


def process_live(pid):
    result = subprocess.run(["/bin/ps", "-p", str(pid), "-o", "stat="], capture_output=True, text=True, check=False)
    return bool(result.stdout.strip()) and not result.stdout.strip().startswith("Z")


def run_case(case):
    directory = OUTPUT / case
    directory.mkdir()
    (directory / "bin").mkdir()
    binaries = directory / "binaries"
    binaries.mkdir()
    for name in ["loadstand.test", "linuxhelper", "sdkedge"]:
        (binaries / name).write_text("fixture bytes\n")
    cli = directory / "bin/docker"
    cli.write_text(FAKE)
    cli.chmod(0o755)
    endpoint = socket.socket(socket.AF_UNIX)
    endpoint.bind(str(directory / "daemon.sock"))
    ACTIVE["endpoint"] = endpoint
    state_path = directory / "state.json"
    store_state(state_path, {"socket": str(directory / "daemon.sock"), "running": False, "container": None, "volume": None})
    lock, trace = directory / "lock", directory / "trace.jsonl"
    trace.write_text("")
    environment = os.environ.copy()
    environment.update(PATH=str(directory / "bin") + ":" + environment["PATH"],
                       HEAVY_LOCK=str(lock), FAKE_DOCKER_STATE=str(state_path),
                       FAKE_DOCKER_TRACE=str(trace), FAKE_SCENARIO=case,
                       FAKE_PIPE_PID=str(directory / "pipe.pid"), FAKE_BINARY_DIR=str(binaries),
                       HEAVY_DOCKER_TIMEOUT_S="2", DOCKER_HOST="unix://" + str(directory / "daemon.sock"))
    candidate, wrapper = SCRIPT, ROOT / "scripts/heavy-run.sh"
    fault_hash = None
    if case == "pregroup":
        fifo = directory / "before-group.fifo"
        os.mkfifo(fifo)
        marker = "setpgrp(0,0) or exit 125;"
        assert SOURCE.count(marker) == 1
        path = str(fifo).replace("\\", "\\\\").replace('"', '\\"').replace("$", "\\$").replace("@", "\\@")
        prefix = 'open(my $before_group,"<","' + path + '") or exit 125; '
        candidate = directory / "harness-fault.sh"
        expected_wrapper = '"$script_dir/heavy-run.sh"'
        assert SOURCE.count(expected_wrapper) == 1
        candidate.write_text(SOURCE.replace(expected_wrapper, '"' + str(wrapper) + '"').replace(marker, prefix + marker))
        fault_hash = hashlib.sha256(candidate.read_bytes()).hexdigest()
    if case in ["forged", "stale"]:
        lock.mkdir(mode=0o700)
        pid = os.getpid() if case == "forged" else 999999
        (lock / "owner").write_text("token=0123456789abcdef\npid=" + str(pid) + "\npid_lstart=wrong\npgid=" + str(os.getpgrp()) + "\nleader_lstart=wrong\n")
        command = ["/bin/bash", str(candidate), str(binaries)]
    else:
        command = [str(wrapper), "t5linux-fake-" + case, "/bin/bash", str(candidate), str(binaries)]
    selection = {"select_restore": "restore", "select_pdeath": "pdeath",
                 "select_fds": "fds", "select_blocked": "blocked-sample", "select_bootstrap": "bootstrap", "select_runtime": "runtime-start"}
    if case in selection:
        command.append(selection[case])
    if case in ["invalid_scenario", "empty_scenario"]:
        command = ["/bin/bash", str(candidate), str(binaries), "" if case == "empty_scenario" else "unknown"]
    actor_lock_confirmed = False
    with (directory / "output.log").open("w") as output:
        started = time.monotonic()
        process = subprocess.Popen(command, env=environment, cwd=ROOT, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
        ACTIVE.update(process=process, lock=lock)
        timed_out = False
        deadline = started + (9 if case in ["pipe", "pregroup"] else 25)
        while process.poll() is None and time.monotonic() < deadline:
            if case in ["stop_kill_fail", "membership_unknown"] and not actor_lock_confirmed:
                observed = trace_rows(trace)
                if any(row["args"][:2] == ["ps", "-q"] and row["outcome_file_present"] and (row["running"] or row["membership_unknown"]) for row in observed):
                    actor_lock_confirmed = lock.exists() and process.poll() is None
                    owned = json.loads(state_path.read_text())
                    owned["running"] = False
                    owned["membership_unknown"] = False
                    store_state(state_path, owned)  # trusted cleanup of this fake membership only
            time.sleep(0.02)
        if process.poll() is None:
            timed_out = True
            code = stop_owned_fixture(process, lock)
        else:
            code = process.wait()
        elapsed = time.monotonic() - started
    rows = trace_rows(trace)
    calls = [row["args"] for row in rows]
    final_state = json.loads(state_path.read_text())
    mutated = any(args and (args[0] in ["create", "start"] or args[:2] == ["volume", "create"]) for args in calls)
    cleanup_confirmed = final_state.get("container") is None and final_state.get("volume") is None
    records = [json.loads(path.read_text()) for path in binaries.glob("outcome-*.json") if path.is_file()]
    retention = [json.loads(path.read_text()) for path in binaries.glob("retention-*.json") if path.is_file()]
    log = (directory / "output.log").read_text()
    start_indices = [i for i, args in enumerate(calls) if args and args[0] == "start"]
    wait_indices = [i for i, args in enumerate(calls) if args and args[0] == "wait"]
    inspect_indices = [i for i, args in enumerate(calls) if args and args[0] == "inspect" and ".State." in args[args.index("--format") + 1]]
    teardown_indices = [i for i, args in enumerate(calls) if args and args[0] in ["stop", "kill", "rm"]]
    ordered = not start_indices or (len(wait_indices) == len(inspect_indices) == 1 and (not teardown_indices or max(wait_indices[0], inspect_indices[0]) < min(teardown_indices)))
    expected = 125
    if case in ["invalid_scenario", "empty_scenario"]:
        expected = 64
    elif case in ["forged", "stale"]:
        expected = 75
    elif case in ["pipe", "pregroup"]:
        expected = 124
    elif case == "exited" or case in selection:
        expected = 0
    elif case in ["failed_test", "volume_after", "create_after", "image_fail"]:
        expected = 1
    elif case == "qualification_once":
        expected = 65
    correct = code == expected and not timed_out and ordered
    if case in ["forged", "stale", "pipe", "pregroup", "image_fail", "invalid_scenario", "empty_scenario"]:
        correct = correct and not mutated
    retained_cases = {"volume_before", "create_before", "qualification_unknown", "owner_mismatch", "owner_read_fail", "stop_kill_fail", "rm_fail", "volume_rm_fail", "absence_fail", "receipt_retained", "membership_unknown"}
    if case in retained_cases:
        if case != "volume_before":
            correct = correct and not cleanup_confirmed
        if case != "receipt_retained":
            correct = correct and len(retention) == 1
    elif case not in ["forged", "stale", "pipe", "pregroup"]:
        correct = correct and cleanup_confirmed
    if records:
        record = records[0]
        correct = correct and record["final_rc"] == str(expected)
        if case == "failed_test":
            correct = correct and record["execution"] == "known_failure" and record["actual_exit"] == "1" and record["start_cli_rc"] == "1"
        if case in ["attach_running", "running", "stop_kill_fail", "membership_unknown"]:
            correct = correct and record["execution"] == "unknown" and record["observed_state"] == "running" and record["inspect_exit"] == "0"
        if case == "mismatch":
            correct = correct and record["execution"] == "contradictory" and record["actual_exit"] == "0" and record["start_cli_rc"] == "1"
        if case in ["volume_rm_fail", "receipt_retained"]:
            correct = correct and record["container_resource"] == "absent_confirmed"
    elif case not in ["forged", "stale", "receipt_removed", "receipt_retained", "invalid_scenario", "empty_scenario"]:
        correct = False
    if case in selection:
        exact = {"select_restore": "^TestLinuxAdversarialRestoreRetiresThread$",
                 "select_pdeath": "^TestLinuxAdversarialPdeathKillsTarget$",
                 "select_fds": "^TestLinuxAdversarialForbiddenFDRefusesReady$",
                 "select_blocked": "^TestLinuxAdversarialExitPrecedesBlockedSampleJoin$",
                 "select_bootstrap": "^(TestLinuxNamespaceOffMainBootstrap|TestLinuxNamespaceTaskRetirementConjunction|TestLinuxNamespaceTaskStateParsing)$",
                 "select_runtime": "^(TestLinuxRuntimePublicTupleValidation|TestLinuxRuntimeJointFactsRequired|TestLinuxRuntimeSharedWorkAndCopiedHistory|TestLinuxRuntimePublicationRejectsTerminalRaces|TestLinuxRuntimeReplyCorrelationAndOriginalNamespace|TestLinuxRuntimeTimeoutNeverReplaysLateSuccess|TestLinuxRuntimeClosedAuthorityInventory|TestLinuxRuntimeClosedAuthorityNegativeReferences|TestLinuxRuntimePublicationContextIsolatedFromOuterPoison|TestLinuxRuntimePinnedNamespaceDescriptors|TestLinuxRuntimeOwnedObservationNative|TestLinuxRuntimeClosedObservationPayload)$"}
        created = [args for args in calls if args and args[0] == "create"]
        correct = correct and len(created) == 1 and created[0][-2:] == ["corsa-owned", exact[case]]
    if case in ["invalid_scenario", "empty_scenario"]:
        correct = correct and not calls and not records and not retention and not lock.exists()
    if case in ["receipt_removed", "receipt_retained"]:
        correct = correct and not records and "diagnostic write failed" in log
    if case in ["stop_kill_fail", "membership_unknown"]:
        correct = correct and actor_lock_confirmed
    if case in ["rm_fail", "volume_rm_fail", "receipt_retained"]:
        correct = correct and not lock.exists()  # exited/volume residue is not a lock fence
    if case in ["volume_before", "create_before", "qualification_unknown", "qualification_once", "volume_after", "create_after", "owner_mismatch"]:
        correct = correct and not start_indices
    if case in ["pipe", "pregroup"]:
        correct = correct and elapsed >= 5 and "owned CLI group deadline" in log
    endpoint.close()
    live_child = False
    if (directory / "pipe.pid").exists():
        live_child = process_live(int((directory / "pipe.pid").read_text()))
        correct = correct and not live_child
    result = {"case":case,"script_sha256":SOURCE_HASH,"fault_sha256":fault_hash,"exit":code,
              "expected_exit":expected,"timed_out":timed_out,"seconds":round(elapsed,3),
              "docker_mutated":mutated,"cleanup_confirmed":cleanup_confirmed,
              "ordered_wait_inspect_before_teardown":ordered,"actor_lock_confirmed":actor_lock_confirmed,
              "control_pass":correct,"owned_pipe_child_live":live_child,"all_docker_calls_fake":True}
    print(json.dumps(result), flush=True)
    ACTIVE.clear()
    return result


try:
    results = [run_case(case) for case in CASES]
    assert hashlib.sha256(SCRIPT.read_bytes()).hexdigest() == SOURCE_HASH
    (OUTPUT / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    sys.exit(0 if all(row["control_pass"] for row in results) else 1)
finally:
    owned = ACTIVE.get("process")
    if owned is not None and owned.poll() is None:
        stop_owned_fixture(owned, ACTIVE["lock"])
    if ACTIVE.get("endpoint") is not None:
        ACTIVE["endpoint"].close()
