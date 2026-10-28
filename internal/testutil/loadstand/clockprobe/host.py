"""One owner-admitted two-actor probe; no Docker or other work on import."""
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import selectors
import stat
import subprocess
import sys
import time

IMAGE = "sha256:910cfcaf707e2051587b72b0b3baaa9ca1e05d3a6289de9e8b05f9e292b5b97e"
DOCKER = "/Users/hades/.docker/bin/docker"
ENDPOINT = "unix:///Users/hades/.docker/run/docker.sock"
WRAPPER = Path("/Users/hades/Documents/repos/corsa/scripts/heavy-run.sh")
LOCK = Path("/private/tmp/corsa-heavy-run.lock")
MAX_ROLE, MAX_TOTAL, MEMORY = 65536, 262144, 134217728
PROBE_TEXT, ROOT, TOTAL, CALLS, LIVE = "", None, 0, [], []
CLAIMED = False
COMMAND_ATTEMPT = 0


class Fault(Exception):
    pass


def need(value, reason):
    if not value:
        raise Fault(reason)


def read_owned(path, limit):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        st = os.fstat(fd)
        need(stat.S_ISREG(st.st_mode) and st.st_uid == 501 and st.st_nlink == 1 and
             not st.st_mode & 0o022 and 0 < st.st_size <= limit, "input-custody")
        raw = os.read(fd, limit + 1)
        stamp = lambda s: (s.st_dev, s.st_ino, s.st_mode, s.st_uid, s.st_gid, s.st_nlink,
                           s.st_size, s.st_mtime_ns, s.st_ctime_ns)
        need(len(raw) == st.st_size and stamp(os.fstat(fd)) == stamp(st) == stamp(Path(path).lstat()), "input-changed")
        return raw
    finally:
        os.close(fd)


def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            need(key not in result, "duplicate-json-key")
            result[key] = value
        return result
    def invalid(_):
        raise Fault("nonfinite-json")
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid)


def put(path, value):
    raw = json.dumps(value, separators=(",", ":"), allow_nan=False).encode() + b"\n"
    need(len(raw) <= MAX_TOTAL, "receipt-limit")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        need(os.write(fd, raw) == len(raw), "receipt-short-write")
        os.fsync(fd)
    finally:
        os.close(fd)
    directory = os.open(Path(path).parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


class Child:
    def __init__(self, args, label, interactive=False):
        self.label, self.out, self.err = label, bytearray(), bytearray()
        self.files, self.selector = {}, None
        self.p = subprocess.Popen([DOCKER, "--host", ENDPOINT] + args,
            stdin=subprocess.PIPE if interactive else subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=False,
            env={"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "HOME": "/Users/hades", "LC_ALL": "C"})
        LIVE.append(self)
        try:
            self.selector = selectors.DefaultSelector()
            for stream, target in ((self.p.stdout, self.out), (self.p.stderr, self.err)):
                os.set_blocking(stream.fileno(), False)
                self.selector.register(stream, selectors.EVENT_READ, target)
            if ROOT is not None:
                for name in ("out", "err"):
                    self.files[name] = os.open(ROOT / (label + "." + name),
                        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        except BaseException:
            self.close(time.monotonic() + 1)
            raise

    def pump(self, deadline, first_line=False):
        global TOTAL
        while True:
            if first_line and b"\n" in self.out:
                return
            if not self.selector.get_map():
                self.p.wait(timeout=max(.001, deadline - time.monotonic()))
                return
            need(time.monotonic() < deadline, "command-deadline")
            for key, _ in self.selector.select(min(.05, max(0, deadline - time.monotonic()))):
                try:
                    raw = os.read(key.fd, 4096)
                except BlockingIOError:
                    continue
                if not raw:
                    self.selector.unregister(key.fileobj)
                    continue
                need(len(self.out) + len(self.err) + len(raw) <= MAX_ROLE and
                     TOTAL + len(raw) <= MAX_TOTAL, "command-output-limit")
                TOTAL += len(raw)
                name = "out" if key.data is self.out else "err"
                if name in self.files:
                    need(os.write(self.files[name], raw) == len(raw), "raw-short-write")
                key.data.extend(raw)

    def close(self, deadline):
        reaped = self.p.poll() is not None
        if not reaped:
            self.p.terminate()
            try:
                self.p.wait(timeout=max(.001, min(.5, deadline - time.monotonic())))
            except subprocess.TimeoutExpired:
                self.p.kill()
                try:
                    self.p.wait(timeout=max(.001, deadline - time.monotonic()))
                except subprocess.TimeoutExpired:
                    pass
        reaped = self.p.returncode is not None
        if self.selector is not None:
            self.selector.close()
        for stream in (self.p.stdin, self.p.stdout, self.p.stderr):
            if stream is not None:
                stream.close()
        for name in list(self.files):
            fd = self.files.pop(name)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        return reaped


def command(args, deadline, permit_failure=False):
    global COMMAND_ATTEMPT
    attempt = COMMAND_ATTEMPT
    COMMAND_ATTEMPT += 1
    label = "command-" + str(attempt)
    record = {"argv": args, "attempt": attempt, "raw_label": label,
              "exit": None, "out": None, "err": None}
    CALLS.append(record)
    child = Child(args, label)
    try:
        child.pump(deadline)
        record.update(exit=child.p.returncode,
                      out=bytes(child.out).decode("utf8"), err=bytes(child.err).decode("utf8"))
        need(permit_failure or child.p.returncode == 0, "docker-command-failed")
        return bytes(child.out), child.p.returncode
    finally:
        record.update(out_bytes=len(child.out), err_bytes=len(child.err))
        try:
            need(child.close(deadline), "docker-cli-unreaped")
        finally:
            record["exit"] = child.p.returncode


def create_argv(role, nonce, keeper_id=None):
    need(role in ("keeper", "observer") and re.fullmatch("[0-9a-f]{32}", nonce), "role-nonce")
    need(role == "keeper" or re.fullmatch("[0-9a-f]{64}", keeper_id or ""), "keeper-id")
    args = ["create", "--pull=never", "--name", "corsa-d1-clockprobe-" + nonce + "-" + role,
        "--label", "corsa.loadstand", "--label", "corsa.clockprobe.owner=" + nonce,
        "--label", "corsa.clockprobe.role=" + role, "--restart=no", "--user=501:20",
        "--cpus=0.25", "--memory=" + str(MEMORY), "--memory-swap=" + str(MEMORY),
        "--pids-limit=32", "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges"]
    args += ["--network=none", "-i"] if role == "keeper" else ["--pid=container:" + keeper_id, "--network=container:" + keeper_id]
    return args + ["--entrypoint=/usr/bin/python3", IMAGE, "-I", "-S", "-B", "-c", PROBE_TEXT, role, nonce]


def offset_values(value):
    need(type(value) is dict and set(value) == {"raw_base64", "sha256", "values"}, "packet-offset-fields")
    need(type(value["raw_base64"]) is str and len(value["raw_base64"]) <= 2048 and
         type(value["sha256"]) is str, "packet-offset-types")
    raw = base64.b64decode(value["raw_base64"], validate=True)
    need(len(raw) <= 1024 and raw.endswith(b"\n") and b"\r" not in raw and
         base64.b64encode(raw).decode("ascii") == value["raw_base64"] and
         hashlib.sha256(raw).hexdigest() == value["sha256"], "packet-offset-raw")
    parsed = {}
    for line in raw.decode("ascii").splitlines():
        parts = line.split()
        need(len(parts) == 3 and parts[0] in ("monotonic", "boottime") and parts[0] not in parsed and
             re.fullmatch(r"-?[0-9]{1,20}", parts[1]) and re.fullmatch(r"[0-9]{1,9}", parts[2]), "packet-offset-format")
        sec, nano = int(parts[1]), int(parts[2])
        need(-(1 << 63) <= sec < (1 << 63) and 0 <= nano < 1000000000, "packet-offset-range")
        parsed[parts[0]] = {"seconds": sec, "nanoseconds": nano}
    need(set(parsed) == {"monotonic", "boottime"} and type(value["values"]) is dict and
         all(type(v) is dict and all(type(n) is int for n in v.values()) for v in value["values"].values()) and
         parsed == value["values"], "packet-offset-values")


def thread_reader_values(row):
    proof = row.get("offsets_reader")
    need(type(proof) is dict and set(proof) == {"path", "thread_self_link_before",
         "thread_self_link_after", "before", "after"}, "packet-thread-reader-fields")
    link = proof["thread_self_link_before"]
    need(type(link) is str and type(proof["thread_self_link_after"]) is str and
         proof["thread_self_link_after"] == link, "packet-thread-link-stability")
    match = re.fullmatch(r"([1-9][0-9]{0,9})/task/([1-9][0-9]{0,9})", link)
    need(match is not None, "packet-thread-link-format")
    tgid, tid = (int(v) for v in match.groups())
    need(tgid < (1 << 31) and tid < (1 << 31) and type(proof["path"]) is str and
         proof["path"] == "/proc/" + str(tid) and row["process"]["pid"] == tid and
         row["status"]["Pid"] == [tid] and row["status"]["Tgid"] == [tgid], "packet-thread-reader-pid-binding")
    expected = {k: row[k] for k in ("process", "status", "time", "time_for_children")}
    for side in ("before", "after"):
        witness = proof[side]
        need(type(witness) is dict and set(witness) == set(expected) and
             type(witness["process"]) is dict and type(witness["status"]) is dict and
             all(type(x) is int for x in witness["process"].values()) and
             all(type(v) is list and all(type(x) is int for x in v)
                 for v in witness["status"].values()) and
             all(type(witness[k]) is list and all(type(x) is int for x in witness[k])
                 for k in ("time", "time_for_children")) and witness == expected,
             "packet-thread-reader-bookend")


def lifetime_values(values):
    need(type(values) is dict and set(values) == {"self", "thread_self", "numeric_pid"} and
         all(type(v) is dict and set(v) == {"time", "time_for_children"} for v in values.values()), "lifetime-shape")
    need(all(type(pair) is list and len(pair) == 2 and all(type(x) is int for x in pair) and
         0 <= pair[0] < (1 << 64) and 0 < pair[1] < (1 << 64)
         for row in values.values() for pair in row.values()), "lifetime-identity")


def packet(raw, role, phase, nonce):
    need(raw.count(b"\n") == 1 and len(raw) <= MAX_ROLE, "packet-framing")
    value = strict_json(raw)
    need(type(value) is dict and value.get("schema") == "d1-clock-probe-v1" and value.get("nonce") == nonce and
         value.get("role") == role and value.get("phase") == phase and
         type(value.get("complete")) is bool, "packet-binding")
    need(type(value.get("pid")) is int and 0 < value["pid"] < (1 << 31) and
         type(value.get("native_tid")) is int and 0 < value["native_tid"] < (1 << 31), "packet-actor-id")
    if value["complete"]:
        names = {"self", "thread_self", "numeric_pid"} | ({"target_keeper"} if role == "observer" else set())
        need(type(value.get("contexts")) is dict and set(value["contexts"]) == names and
             type(value.get("supported_source_guard")) is bool and
             type(value.get("shared_time_domain")) is bool and value.get("qualification") == "UNKNOWN" and
             all(type(value.get(k)) is bool for k in ("own_aliases_equal", "actor_pid_view_matches",
                 "old_numeric_comparator_equal", "actor_current_equals_target_current")), "complete-packet-fields")
        need(type(value.get("current_equals_for_children")) is dict and set(value["current_equals_for_children"]) == names and
             all(type(v) is bool for v in value["current_equals_for_children"].values()), "packet-current-children")
        for name, row in value["contexts"].items():
            need(type(row) is dict, "packet-context")
            expected = {"self": "/proc/self", "thread_self": "/proc/thread-self", "numeric_pid": "/proc/" + str(value["pid"]), "target_keeper": "/proc/1"}
            need(row.get("path") == expected[name], "packet-proc-path")
            for key in ("time", "time_for_children"):
                pair = row.get(key)
                need(type(pair) is list and len(pair) == 2 and all(type(x) is int for x in pair) and
                     0 <= pair[0] < (1 << 64) and 0 < pair[1] < (1 << 64), "packet-ns-identity")
            proc = row.get("process", {})
            need(type(proc) is dict and set(proc) == {"pid", "ppid", "start_ticks"} and
                 all(type(x) is int and 0 <= x < (1 << 64) for x in proc.values()) and
                 0 < proc["pid"] < (1 << 31) and proc["ppid"] < (1 << 31), "packet-process")
            status = row.get("status", {})
            need(type(status) is dict and set(status) == {"Pid", "Tgid", "Uid", "Gid", "NSpid"} and all(type(v) is list and
                 1 <= len(v) <= 32 and all(type(x) is int and 0 <= x < (1 << 64) for x in v) for v in status.values()) and
                 len(status["Pid"]) == len(status["Tgid"]) == 1 and len(status["Uid"]) == len(status["Gid"]) == 4,
                 "packet-status")
            need(all(0 < x < (1 << 31) for k in ("Pid", "Tgid", "NSpid") for x in status[k]) and
                 all(x < (1 << 32) for k in ("Uid", "Gid") for x in status[k]), "packet-status-range")
            offset_values(row.get("offsets_for_children"))
        thread_reader_values(value["contexts"]["thread_self"])
        if role == "keeper":
            lifetime_values(value.get("lifetime_held_before" if phase == "keeper-ready" else "lifetime_held_after"))
        bracket = value.get("own_boottime_bracket_ns")
        need(type(bracket) is list and len(bracket) == 2 and all(type(x) is int and 0 <= x < (1 << 63) for x in bracket)
             and type(value.get("own_boottime_observation_ns")) is int and
             bracket[0] <= value["own_boottime_observation_ns"] <= bracket[1], "packet-clock")
    return value


def inspect_container(ident, deadline):
    fields = {"Id": ".Id", "Image": ".Image", "Name": ".Name", "Labels": ".Config.Labels", "User": ".Config.User",
        "State": ".State", "Mounts": ".Mounts"}
    fields.update({n: ".HostConfig." + n for n in ("Memory", "MemorySwap", "NanoCpus", "PidsLimit", "Privileged",
        "ReadonlyRootfs", "CapDrop", "SecurityOpt", "PidMode", "NetworkMode")})
    template = "{" + ",".join(json.dumps(k) + ":{{json " + v + "}}" for k, v in fields.items()) + "}"
    return strict_json(command(["inspect", "--format", template, ident], deadline)[0])


def verify_created(meta, role, nonce, keeper_id):
    need(meta["Image"] == IMAGE and meta["User"] == "501:20" and meta["Mounts"] == [] and
         meta["Privileged"] is False and meta["ReadonlyRootfs"] is True and meta["CapDrop"] == ["ALL"] and
         meta["SecurityOpt"] == ["no-new-privileges"], "actual-container-isolation")
    need(all(type(meta[k]) is int and meta[k] == v for k, v in
         {"Memory": MEMORY, "MemorySwap": MEMORY, "NanoCpus": 250000000, "PidsLimit": 32}.items()), "actual-role-budget")
    need(meta["Labels"].get("corsa.clockprobe.owner") == nonce and meta["Labels"].get("corsa.clockprobe.role") == role
         and meta["Name"] == "/corsa-d1-clockprobe-" + nonce + "-" + role, "actual-owner")
    need(meta["PidMode"] == ("" if role == "keeper" else "container:" + keeper_id) and
         meta["NetworkMode"] == ("none" if role == "keeper" else "container:" + keeper_id), "actual-role-namespaces")


def join_packets(first, observed, stopped):
    keeper, target, after = first["contexts"]["self"], observed["contexts"]["target_keeper"], stopped["contexts"]["self"]
    lifetime = first.get("lifetime_held_before")
    for values in (lifetime, stopped.get("lifetime_held_after")):
        lifetime_values(values)
    bound = (first["pid"] == stopped["pid"] == 1 and all(keeper[k] == target[k] == after[k]
        for k in ("process", "status", "time", "time_for_children")) and
        lifetime == stopped.get("lifetime_held_after") and
        all(lifetime[n][k] == first["contexts"][n][k] == stopped["contexts"][n][k]
            for n in lifetime for k in ("time", "time_for_children")))
    actor_equal = first["contexts"]["thread_self"]["time"] == observed["contexts"]["thread_self"]["time"] == stopped["contexts"]["thread_self"]["time"]
    return {"cross_actor_binding_equal": bound, "actor_current_equal": actor_equal,
        "shared_time_domain": bound and actor_equal and all(p["supported_source_guard"] for p in (first, observed, stopped))}


def run(admission_path):
    global ROOT, PROBE_TEXT, TOTAL, CALLS, LIVE, CLAIMED, COMMAND_ATTEMPT
    ROOT, TOTAL, CALLS, LIVE = None, 0, [], []
    CLAIMED = False
    COMMAND_ATTEMPT = 0
    began = time.monotonic(); work, end = began + 30, began + 60
    admission_raw = read_owned(admission_path, 16384)
    cfg = strict_json(admission_raw)
    need(type(cfg) is dict and cfg.get("owner_approved") is True and cfg.get("one_attempt") is True and
         cfg.get("allow_recovery_hold") is True and cfg.get("profile") == "d1-clock-probe-v1", "INERT-owner-admission")
    source = read_owned(Path(__file__).with_name("probe.py"), 32768)
    need(hashlib.sha256(source).hexdigest() == cfg["probe_sha256"] and
         hashlib.sha256(read_owned(__file__, 65536)).hexdigest() == cfg["host_sha256"] and
         hashlib.sha256(read_owned(WRAPPER, 32768)).hexdigest() == cfg["wrapper_sha256"], "source-binding")
    PROBE_TEXT = source.decode("utf8")
    need((os.getuid(), os.getgid()) == (501, 20), "host-owner")
    owner = dict(line.split("=", 1) for line in read_owned(LOCK / "owner", 8192).decode().splitlines() if "=" in line)
    need(owner.get("name") == "d1-clock-probe" and owner.get("pgid") == str(os.getpgrp()), "own-heavy-group")
    parent = Path(cfg["run_parent"])
    need(parent.is_absolute() and len(str(parent)) <= 512 and parent.resolve(strict=True) == parent, "output-parent-path")
    pst = parent.stat(); need(stat.S_ISDIR(pst.st_mode) and pst.st_uid == 501 and not pst.st_mode & 0o077, "output-parent-custody")
    space = os.statvfs(parent); need(space.f_bavail * space.f_frsize >= 10 * 1024**3 + len(source), "host-space")
    endpoint = strict_json(command(["context", "inspect", "--format", "{{json .Endpoints.docker.Host}}", "desktop-linux"], work)[0])
    need(endpoint == ENDPOINT, "local-endpoint-required")
    command(["info", "--format", "{{json .ServerVersion}}"], work)
    image = strict_json(command(["image", "inspect", "--format", '{"Id":{{json .Id}},"Os":{{json .Os}},"Architecture":{{json .Architecture}},"Volumes":{{json (index .Config "Volumes")}}}', IMAGE], work)[0])
    need(image == {"Id": IMAGE, "Os": "linux", "Architecture": "arm64", "Volumes": None} or
         image == {"Id": IMAGE, "Os": "linux", "Architecture": "arm64", "Volumes": {}}, "image-platform-or-implicit-volume")
    nonce = secrets.token_hex(16); ROOT = parent / ("clockprobe-" + nonce); ROOT.mkdir(mode=0o700)
    need(read_owned(admission_path, 16384) == admission_raw, "admission-changed")
    CLAIMED = True
    put(Path(admission_path).with_name("ONE-ATTEMPT-CLAIM.json"), {"nonce": nonce, "retries": False})
    children, ids, uncertain_create = [], [], False
    result = {"nonce": nonce, "status": "INCOMPLETE", "diagnostic_complete": False, "shared_time_domain": None,
              "capture_complete": False, "provisional": True, "resource_qualification": False,
              "admission_sha256": hashlib.sha256(admission_raw).hexdigest()}
    try:
        for role in ("keeper", "observer"):
            uncertain_create = True
            raw, _ = command(create_argv(role, nonce, ids[0] if ids else None), work)
            ident = raw.decode().strip(); need(re.fullmatch("[0-9a-f]{64}", ident), "created-id")
            ids.append(ident); uncertain_create = False
            verify_created(inspect_container(ident, work), role, nonce, ids[0])
            child = Child(["start", "-a"] + (["-i"] if role == "keeper" else []) + [ident], role, role == "keeper")
            children.append(child); child.pump(work, role == "keeper")
            if role == "keeper":
                first = packet(bytes(child.out), role, "keeper-ready", nonce)
                need(first["complete"] and not child.err, "keeper-capture-incomplete")
            else:
                observed = packet(bytes(child.out), role, "observer", nonce)
                need(observed["complete"] and child.p.returncode == 0 and not child.err, "observer-capture-incomplete")
        children[0].p.stdin.write(b"STOP\n"); children[0].p.stdin.flush(); children[0].p.stdin.close()
        children[0].pump(work)
        lines = bytes(children[0].out).splitlines(keepends=True); need(len(lines) == 2, "keeper-record-count")
        stopped = packet(lines[1], "keeper", "keeper-stop", nonce)
        need(stopped["complete"] and children[0].p.returncode == 0 and not children[0].err, "keeper-stop-incomplete")
        for ident in ids:
            state = inspect_container(ident, work)["State"]
            need(state["Running"] is False and type(state["ExitCode"]) is int and state["ExitCode"] == 0
                 and state["OOMKilled"] is False, "actor-exit-or-oom")
        result.update(status="COMPLETE_DIAGNOSTIC", capture_complete=True, diagnostic_complete=True,
            **join_packets(first, observed, stopped),
            packets={"keeper_before": first, "observer": observed, "keeper_after": stopped})
    except (OSError, ValueError, Fault, subprocess.SubprocessError, TypeError, KeyError, AttributeError) as error:
        result["error"] = {"type": type(error).__name__, "reason": str(error)[:160]}
    finally:
        closed = not uncertain_create
        for child in reversed(LIVE):
            try:
                closed = child.close(end) and closed
            except BaseException:
                result["diagnostic_complete"] = False; result["status"] = "INCOMPLETE"
                closed = child.p.returncode is not None and closed
        try:
            owned = command(["ps", "-a", "--no-trunc", "--filter", "label=corsa.clockprobe.owner=" + nonce, "--format", "{{.ID}}"], end)[0].decode().splitlines()
            for ident in ids:
                owned += command(["ps", "-a", "--no-trunc", "--filter", "id=" + ident, "--format", "{{.ID}}"], end)[0].decode().splitlines()
            for ident in sorted(set(owned)):
                need(re.fullmatch("[0-9a-f]{64}", ident), "cleanup-id")
                meta = strict_json(command(["inspect", "--format", '{"Owner":{{json (index .Config.Labels "corsa.clockprobe.owner")}},"Role":{{json (index .Config.Labels "corsa.clockprobe.role")}},"Name":{{json .Name}}}', ident], end)[0])
                need(meta["Owner"] == nonce and meta["Role"] in ("keeper", "observer") and meta["Name"] == "/corsa-d1-clockprobe-" + nonce + "-" + meta["Role"], "cleanup-ownership")
                command(["rm", "-f", ident], end)
            need(not command(["ps", "-a", "--no-trunc", "--filter", "label=corsa.clockprobe.owner=" + nonce, "--format", "{{.ID}}"], end)[0].strip(), "owned-containers-remain")
            for ident in ids:
                need(not command(["ps", "-a", "--no-trunc", "--filter", "id=" + ident, "--format", "{{.ID}}"], end)[0].strip(), "known-container-remains")
        except BaseException:
            closed = False
        result["physical_closure"] = "KNOWN" if closed else "UNKNOWN"
        if not closed or not result["diagnostic_complete"]:
            result["status"] = "INCOMPLETE" if closed else "RECOVERY_REQUIRED"
            result["diagnostic_complete"] = False; result["shared_time_domain"] = None
        try:
            put(ROOT / "commands.json", CALLS); put(ROOT / "result.json", result)
        except BaseException:
            result["diagnostic_complete"] = False; result["status"] = "INCOMPLETE"
            result["shared_time_domain"] = None
        if not closed:
            # Remain in the real heavy job group. Only operator recovery may
            # stop this hold; a text flag or an exit would not hold that group.
            try:
                print(json.dumps({"status": "RECOVERY_REQUIRED", "run": str(ROOT)}), flush=True)
            except BaseException:
                pass
            while True:
                time.sleep(3600)
    print(json.dumps({"status": result["status"], "run": str(ROOT)}), flush=True)
    return 0 if result["diagnostic_complete"] else 2


def main(argv=None):
    args = sys.argv[1:] if argv is None else argv
    need(len(args) == 1, "INERT-admission-path-required")
    return run(Path(args[0]))


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, Fault, subprocess.SubprocessError, TypeError, KeyError, AttributeError) as error:
        print(json.dumps({"status": "INCOMPLETE" if CLAIMED else "NOT_STARTED", "claim_attempted": CLAIMED,
            "error": type(error).__name__, "reason": str(error)[:160]}), flush=True)
        raise SystemExit(2)
