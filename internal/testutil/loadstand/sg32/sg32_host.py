"""One inert-until-admitted SG32 controller. No Docker work on import."""
import json
import math
import os
from pathlib import Path
import re
import secrets
import selectors
import stat
import subprocess
import sys
import time

from sg32_common import Fault, need, strict_json, encode, read_owned, digest, static_elf, hex_value, INPUT_NAMES, IMAGE, verify_driver, verify_service

DOCKER = "/Users/hades/.docker/bin/docker"
ENDPOINT = "unix:///Users/hades/.docker/run/docker.sock"
WRAPPER = Path("/Users/hades/Documents/repos/corsa/scripts/heavy-run.sh")
LOCK = Path("/private/tmp/corsa-heavy-run.lock")
ROLE_LIMITS = {"preparer": (250000000, 134217728, 32),
               "service": (1000000000, 512000000, 64),
               "driver": (1000000000, 268435456, 32)}
OUTPUT_LIMITS = {"control": 4 * 1024**2, "service": 16 * 1024**2, "driver": 16 * 1024**2}
BOOTSTRAP = "import sys;sys.path.insert(0,'/inputs');import sg32_service;sys.exit(sg32_service.main())"


def volume_names(nonce):
    need(hex_value(nonce, 32), "nonce")
    return {role: "corsa-d1-sg32-" + nonce + "-" + role for role in ("inputs", "secret", "state")}


def create_argv(role, nonce, volumes, service_id=None, cli_sha=None):
    need(role in ROLE_LIMITS and volumes == volume_names(nonce), "role-volumes")
    cpu, memory, pids = ROLE_LIMITS[role]
    args = ["create", "--pull=never", "--name", "corsa-d1-sg32-" + nonce + "-" + role,
        "--label", "corsa.loadstand", "--label", "corsa.sg32.owner=" + nonce,
        "--label", "corsa.sg32.role=" + role, "--restart=no", "--read-only", "--cgroupns=private",
        "--cpus=" + ("0.25" if role == "preparer" else "1"), "--memory=" + str(memory),
        "--memory-swap=" + str(memory), "--pids-limit=" + str(pids), "--cap-drop=ALL",
        "--security-opt=no-new-privileges", "--user=" + ("0:0" if role == "preparer" else "501:20")]
    if role == "preparer":
        args += ["--cap-add=CHOWN", "--network=none", "-i"]
    elif role == "service":
        need(hex_value(cli_sha, 64), "cli-sha")
        args += ["--network=none", "-i"]
    else:
        need(hex_value(service_id, 64), "service-id")
        args += ["--network=container:" + service_id, "--env=CORSA_C2B32_RUN=1", "-i"]
    destinations = {"inputs": "/inputs", "secret": "/run/c1", "state": "/work"}
    for name in ("inputs", "secret", "state"):
        if role == "driver" and name == "state":
            continue
        readonly = role != "preparer" and name != "state"
        args += ["--mount", "type=volume,source=" + volumes[name] + ",target=" + destinations[name] +
                 (",readonly,volume-nocopy" if readonly else ",volume-nocopy")]
    if role == "driver":
        return args + ["--entrypoint=/inputs/c2driver", IMAGE]
    return args + ["--entrypoint=/usr/bin/python3", IMAGE, "-I", "-S", "-B", "-c", BOOTSTRAP,
                   "preparer" if role == "preparer" else "keeper", nonce] + ([] if role == "preparer" else [cli_sha])


def verify_created(meta, role, nonce, volumes, service_id=None):
    need(role in ROLE_LIMITS and volumes == volume_names(nonce), "role-volumes")
    cpu, memory, pids = ROLE_LIMITS[role]
    config, host, state = meta["Config"], meta["HostConfig"], meta["State"]
    need(meta["Image"] == IMAGE and meta["Name"] == "/corsa-d1-sg32-" + nonce + "-" + role and
         config["Labels"].get("corsa.sg32.owner") == nonce and config["Labels"].get("corsa.sg32.role") == role,
         "actual-owner-or-image")
    need(config["User"] == ("0:0" if role == "preparer" else "501:20") and
         host["Privileged"] is False and host["ReadonlyRootfs"] is True and host["CapDrop"] == ["ALL"] and
         host["SecurityOpt"] == ["no-new-privileges"] and host["RestartPolicy"]["Name"] == "no",
         "actual-isolation")
    need(host.get("CapAdd") in (None, []) if role != "preparer" else host.get("CapAdd") in (["CHOWN"], ["CAP_CHOWN"]), "actual-capabilities")
    need(all(type(host[k]) is int and host[k] == value for k, value in
         {"Memory": memory, "MemorySwap": memory, "NanoCpus": cpu, "PidsLimit": pids}.items()), "actual-budget")
    need(host["NetworkMode"] == ("container:" + service_id if role == "driver" else "none") and
         host.get("PidMode", "") == "" and host.get("CgroupnsMode") == "private" and
         not host.get("Binds") and not host.get("PortBindings"), "actual-namespaces")
    expected = {"/inputs": (volumes["inputs"], role == "preparer"),
                "/run/c1": (volumes["secret"], role == "preparer")}
    if role != "driver":
        expected["/work"] = (volumes["state"], True)
    mounts = meta["Mounts"]
    need(type(mounts) is list and len(mounts) == len(expected), "actual-mount-count")
    for mount in mounts:
        need(mount["Type"] == "volume" and type(mount["RW"]) is bool and mount["Destination"] in expected and
             (mount["Name"], mount["RW"]) == expected.pop(mount["Destination"]), "actual-mount")
    need(not expected and state["Running"] is False, "actual-created-state")
    if role == "driver":
        env = config.get("Env", [])
        need(env.count("CORSA_C2B32_RUN=1") == 1 and not any(x.startswith("CORSA_C2A_RUN=") for x in env), "driver-environment")


class Child:
    def __init__(self, engine, args, role, interactive=False):
        self.engine, self.role = engine, role
        self.out, self.err, self.fds, self.files = bytearray(), bytearray(), set(), {}
        self.number = engine.attempt
        engine.attempt += 1
        self.p = subprocess.Popen([DOCKER, "--host", ENDPOINT] + args,
            stdin=subprocess.PIPE if interactive else subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=False,
            env={"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "HOME": "/Users/hades", "LC_ALL": "C"})
        engine.live.append(self)  # Ownership precedes all pipe/file setup.
        try:
            for name, stream in (("out", self.p.stdout), ("err", self.p.stderr)):
                fd = stream.fileno()
                os.set_blocking(fd, False)
                engine.selector.register(stream, selectors.EVENT_READ, (self, name))
                self.fds.add(fd)
                if engine.root is not None:
                    self.files[name] = os.open(engine.root / ("child-" + str(self.number) + "." + name),
                        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        except BaseException:
            self.close(time.monotonic() + 1)
            raise

    def accept(self, name, raw):
        self.engine.charge(self.role, len(raw))
        target = self.out if name == "out" else self.err
        if name in self.files:
            need(os.write(self.files[name], raw) == len(raw), "raw-short-write")
        target.extend(raw)

    def send(self, raw):
        need(type(raw) is bytes and 0 < len(raw) <= 4096 and self.p.stdin is not None, "control-bound")
        need(self.p.stdin.write(raw) == len(raw), "control-short-write")
        self.p.stdin.flush()

    def finish_input(self):
        if self.p.stdin is not None and not self.p.stdin.closed:
            self.p.stdin.close()

    def done(self):
        return not self.fds and self.p.poll() is not None

    def records(self, complete=False):
        raw = bytes(self.out)
        if complete:
            need(not raw or raw.endswith(b"\n"), "partial-protocol-line")
        lines = raw.split(b"\n")[:-1]
        need(len(raw.rsplit(b"\n", 1)[-1]) <= 65536, "partial-protocol-line-bound")
        need(len(lines) <= 4096 and all(0 < len(line) <= 65536 for line in lines), "protocol-lines-bound")
        return [strict_json(line) for line in lines]

    def close(self, deadline):
        if self.p.poll() is None:
            self.p.terminate()
            try:
                self.p.wait(timeout=max(.001, min(.5, deadline - time.monotonic())))
            except subprocess.TimeoutExpired:
                self.p.kill()
                try:
                    self.p.wait(timeout=max(.001, deadline - time.monotonic()))
                except subprocess.TimeoutExpired:
                    pass
        for stream in (self.p.stdout, self.p.stderr):
            if stream.closed:
                continue
            if stream.fileno() in self.fds:
                self.engine.selector.unregister(stream)
                self.fds.remove(stream.fileno())
            stream.close()
        self.finish_input()
        for name in list(self.files):
            fd = self.files.pop(name)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        return self.p.returncode is not None


class Engine:
    def __init__(self):
        self.live, self.commands, self.attempt = [], [], 0
        self.root = None
        self.used = {name: 0 for name in OUTPUT_LIMITS}
        self.selector = selectors.DefaultSelector()
        self.uncertain = False
        self.output_failed = False

    def charge(self, role, count):
        need(self.used[role] + count <= OUTPUT_LIMITS[role], "output-limit-" + role)
        self.used[role] += count

    def pump_until(self, predicate, deadline):
        while not predicate():
            need(time.monotonic() < deadline, "host-deadline")
            for key, _ in self.selector.select(min(.05, max(0, deadline - time.monotonic()))):
                child, name = key.data
                try:
                    raw = os.read(key.fd, 65536)
                except BlockingIOError:
                    continue
                if raw:
                    child.accept(name, raw)
                else:
                    self.selector.unregister(key.fileobj)
                    child.fds.remove(key.fd)
            for child in self.live:
                child.p.poll()

    def command(self, args, deadline, mutation=False):
        need(time.monotonic() < deadline, "host-deadline-before-command")
        previous_uncertainty = self.uncertain
        if mutation:
            self.uncertain = True
        child = Child(self, args, "control")
        record = {"argv": args, "child": child.number, "exit": None}
        self.commands.append(record)
        try:
            self.pump_until(child.done, deadline)
            record.update(exit=child.p.returncode, stdout=bytes(child.out).decode(), stderr=bytes(child.err).decode())
            need(child.p.returncode == 0, "docker-command-failed")
            if mutation:
                self.uncertain = previous_uncertainty
            return bytes(child.out)
        finally:
            if child.done():
                try:
                    child.close(deadline)
                except BaseException:
                    self.output_failed = True
                    raise

    def inspect(self, ident, deadline):
        return strict_json(self.command(["inspect", "--format", "{{json .}}", ident], deadline))

    def put(self, name, value):
        raw = encode(value, OUTPUT_LIMITS["control"])
        self.charge("control", len(raw))
        fd = os.open(self.root / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try:
            need(os.write(fd, raw) == len(raw), "receipt-short-write")
            if name == "inputs-manifest.json":
                before = os.fstat(fd)
                need(stat.S_ISREG(before.st_mode) and before.st_uid == 501 and before.st_nlink == 1,
                     "upload-manifest-custody")
                os.fchmod(fd, 0o644)  # Only the NONSECRET nonce/file-digest upload manifest.
                after = os.fstat(fd)
                need(stat.S_IMODE(after.st_mode) == 0o644 and after.st_size == len(raw) and
                     (before.st_dev, before.st_ino, before.st_uid, before.st_gid, before.st_nlink) ==
                     (after.st_dev, after.st_ino, after.st_uid, after.st_gid, after.st_nlink), "upload-manifest-mode")
            os.fsync(fd)
        finally:
            os.close(fd)
        directory = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    def close_children(self, deadline):
        closed = True
        for child in reversed(self.live):
            try:
                closed = child.close(deadline) and closed
            except BaseException:
                self.output_failed = True
                closed = child.p.returncode is not None and closed
        return closed


def read_admission(path, expected_profile="SG32-v1"):
    need(expected_profile in ("SG32-v1", "SG32-PREPARER-ONLY-v1"), "unknown-profile")
    raw = read_owned(path, 65536)
    cfg = strict_json(raw)
    need(type(cfg) is dict and cfg.get("profile") == expected_profile and cfg.get("owner_approved") is True and
         cfg.get("allow_recovery_hold") is True and cfg.get("one_attempt") is True, "INERT-owner-admission")
    need(cfg.get("inputs_ready") is True and type(cfg.get("files")) is dict and set(cfg["files"]) == INPUT_NAMES,
         "qualified-inputs-missing")
    qualification_raw = read_owned(cfg["qualification_path"], 1048576)
    need(digest(qualification_raw) == cfg["qualification_sha256"], "qualification-binding")
    qualification = strict_json(qualification_raw)
    need(qualification.get("status") == "QUALIFIED_CURRENT_CLI_AND_REBOUND_DRIVER" and
         qualification.get("source_commit") == "8afa7f5b365a56d55f116c8a0dfb0ac581c79000" and
         qualification.get("platform") == "linux-arm64-v8.0-cgo0" and
         type(qualification.get("build_exit")) is int and qualification.get("build_exit") == 0,
         "qualified-artifact-provenance")
    for name, entry in cfg["files"].items():
        need(type(entry) is dict and set(entry) == {"path", "sha256", "bytes"} and hex_value(entry["sha256"], 64) and
             type(entry["bytes"]) is int and 0 < entry["bytes"] <= 128 * 1024**2, "input-entry")
        body = read_owned(entry["path"], 128 * 1024**2)
        need(len(body) == entry["bytes"] and digest(body) == entry["sha256"], "input-bytes")
        if name in ("corsad", "c2driver"):
            static_elf(body)
            need(qualification.get(name + "_sha256") == entry["sha256"], "artifact-digest")
    need(qualification.get("driver_compiled_cli_sha256") == cfg["files"]["corsad"]["sha256"] and
         qualification.get("driver_source_reviewed") is True and qualification.get("tester_accepted") is True and
         qualification.get("reviewer_accepted") is True, "driver-closed-pin-or-review")
    need(digest(read_owned(__file__, 131072)) == cfg["host_sha256"] and
         digest(read_owned(WRAPPER, 32768)) == cfg["wrapper_sha256"], "controller-binding")
    return raw, cfg


def activate_driver(driver, activation, work):
    payload = encode(activation, 4096)
    need(work - time.monotonic() >= 125, "insufficient-work-and-close-budget")
    driver.send(payload); driver.finish_input()


def find_record(child, kind, nonce):
    records = [r for r in child.records() if type(r) is dict and r.get("kind") == kind and r.get("nonce") == nonce]
    need(len(records) <= 1, "duplicate-control-receipt")
    return records[0] if records else None


def cleanup(engine, nonce, ids, volumes, deadline):
    closed = engine.close_children(deadline) and not engine.uncertain
    try:
        listed = engine.command(["ps", "-a", "--no-trunc", "--filter", "label=corsa.sg32.owner=" + nonce,
                                 "--format", "{{.ID}}"], deadline).decode().splitlines()
        for ident in sorted(set(listed + ids)):
            need(hex_value(ident, 64), "cleanup-container-id")
            present = engine.command(["ps", "-a", "--no-trunc", "--filter", "id=" + ident,
                                      "--format", "{{.ID}}"], deadline).decode().splitlines()
            if not present:
                continue
            meta = engine.inspect(ident, deadline)
            labels = meta["Config"]["Labels"]
            role = labels.get("corsa.sg32.role")
            need(labels.get("corsa.sg32.owner") == nonce and role in ROLE_LIMITS and
                 meta["Name"] == "/corsa-d1-sg32-" + nonce + "-" + role, "cleanup-container-owner")
            engine.command(["rm", "-f", ident], deadline, mutation=True)
        own = engine.command(["volume", "ls", "--filter", "label=corsa.sg32.owner=" + nonce,
                              "--format", "{{.Name}}"], deadline).decode().splitlines()
        for name in sorted(set(own + volumes)):
            exists = engine.command(["volume", "ls", "--filter", "name=" + name,
                                     "--format", "{{.Name}}"], deadline).decode().splitlines()
            if name not in exists:
                continue
            meta = strict_json(engine.command(["volume", "inspect", "--format", "{{json .}}", name], deadline))
            role = (meta.get("Labels") or {}).get("corsa.sg32.role")
            need(role in ("inputs", "secret", "state") and
                 (meta.get("Labels") or {}).get("corsa.sg32.owner") == nonce and
                 name == volume_names(nonce)[role], "cleanup-volume-owner")
            engine.command(["volume", "rm", name], deadline, mutation=True)
        need(not engine.command(["ps", "-a", "--no-trunc", "--filter", "label=corsa.sg32.owner=" + nonce,
                                 "--format", "{{.ID}}"], deadline).strip(), "own-containers-remain")
        need(not engine.command(["volume", "ls", "--filter", "label=corsa.sg32.owner=" + nonce,
                                 "--format", "{{.Name}}"], deadline).strip(), "own-volumes-remain")
        for ident in ids:
            need(not engine.command(["ps", "-a", "--no-trunc", "--filter", "id=" + ident,
                                     "--format", "{{.ID}}"], deadline).strip(), "known-container-remains")
        for name in volumes:
            need(name not in engine.command(["volume", "ls", "--filter", "name=" + name,
                 "--format", "{{.Name}}"], deadline).decode().splitlines(), "known-volume-remains")
    except BaseException:
        closed = False
    return engine.close_children(deadline) and closed and not engine.uncertain


def run(admission_path, started_at=None):
    now = time.monotonic()
    began = now if started_at is None else started_at
    need(type(began) in (int, float) and math.isfinite(began) and 0 <= began <= now, "invalid-entry-origin")
    work, end = began + 180, began + 210
    admission_path = Path(admission_path)
    raw, cfg = read_admission(admission_path)  # Inert/missing inputs refuse before Docker/claim.
    need(time.monotonic() < work, "work-prefix-exhausted")
    need((os.getuid(), os.getgid()) == (501, 20), "controller-owner")
    parent = Path(cfg["run_parent"])
    need(parent.is_absolute() and parent.resolve(strict=True) == parent and len(str(parent)) <= 512,
         "run-parent-path")
    st = parent.stat()
    need(st.st_uid == 501 and stat.S_IMODE(st.st_mode) == 0o700, "run-parent-custody")
    space = os.statvfs(parent)
    need(space.f_bavail * space.f_frsize >= 10 * 1024**3 + sum(v["bytes"] for v in cfg["files"].values()), "free-space")
    owner = dict(line.split("=", 1) for line in read_owned(LOCK / "owner", 8192).decode().splitlines() if "=" in line)
    need(owner.get("name") == "d1-sg32" and owner.get("pgid") == str(os.getpgrp()), "own-heavy-group")
    engine = Engine()
    ids, volume_attempts = [], []
    nonce = secrets.token_hex(16)
    names = volume_names(nonce)
    claimed = False
    result = {"status": "NOT_STARTED", "measurement_qualified": False, "claim_attempted": False,
              "nonce": nonce, "physical_closure": "UNKNOWN", "keeper_exit_tail": "UNKNOWN"}
    try:
        endpoint = strict_json(engine.command(["context", "inspect", "--format", "{{json .Endpoints.docker.Host}}",
                                               "desktop-linux"], work))
        need(endpoint == ENDPOINT, "local-endpoint")
        engine.command(["info", "--format", "{{json .ServerVersion}}"], work)
        image = strict_json(engine.command(["image", "inspect", "--format", "{{json .}}", IMAGE], work))
        need(image["Id"] == IMAGE and image["Os"] == "linux" and image["Architecture"] == "arm64" and
             image["Config"].get("Volumes") in (None, {}), "image-platform-or-implicit-volume")
        engine.root = parent / ("sg32-" + nonce)
        engine.root.mkdir(mode=0o700)
        need(read_owned(admission_path, 65536) == raw, "admission-changed")
        claimed = result["claim_attempted"] = True
        claim = os.open(admission_path.with_name("ONE-ATTEMPT-CLAIM.json"),
                        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try:
            body = encode({"nonce": nonce, "admission_sha256": digest(raw), "retry": False}, 4096)
            need(os.write(claim, body) == len(body), "claim-short-write")
            os.fsync(claim)
        finally:
            os.close(claim)
        directory = os.open(admission_path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        result["status"] = "INCOMPLETE"
        for role, name in names.items():
            need(not engine.command(["volume", "ls", "--filter", "name=" + name, "--format", "{{.Name}}"], work).strip(),
                 "volume-name-not-fresh")
            volume_attempts.append(name)
            args = ["volume", "create", "--driver=local", "--label", "corsa.sg32.owner=" + nonce,
                    "--label", "corsa.sg32.role=" + role]
            if role == "secret":
                args += ["--opt", "type=tmpfs", "--opt", "device=tmpfs", "--opt", "o=size=1048576,mode=0700"]
            engine.command(args + [name], work, mutation=True)
            meta = strict_json(engine.command(["volume", "inspect", "--format", "{{json .}}", name], work))
            expected = {"type": "tmpfs", "device": "tmpfs", "o": "size=1048576,mode=0700"} if role == "secret" else None
            need(meta["Name"] == name and meta["Driver"] == "local" and
                 (meta.get("Options") == expected or (expected is None and meta.get("Options") == {})) and
                 meta["Labels"].get("corsa.sg32.owner") == nonce and meta["Labels"].get("corsa.sg32.role") == role,
                 "actual-volume")

        def create(role, service_id=None):
            ident = engine.command(create_argv(role, nonce, names, service_id, cfg["files"]["corsad"]["sha256"]),
                                   work, mutation=True).decode().strip()
            need(hex_value(ident, 64), "created-container-id")
            ids.append(ident)
            verify_created(engine.inspect(ident, work), role, nonce, names, service_id)
            return ident

        prep_id = create("preparer")
        manifest = {"nonce": nonce, "files": {k: {"sha256": v["sha256"], "bytes": v["bytes"]} for k, v in cfg["files"].items()}}
        engine.put("inputs-manifest.json", manifest)
        for name, entry in cfg["files"].items():
            before = read_owned(entry["path"], 128 * 1024**2)
            need(digest(before) == entry["sha256"], "upload-input-changed")
            engine.command(["cp", entry["path"], prep_id + ":/inputs/" + name], work, mutation=True)
            need(read_owned(entry["path"], 128 * 1024**2) == before, "upload-input-changed-after")
        engine.command(["cp", str(engine.root / "inputs-manifest.json"), prep_id + ":/inputs/manifest.json"], work, mutation=True)
        prep_deadline = min(work, time.monotonic() + 20)
        prep = Child(engine, ["start", "-a", "-i", prep_id], "control", True)
        prep.send(encode(manifest, 4096)); prep.finish_input()
        engine.pump_until(prep.done, prep_deadline)
        receipts = prep.records(True)
        need(prep.p.returncode == 0 and not prep.err and len(receipts) == 1 and
             receipts[0].get("kind") == "PREPARED" and receipts[0].get("nonce") == nonce and
             receipts[0].get("files") == manifest["files"], "preparer-receipt")
        prep_state = engine.inspect(prep_id, work)["State"]
        need(prep_state["Running"] is False and type(prep_state["ExitCode"]) is int and
             prep_state["ExitCode"] == 0 and prep_state["OOMKilled"] is False, "preparer-exit")
        engine.command(["rm", prep_id], work, mutation=True)
        need(not engine.command(["ps", "-a", "--no-trunc", "--filter", "id=" + prep_id,
             "--format", "{{.ID}}"], work).strip(), "preparer-not-removed")
        service_id = create("service")
        service = Child(engine, ["start", "-a", "-i", service_id], "service", True)
        engine.pump_until(lambda: find_record(service, "READY", nonce) is not None or service.done(),
                          min(work, time.monotonic() + 20))
        ready = find_record(service, "READY", nonce)
        need(ready is not None and service.p.poll() is None and hex_value(ready.get("identity"), 40) and
             type(ready.get("started_at")) is str and ready["started_at"].endswith("Z"), "service-ready")
        need(work - time.monotonic() >= 125, "insufficient-work-and-close-budget")
        driver_id = create("driver", service_id)
        driver = Child(engine, ["start", "-a", "-i", driver_id], "driver", True)
        activation = {"ready": True, "profile": "C2B32-v1", "nonce": nonce, "approval_sha256": digest(raw),
                      "cli_sha256": cfg["files"]["corsad"]["sha256"], "server_identity": ready["identity"],
                      "server_started_at": ready["started_at"], "work_millis": 80000}
        activate_driver(driver, activation, work)
        driver_deadline = min(work - 35, time.monotonic() + 90)
        phases = ("thirty-two-proven", "sixteen-survivors", "thirty-two-recovered", "zero-drained")
        for index, phase in enumerate(phases):
            engine.pump_until(lambda: len(driver.records()) > index or driver.done() or service.done(), driver_deadline)
            rows = driver.records()
            need(service.p.poll() is None and len(rows) > index and type(rows[index]) is dict and
                 rows[index].get("nonce") == nonce and rows[index].get("phase") == phase, "driver-phase-stream")
            service.send(encode({"kind": "CHECK", "nonce": nonce, "phase": phase}, 4096))
            engine.pump_until(lambda: any(p.get("kind") == "STATE_CHECK" and p.get("phase") == phase
                for p in service.records()) or service.done(), driver_deadline)
            need(service.p.poll() is None, "service-lost-during-work")
        engine.pump_until(lambda: driver.done() or service.done(), driver_deadline)
        need(driver.done() and driver.p.returncode == 0 and not driver.err and service.p.poll() is None, "driver-exit-or-service-loss")
        result["work"] = verify_driver(driver.records(True), nonce, ready["identity"])
        service.send(encode({"kind": "STOP", "nonce": nonce}, 4096))
        engine.pump_until(service.done, min(work, time.monotonic() + 30))
        need(service.p.returncode == 0 and not service.err, "service-nonnormal-exit")
        result["measurement"] = verify_service(service.records(True), nonce, cfg["files"]["corsad"]["sha256"])
        for ident in (service_id, driver_id):
            state = engine.inspect(ident, work)["State"]
            need(state["Running"] is False and type(state["ExitCode"]) is int and state["ExitCode"] == 0 and
                 state["OOMKilled"] is False, "runtime-exit-or-oom")
        result.update(status="SCOPED_SUCCESS", measurement_qualified=True)
    except (Fault, OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError) as error:
        result["error"] = {"type": type(error).__name__, "reason": str(error)[:128] if isinstance(error, Fault) else "operation-failed"}
        errors = []
        for child in engine.live:
            if child.role == "service":
                try:
                    errors.extend(p for p in child.records() if type(p) is dict and p.get("kind") == "ERROR")
                except (Fault, ValueError, TypeError):
                    result["service_protocol"] = "INCOMPLETE"
            if child.role in ("service", "driver"):
                result.setdefault("owned_cli_exits", []).append({"role": child.role, "pid": child.p.pid,
                    "exit": child.p.poll(), "scope": "before-failure-cleanup"})
        if any(p.get("status") == "FAIL" for p in errors):
            result["status"] = "FAIL"
        elif claimed:
            result["status"] = "INCOMPLETE"
    finally:
        closed = cleanup(engine, nonce, ids, volume_attempts, end) if claimed else engine.close_children(end)
        result["physical_closure"] = "KNOWN" if closed else "UNKNOWN"
        if not closed or engine.output_failed:
            result["measurement_qualified"] = False
            result["status"] = "INCOMPLETE" if closed else "RECOVERY_REQUIRED"
        if engine.root is not None:
            try:
                engine.put("commands.json", engine.commands)
                engine.put("result.json", result)
            except BaseException:
                result["measurement_qualified"] = False
                if closed:
                    result["status"] = "INCOMPLETE"
        if not closed:
            try:
                print(json.dumps({"status": "RECOVERY_REQUIRED", "run": str(engine.root)}), flush=True)
            except BaseException:
                pass
            while True:
                time.sleep(3600)
        engine.selector.close()
    print(json.dumps({"status": result["status"], "run": str(engine.root)}), flush=True)
    return 0 if result["measurement_qualified"] else 2


def main(argv=None, started_at=None):
    args = sys.argv[1:] if argv is None else argv
    need(len(args) == 1, "INERT-admission-path-required")
    return run(Path(args[0]), started_at=started_at)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (Fault, OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError) as error:
        print(json.dumps({"status": "NOT_STARTED", "reason": str(error)[:128] if isinstance(error, Fault) else "operation-failed"}))
        raise SystemExit(2)
