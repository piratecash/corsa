#!/usr/bin/env python3
"""Inspect and package CI candidates; print evidence, never execute or publish them."""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import plistlib
import re
import shutil
import stat
import struct
import subprocess
import tarfile
import zipfile

PLATFORMS = {
    "linux-amd64": ("linux", "amd64"),
    "darwin-arm64": ("darwin", "arm64"),
    "darwin-amd64": ("darwin", "amd64"),
    "windows-amd64": ("windows", "amd64"),
    "windows-arm64": ("windows", "arm64"),
}

RELEASE_ARTIFACTS = {
    "linux-amd64": "corsa-linux",
    "darwin-arm64": "corsa-macos-arm64",
    "darwin-amd64": "corsa-macos-amd64",
    "windows-amd64": "corsa-windows-amd64",
    "windows-arm64": "corsa-windows-arm64",
}

LINUX_DESKTOP_FILES = {
    "share/applications/corsa.desktop": Path("packaging/linux/corsa.desktop"),
    "share/icons/hicolor/256x256/apps/corsa.png": Path("assets/icons/png/app-icon-256.png"),
}


def digest(path):
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"not a regular candidate file: {path}")
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def check_header(path, system, arch):
    with path.open("rb") as stream:
        header = stream.read(4096)
    if system == "linux":
        if len(header) < 20 or header[:6] != b"\x7fELF\x02\x01" or struct.unpack_from("<H", header, 18)[0] != 62:
            raise ValueError(f"not Linux ELF64 amd64: {path}")
    elif system == "darwin":
        cpu = {"arm64": 0x100000C, "amd64": 0x1000007}[arch]
        if len(header) < 8 or header[:4] != b"\xcf\xfa\xed\xfe" or struct.unpack_from("<I", header, 4)[0] != cpu:
            raise ValueError(f"not thin Mach-O64 {arch}: {path}")
    else:
        if len(header) < 64 or header[:2] != b"MZ":
            raise ValueError(f"not Windows PE: {path}")
        offset = struct.unpack_from("<I", header, 60)[0]
        with path.open("rb") as stream:
            stream.seek(offset)
            pe = stream.read(26)
        machine = {"amd64": 0x8664, "arm64": 0xAA64}[arch]
        if len(pe) != 26 or pe[:4] != b"PE\0\0" or struct.unpack_from("<H", pe, 4)[0] != machine or struct.unpack_from("<H", pe, 24)[0] != 0x20B:
            raise ValueError(f"not PE32+ {arch}: {path}")


def inspect_binary(path, product, system, arch, source_sha):
    sha = digest(path)
    if system != "windows" and not path.stat().st_mode & stat.S_IXUSR:
        raise ValueError(f"candidate executable has no owner execute permission: {path}")
    check_header(path, system, arch)
    raw = subprocess.check_output(["go", "version", "-m", str(path)], text=True, encoding="utf-8", timeout=30)
    fields = {}
    package = None
    for line in raw.splitlines():
        parts = line.strip().split("\t")
        if len(parts) == 2 and parts[0] == "path":
            package = parts[1]
        if len(parts) == 2 and parts[0] == "build" and "=" in parts[1]:
            key, value = parts[1].split("=", 1)
            if key in fields:
                raise ValueError(f"duplicate candidate BuildInfo field {key}: {path}")
            fields[key] = value
    if package != f"github.com/piratecash/corsa/cmd/corsa-{product}":
        raise ValueError(f"unexpected executable package: {path}")
    expected_fields = {"GOOS": system, "GOARCH": arch, "vcs.revision": source_sha, "vcs.modified": "false"}
    if product == "desktop" and system in ("linux", "darwin"):
        expected_fields["CGO_ENABLED"] = "1"
    if product == "desktop" and system == "linux":
        expected_fields["GOAMD64"] = "v1"
    for key, expected in expected_fields.items():
        if fields.get(key) != expected:
            raise ValueError(f"candidate {path}: {key}={fields.get(key)!r}, expected {expected!r}")
    return {"path": str(path), "bytes": path.stat().st_size, "sha256": sha, "go_build_info": raw}


def verify_checkout(source_sha):
    if not re.fullmatch(r"[0-9a-f]{40}", source_sha):
        raise ValueError("a full source commit is required")
    actual = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True, encoding="utf-8").strip()
    if actual != source_sha:
        raise ValueError("checkout does not match the requested commit")
    subprocess.run(["git", "diff", "--quiet", "HEAD", "--"], check=True)
    if Path(".env").exists():
        raise ValueError("candidate checkout must not contain a private .env")


def archive_extension(system):
    return ".zip" if system == "windows" else ".tar.bz2"


def verify_archive(archive, platform, binaries, resources):
    system = PLATFORMS[platform][0]
    if system == "windows":
        names = [f"corsa-{product}-{platform}.exe" for product in ("node", "desktop")]
        with zipfile.ZipFile(archive) as stream:
            members = stream.infolist()
            if sorted(member.filename for member in members) != sorted(names):
                raise ValueError("Windows archive must contain exactly the daemon and Desktop")
            for name, binary in zip(names, binaries, strict=True):
                member = stream.getinfo(name)
                with stream.open(member) as file:
                    if member.file_size != binary["bytes"] or hashlib.file_digest(file, "sha256").hexdigest() != binary["sha256"]:
                        raise ValueError(f"packed executable differs from the inspected build: {name}")
        return

    if system == "darwin":
        names = [f"corsa-node-{platform}", f"Corsa-{platform}.app/Contents/MacOS/Corsa"]
    else:
        names = [f"corsa-{platform}/bin/corsa-{product}" for product in ("node", "desktop")]
    files = dict(zip(names, binaries, strict=True)) | resources
    directories = {str(parent) for name in files for parent in PurePosixPath(name).parents if parent.parts}
    seen = set()
    with tarfile.open(archive, "r|bz2") as stream:
        for member in stream:
            name = member.name
            if name in seen:
                raise ValueError(f"duplicate path in the candidate archive: {name}")
            seen.add(name)
            if name in directories and member.isdir():
                continue
            expected = files.get(name)
            if expected is None:
                raise ValueError(f"unexpected path or type in the candidate archive: {name}")
            executable = name in names
            kind = "executable" if executable else "resource"
            if (not member.isfile() or member.size != expected["bytes"]
                    or executable and not member.mode & stat.S_IXUSR):
                raise ValueError(f"packed {kind} has unexpected metadata: {name}")
            with stream.extractfile(member) as file:
                if hashlib.file_digest(file, "sha256").hexdigest() != expected["sha256"]:
                    raise ValueError(f"packed {kind} differs from the inspected build: {name}")
    if missing := (set(files) | directories) - seen:
        raise ValueError(f"candidate archive is missing paths: {', '.join(sorted(missing))}")


def resource_receipt(path):
    return {"sha256": digest(path), "bytes": path.stat().st_size}


def release_run_id():
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    if not re.fullmatch(r"[1-9][0-9]*", run_id):
        raise ValueError("a GitHub workflow run ID is required for release archives")
    return run_id


def read_release_receipt(path):
    def unique_fields(pairs):
        fields = dict(pairs)
        if len(fields) != len(pairs):
            raise ValueError(f"duplicate field in release archive receipt: {path.name}")
        return fields

    with path.open("rb") as stream:
        raw = stream.read(4097)
    if not 0 < len(raw) <= 4096:
        raise ValueError(f"invalid release archive receipt size: {path.name}")
    fields = json.loads(raw.decode("utf-8"), object_pairs_hook=unique_fields)
    if type(fields) is not dict or type(fields.get("bytes")) is not int:
        raise ValueError(f"invalid release archive receipt fields: {path.name}")
    return fields


def report(platform, source_sha, *, release_archive=False):
    run_id = release_run_id() if release_archive else None
    verify_checkout(source_sha)
    config = Path("internal/core/config/config.go").read_text(encoding="utf-8")
    parts = []
    for component in ("Major", "Minor", "Build"):
        matches = re.findall(rf"^\s*ClientVersion{component}\s*=\s*(\d+)\s*$", config, re.MULTILINE)
        if len(matches) != 1:
            raise ValueError("ambiguous source release version")
        parts.append(matches[0])
    system, arch = PLATFORMS[platform]
    suffix = ".exe" if system == "windows" else ""
    paths = [Path("dist") / f"corsa-{product}-{platform}{suffix}" for product in ("node", "desktop")]
    binaries = [inspect_binary(path, product, system, arch, source_sha)
                for path, product in zip(paths, ("node", "desktop"))]
    out = Path("release" if release_archive else "dist/candidate")
    out.mkdir(parents=True, exist_ok=False)
    archive_name = f"corsa-{platform}" if release_archive else f"candidate-corsa-{platform}-{source_sha}"
    archive = out / f"{archive_name}{archive_extension(system)}"
    bundle_info = None
    resources = {}
    if system == "windows":
        with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as stream:
            for path in paths:
                stream.write(path, path.name)
    else:
        with tarfile.open(archive, "w:bz2") as stream:
            if system == "darwin":
                bundle = Path("dist") / f"Corsa-{platform}.app"
                if bundle.is_symlink() or any(path.is_symlink() for path in bundle.rglob("*")):
                    raise ValueError("candidate app contains a symlink")
                if digest(bundle / "Contents/MacOS/Corsa") != binaries[1]["sha256"]:
                    raise ValueError("bundled Desktop differs from the built executable")
                if not (bundle / "Contents/MacOS/Corsa").stat().st_mode & stat.S_IXUSR:
                    raise ValueError("bundled Desktop has no owner execute permission")
                plist = (bundle / "Contents/Info.plist").read_bytes()
                plist_sha = hashlib.sha256(plist).hexdigest()
                if plist_sha != digest(Path("packaging/macos/Info.plist")):
                    raise ValueError("unexpected macOS bundle Info.plist bytes")
                info = plistlib.loads(plist)
                schemes = [scheme for row in info.get("CFBundleURLTypes", []) for scheme in row.get("CFBundleURLSchemes", [])]
                required = {"CFBundleIdentifier": "chat.corsa", "CFBundleExecutable": "Corsa",
                            "CFBundlePackageType": "APPL", "CFBundleIconFile": "app-icon.icns"}
                if any(info.get(key) != value for key, value in required.items()) or "corsa" not in schemes:
                    raise ValueError("unexpected macOS bundle identity or URL scheme")
                icon = resource_receipt(Path("assets/icons/app-icon.icns"))
                if digest(bundle / "Contents/Resources/app-icon.icns") != icon["sha256"]:
                    raise ValueError("unexpected macOS bundle icon")
                resources = {
                    f"{bundle.name}/Contents/Info.plist": {"sha256": plist_sha, "bytes": len(plist)},
                    f"{bundle.name}/Contents/Resources/app-icon.icns": icon,
                }
                bundle_info = {"identifier": info["CFBundleIdentifier"], "schemes": schemes}
                stream.add(paths[0], arcname=paths[0].name)
                stream.add(bundle, arcname=bundle.name)
            else:
                root = out / f"corsa-{platform}"
                resources = {f"{root.name}/{target}": resource_receipt(source)
                             for target, source in LINUX_DESKTOP_FILES.items()}
                inputs = {"bin/corsa-node": paths[0], "bin/corsa-desktop": paths[1]} | LINUX_DESKTOP_FILES
                for target, source in inputs.items():
                    destination = root / target
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(source, destination)
                    destination.chmod(0o755 if target.startswith("bin/") else 0o644)
                stream.add(root, arcname=root.name)
    verify_archive(archive, platform, binaries, resources)
    receipt = {"commit": source_sha, "platform": platform, "bytes": archive.stat().st_size,
               "sha256": digest(archive)}
    if release_archive:
        with archive.with_suffix(archive.suffix + ".json").open("x", encoding="utf-8") as stream:
            json.dump(dict(receipt, run_id=run_id), stream, sort_keys=True)
    return {"kind": "candidate build validation; no publication or binary execution",
            "commit": source_sha, "platform": platform, "source_version": ".".join(parts),
            "binaries": binaries, "bundle": bundle_info,
            "archive": {"path": str(archive), "bytes": receipt["bytes"], "sha256": receipt["sha256"]}}


def collect_release(source_sha):
    run_id = release_run_id()
    verify_checkout(source_sha)
    inbox = Path("release-artifacts")
    if inbox.is_symlink() or not inbox.is_dir() or {p.name for p in inbox.iterdir()} != set(RELEASE_ARTIFACTS.values()):
        raise ValueError("unexpected release artifact groups")
    verified = {}
    for platform, group in RELEASE_ARTIFACTS.items():
        directory = inbox / group
        archive = directory / f"corsa-{platform}{archive_extension(PLATFORMS[platform][0])}"
        receipt = archive.with_suffix(archive.suffix + ".json")
        if (directory.is_symlink() or not directory.is_dir() or
                set(directory.iterdir()) != {archive, receipt} or receipt.is_symlink() or not receipt.is_file()):
            raise ValueError(f"unexpected files in release artifact group: {group}")
        sha = digest(archive)
        expected = {"commit": source_sha, "platform": platform, "bytes": archive.stat().st_size,
                    "sha256": sha, "run_id": run_id}
        if read_release_receipt(receipt) != expected:
            raise ValueError(f"release archive does not match its build receipt: {archive.name}")
        verified[archive] = sha

    destination = Path("dist")
    destination.mkdir(exist_ok=False)
    for archive, sha in verified.items():
        target = destination / archive.name
        shutil.copyfile(archive, target)
        if digest(target) != sha:
            raise ValueError(f"release archive changed while collecting: {archive.name}")
    return {"commit": source_sha, "run_id": run_id,
            "archives": {path.name: sha for path, sha in verified.items()}}


def verify_release_ref(source_sha):
    ref = os.environ.get("GITHUB_REF", "")
    if not re.fullmatch(r"refs/tags/v[0-9]+\.[0-9]+\.[0-9]+", ref):
        raise ValueError("publication requires a version tag ref")
    peeled = ref + "^{}"
    try:
        output = subprocess.check_output(
            ["git", "ls-remote", "--exit-code", "origin", ref, peeled],
            text=True, encoding="utf-8", timeout=30,
            env=dict(os.environ, GIT_TERMINAL_PROMPT="0"),
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        raise ValueError("could not verify the live release tag") from exc
    refs = {}
    for line in output.splitlines():
        parts = line.split("\t")
        if len(parts) != 2 or not re.fullmatch(r"[0-9a-f]{40}", parts[0]):
            raise ValueError("invalid release tag response")
        sha, name = parts
        if name not in {ref, peeled} or name in refs:
            raise ValueError("unexpected or duplicate release tag response")
        refs[name] = sha
    if ref not in refs or refs.get(peeled, refs[ref]) != source_sha:
        raise ValueError("live release tag no longer points to the checked commit")
    return ref


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--platform", choices=PLATFORMS)
    parser.add_argument("--source-sha", required=True)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--release-archive", action="store_true", help="write the checked archive under its release asset name")
    mode.add_argument("--collect-release", action="store_true", help="verify downloaded release archives and collect them in dist")
    args = parser.parse_args()
    if args.collect_release:
        if args.platform:
            parser.error("--collect-release verifies all platforms")
        result = collect_release(args.source_sha)
        result["verified_ref"] = verify_release_ref(args.source_sha)
    else:
        if not args.platform:
            parser.error("--platform is required when packaging an archive")
        result = report(args.platform, args.source_sha, release_archive=args.release_archive)
    print(json.dumps(result, indent=2))
