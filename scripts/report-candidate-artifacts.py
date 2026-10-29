#!/usr/bin/env python3
"""Inspect and package CI candidates; print evidence, never execute or publish them."""
import argparse
import hashlib
import json
from pathlib import Path
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
    raw = subprocess.check_output(["go", "version", "-m", str(path)], text=True, timeout=30)
    fields = {}
    package = None
    for line in raw.splitlines():
        parts = line.strip().split("\t")
        if len(parts) == 2 and parts[0] == "path":
            package = parts[1]
        if len(parts) == 2 and parts[0] == "build" and "=" in parts[1]:
            key, value = parts[1].split("=", 1)
            fields[key] = value
    if package != f"github.com/piratecash/corsa/cmd/corsa-{product}":
        raise ValueError(f"unexpected executable package: {path}")
    for key, expected in {"GOOS": system, "GOARCH": arch, "vcs.revision": source_sha, "vcs.modified": "false"}.items():
        if fields.get(key) != expected:
            raise ValueError(f"candidate {path}: {key}={fields.get(key)!r}, expected {expected!r}")
    return {"path": str(path), "bytes": path.stat().st_size, "sha256": sha, "go_build_info": raw}


def report(platform, source_sha):
    if not re.fullmatch(r"[0-9a-f]{40}", source_sha):
        raise ValueError("a full source commit is required")
    actual = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    if actual != source_sha:
        raise ValueError("checkout does not match the requested commit")
    subprocess.run(["git", "diff", "--quiet", "HEAD", "--"], check=True)
    if Path(".env").exists():
        raise ValueError("candidate checkout must not contain a private .env")
    config = Path("internal/core/config/config.go").read_text()
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
    out = Path("dist/candidate")
    out.mkdir(parents=True, exist_ok=False)
    bundle_info = None
    if system == "windows":
        archive = out / f"candidate-corsa-{platform}-{source_sha}.zip"
        with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as stream:
            for path in paths:
                stream.write(path, path.name)
    else:
        archive = out / f"candidate-corsa-{platform}-{source_sha}.tar.bz2"
        with tarfile.open(archive, "w:bz2") as stream:
            if system == "darwin":
                bundle = Path("dist") / f"Corsa-{platform}.app"
                if bundle.is_symlink() or any(path.is_symlink() for path in bundle.rglob("*")):
                    raise ValueError("candidate app contains a symlink")
                if digest(bundle / "Contents/MacOS/Corsa") != binaries[1]["sha256"]:
                    raise ValueError("bundled Desktop differs from the built executable")
                if not (bundle / "Contents/MacOS/Corsa").stat().st_mode & stat.S_IXUSR:
                    raise ValueError("bundled Desktop has no owner execute permission")
                with (bundle / "Contents/Info.plist").open("rb") as file:
                    info = plistlib.load(file)
                schemes = [scheme for row in info.get("CFBundleURLTypes", []) for scheme in row.get("CFBundleURLSchemes", [])]
                required = {"CFBundleIdentifier": "chat.corsa", "CFBundleExecutable": "Corsa",
                            "CFBundlePackageType": "APPL", "CFBundleIconFile": "app-icon.icns"}
                if any(info.get(key) != value for key, value in required.items()) or "corsa" not in schemes:
                    raise ValueError("unexpected macOS bundle identity or URL scheme")
                if digest(bundle / "Contents/Resources/app-icon.icns") != digest(Path("assets/icons/app-icon.icns")):
                    raise ValueError("unexpected macOS bundle icon")
                bundle_info = {"identifier": info["CFBundleIdentifier"], "schemes": schemes}
                stream.add(paths[0], arcname=paths[0].name)
                stream.add(bundle, arcname=bundle.name)
            else:
                root = out / f"corsa-{platform}"
                for target, source in (("bin/corsa-node", paths[0]), ("bin/corsa-desktop", paths[1]),
                                       ("share/applications/corsa.desktop", Path("packaging/linux/corsa.desktop")),
                                       ("share/icons/hicolor/256x256/apps/corsa.png", Path("assets/icons/png/app-icon-256.png"))):
                    destination = root / target
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(source, destination)
                    destination.chmod(0o755 if target.startswith("bin/") else 0o644)
                stream.add(root, arcname=root.name)
    return {"kind": "candidate build validation; no publication or binary execution",
            "commit": source_sha, "platform": platform, "source_version": ".".join(parts),
            "binaries": binaries, "bundle": bundle_info,
            "archive": {"path": str(archive), "bytes": archive.stat().st_size, "sha256": digest(archive)}}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--platform", choices=PLATFORMS, required=True)
    parser.add_argument("--source-sha", required=True)
    args = parser.parse_args()
    print(json.dumps(report(args.platform, args.source_sha), indent=2))
