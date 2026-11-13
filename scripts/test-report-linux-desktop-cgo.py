"""Controls for the Linux Desktop CGO receipt without starting a compiler."""

import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("report-linux-desktop-cgo.py")
SPEC = importlib.util.spec_from_file_location("desktop_cgo_report", SCRIPT)
REPORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REPORT)
SHA = "a" * 40
LINUX = ("Linux", "x86_64")


class DesktopCGOControls(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        for name in ("Makefile", "go.mod", "egl.pc", "xkbcommon.pc"):
            (self.root / name).write_text(f"fixture {name}", encoding="utf-8")
        self.go = {"GOOS": "linux", "GOARCH": "amd64", "GOAMD64": "v1", "CGO_ENABLED": "1",
                   "GOVERSION": "go1.27.3", "CC": "gcc", "CXX": "g++"}
        self.responses = {
            ("git", "rev-parse", "HEAD"): SHA,
            ("git", "status", "--porcelain"): "",
            ("gcc", "-dumpmachine"): "x86_64-linux-gnu",
            ("gcc", "--version"): "gcc fixture\nsecond line",
            ("gcc", "-print-sysroot"): "",
            ("pkg-config", "--version"): "2.3.0",
        }
        for name in ("egl", "xkbcommon"):
            self.responses["pkg-config", "--modversion", name] = "1.0"
            self.responses["pkg-config", "--variable=pcfiledir", name] = str(self.root)
        self.selected = "xkbcommon\negl\nxkbcommon\n"

    def run_command(self, *argv):
        if argv == ("go", "env", "-json", *REPORT.GO_ENV):
            return json.dumps(self.go)
        if argv == ("go", "list", "-mod=readonly", "-deps", "-f", REPORT.PKG_CONFIG_FORMAT, REPORT.TARGET):
            return self.selected
        return self.responses[argv]

    def collect(self, host=LINUX):
        return REPORT.collect(SHA, root=self.root, run=self.run_command, host=host)

    def test_receipt_uses_selected_pc_files_and_exact_checkout(self):
        value = self.collect()
        self.assertEqual(value["source_sha"], SHA)
        self.assertEqual(value["compiler_sysroot"], "")
        self.assertEqual(value["compiler_version"], "gcc fixture")
        self.assertEqual(list(value["pkg_config_modules"]), ["egl", "xkbcommon"])
        record = value["pkg_config_modules"]["egl"]
        self.assertEqual(record["pc_sha256"], hashlib.sha256((self.root / "egl.pc").read_bytes()).hexdigest())
        self.assertEqual(record["pc_file"], str(self.root / "egl.pc"))

    def test_wrong_checkout_or_platform_refused(self):
        with self.assertRaisesRegex(ValueError, "Linux x86_64"):
            self.collect(("Darwin", "arm64"))
        self.responses["git", "rev-parse", "HEAD"] = "b" * 40
        with self.assertRaisesRegex(ValueError, "SHA differs"):
            self.collect()

    def test_dirty_checkout_or_dotenv_refused(self):
        self.responses["git", "status", "--porcelain"] = " M Makefile"
        with self.assertRaisesRegex(ValueError, "clean checkout"):
            self.collect()
        self.responses["git", "status", "--porcelain"] = ""
        (self.root / ".env").symlink_to(self.root / "missing")
        with self.assertRaisesRegex(ValueError, "clean checkout"):
            self.collect()

    def test_disabled_cgo_and_non_amd64_compiler_refused(self):
        self.go["CGO_ENABLED"] = "0"
        with self.assertRaisesRegex(ValueError, "disabled CGO"):
            self.collect()
        self.go["CGO_ENABLED"] = "1"
        self.responses["gcc", "-dumpmachine"] = "aarch64-linux-gnu"
        with self.assertRaisesRegex(ValueError, "does not target"):
            self.collect()

    def test_missing_pc_file_and_invalid_pkg_name_refused(self):
        self.responses["pkg-config", "--variable=pcfiledir", "egl"] = str(self.root / "missing")
        with self.assertRaisesRegex(ValueError, "source file missing"):
            self.collect()
        self.selected = "--define-variable=test=secret"
        with self.assertRaisesRegex(ValueError, "invalid Desktop"):
            self.collect()


if __name__ == "__main__":
    unittest.main(verbosity=2)
