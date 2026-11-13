#!/usr/bin/env python3
"""Exercise release test selection and Checks log binding using stubs."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().with_name("test-release.sh")
CHECKS_SCRIPT = SCRIPT.with_name("run-ci-test-step.sh")
MODULE = "github.com/piratecash/corsa"
DEFERRED = MODULE + "/internal/overlaysim"


class ReleaseTestScope(unittest.TestCase):
    def run_scope(self, inventory, list_exit=0, test_exit=0, extra_env=None):
        with tempfile.TemporaryDirectory(prefix="corsa-release-scope-") as directory:
            root = Path(directory)
            stub = root / "go"
            stub.write_text(
                "#!/bin/sh\n"
                'if [ "$1" = list ]; then cat "$INVENTORY"; exit "$LIST_EXIT"; fi\n'
                'printf "%s\\n" "$@" > "$ARGUMENTS"\n'
                'exit "$TEST_EXIT"\n'
            )
            stub.chmod(0o700)
            data = root / "inventory"
            data.write_text(inventory)
            arguments = root / "arguments"
            env = dict(os.environ, GO=str(stub), INVENTORY=str(data),
                       ARGUMENTS=str(arguments), LIST_EXIT=str(list_exit), TEST_EXIT=str(test_exit))
            env.update(extra_env or {})
            result = subprocess.run(["bash", str(SCRIPT)], cwd=root, env=env, capture_output=True,
                                    text=True, timeout=5, check=False)
            argv = arguments.read_text().splitlines() if arguments.exists() else None
            return result, argv

    def test_only_exact_package_is_deferred(self):
        kept = [MODULE, MODULE + "/cmd/corsa-node", MODULE + "/cmd/corsa-desktop",
                MODULE + "/internal/overlaysimulator", DEFERRED + "_extra", DEFERRED + "/subpkg"]
        result, argv = self.run_scope("\n".join(kept[:2] + [DEFERRED] + kept[2:]) + "\n")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(argv, ["test", "-count=1", "-timeout=15m", *kept])
        self.assertEqual(result.stderr.splitlines(), [
            f"DEFERRED to D1 (not PASS): {DEFERRED}",
            f"Release test scope: {len(kept)} packages selected",
            *(f"Release test package: {package}" for package in kept),
        ])

    def test_tag_and_skip_flags_cannot_exclude_node_tests(self):
        node = MODULE + "/internal/core/node"
        kept = [node, MODULE + "/internal/core/service/filetransfer"]
        result, argv = self.run_scope("\n".join([DEFERRED, *kept]) + "\n", extra_env={
            "RELEASE_TAG": "v2.5.71", "GITHUB_REF": "refs/tags/v2.5.71",
            "CORSA_RELEASE_VERSION": "2.5.71", "CORSA_SKIP_D1_TESTS": "1",
        })
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(argv, ["test", "-count=1", "-timeout=15m", *kept])
        self.assertEqual(result.stderr.splitlines()[0], f"DEFERRED to D1 (not PASS): {DEFERRED}")

    def test_failed_inventory_never_runs_partial_scope(self):
        result, argv = self.run_scope(DEFERRED + "\n" + MODULE + "/cmd/corsa-node\n", list_exit=7)
        self.assertEqual(result.returncode, 7)
        self.assertIsNone(argv)

    def test_test_failure_is_preserved(self):
        result, argv = self.run_scope(DEFERRED + "\n" + MODULE + "/cmd/corsa-node\n", test_exit=9)
        self.assertEqual(result.returncode, 9)
        self.assertIsNotNone(argv)

    def test_refactoring_archives_refuse_before_running_tests(self):
        for suffix in ["/docs/refactoring", "/docs/refactoring/dht/logs/run/sources/old"]:
            with self.subTest(suffix=suffix):
                result, argv = self.run_scope(
                    "\n".join([DEFERRED, MODULE + "/cmd/corsa-node", MODULE + suffix]) + "\n"
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIsNone(argv)
                self.assertIn("local refactoring archives", result.stderr)

    def test_archive_refusal_does_not_exclude_other_documentation_packages(self):
        kept = [MODULE + "/docs/refactoring_extra", MODULE + "/docs/examples"]
        result, argv = self.run_scope("\n".join([DEFERRED, *kept]) + "\n")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(argv, ["test", "-count=1", "-timeout=15m", *kept])

    def test_ambiguous_or_empty_inventory_refuses(self):
        peer = MODULE + "/cmd/corsa-node"
        cases = ["", DEFERRED + "\n", peer + "\n", DEFERRED + "\n" + DEFERRED + "\n" + peer,
                 DEFERRED + "\n" + peer + "\n" + peer, DEFERRED + "\n\n" + peer,
                 DEFERRED + "\n" + peer + "\r\n", DEFERRED + "\n" + peer + " space",
                 DEFERRED + "\nexample.org/foreign", DEFERRED + "\n" + MODULE + "/a/../b",
                 DEFERRED + "\n" + MODULE + "/a//b", DEFERRED + "\n" + MODULE + "/a/.used;exit"]
        for inventory in cases:
            with self.subTest(inventory=inventory):
                result, argv = self.run_scope(inventory)
                self.assertNotEqual(result.returncode, 0)
                self.assertIsNone(argv)


class ChecksTestStep(unittest.TestCase):
    sha = "a" * 40

    def run_step(self, target, make_exit=0, **overrides):
        with tempfile.TemporaryDirectory(prefix="corsa-ci-test-step-") as directory:
            root = Path(directory)
            stubs = {
                "git": '#!/bin/sh\n[ "$*" = "rev-parse HEAD" ] || exit 2\nprintf "%s\\n" "$CHECKOUT_SHA"\n',
                "make": '#!/bin/sh\nprintf "make %s\\n" "$*"\nexit "$MAKE_EXIT"\n',
            }
            for name, source in stubs.items():
                stub = root / name
                stub.write_text(source)
                stub.chmod(0o700)
            env = dict(os.environ, PATH=f"{root}{os.pathsep}{os.environ['PATH']}",
                       CHECKOUT_SHA=self.sha, GITHUB_SHA=self.sha, GITHUB_RUN_ID="100",
                       GITHUB_RUN_ATTEMPT="2", MAKE_EXIT=str(make_exit))
            env.update(overrides)
            return subprocess.run(["bash", str(CHECKS_SCRIPT), target], cwd=root, env=env,
                                  capture_output=True, text=True, timeout=5, check=False)

    def test_each_target_binds_its_own_log(self):
        for target in ("test-release", "test-s6-observation", "test-cgo"):
            with self.subTest(target=target):
                result = self.run_step(target)
                binding = f"{target} {self.sha} 100 2"
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout.splitlines(), [
                    f"S6 test begin {binding}", f"make {target}", f"S6 test end {binding}",
                ])

    def test_make_failure_has_no_end_marker(self):
        result = self.run_step("test-release", make_exit=9)
        self.assertEqual(result.returncode, 9)
        self.assertEqual(result.stdout.splitlines(), [
            f"S6 test begin test-release {self.sha} 100 2", "make test-release",
        ])

    def test_invalid_target_or_attempt_is_refused_before_make(self):
        for target, overrides in (
            ("lint", {}),
            ("test-release", {"GITHUB_SHA": "b" * 40}),
            ("test-release", {"GITHUB_RUN_ID": "0"}),
            ("test-release", {"GITHUB_RUN_ATTEMPT": "0"}),
            ("test-release", {"GITHUB_RUN_ATTEMPT": ""}),
        ):
            with self.subTest(target=target, overrides=overrides):
                result = self.run_step(target, **overrides)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, "")


if __name__ == "__main__":
    unittest.main()
