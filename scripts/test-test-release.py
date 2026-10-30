#!/usr/bin/env python3
"""Finite refusal/coverage controls; uses a stub GO, never the real toolchain."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().with_name("test-release.sh")
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
        self.assertEqual(result.stderr, f"DEFERRED to D1 (not PASS): {DEFERRED}\n")

    def test_tag_and_skip_flags_cannot_exclude_node_tests(self):
        node = MODULE + "/internal/core/node"
        kept = [node, MODULE + "/internal/core/service/filetransfer"]
        result, argv = self.run_scope("\n".join([DEFERRED, *kept]) + "\n", extra_env={
            "RELEASE_TAG": "v2.5.71", "GITHUB_REF": "refs/tags/v2.5.71",
            "CORSA_RELEASE_VERSION": "2.5.71", "CORSA_SKIP_D1_TESTS": "1",
        })
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(argv, ["test", "-count=1", "-timeout=15m", *kept])
        self.assertEqual(result.stderr, f"DEFERRED to D1 (not PASS): {DEFERRED}\n")

    def test_failed_inventory_never_runs_partial_scope(self):
        result, argv = self.run_scope(DEFERRED + "\n" + MODULE + "/cmd/corsa-node\n", list_exit=7)
        self.assertEqual(result.returncode, 7)
        self.assertIsNone(argv)

    def test_test_failure_is_preserved(self):
        result, argv = self.run_scope(DEFERRED + "\n" + MODULE + "/cmd/corsa-node\n", test_exit=9)
        self.assertEqual(result.returncode, 9)
        self.assertIsNotNone(argv)

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


if __name__ == "__main__":
    unittest.main()
