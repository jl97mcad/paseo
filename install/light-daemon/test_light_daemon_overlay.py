"""Apply/verify/revert rules of light_daemon_overlay.py on synthetic files (no install needed)."""
import contextlib
import importlib.util
import io
import json
import os
import pathlib
import tempfile
import unittest
from unittest import mock

HERE = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("light_daemon_overlay", HERE / "light_daemon_overlay.py")
light_daemon_overlay = importlib.util.module_from_spec(spec)
spec.loader.exec_module(light_daemon_overlay)

REPLACED = {
    "dist/server/server/workspace-git-service.js": "// stock workspace git service\n",
    "dist/server/server/bootstrap.js": "// stock bootstrap\n",
}
ADDED = "dist/server/server/workspace-archive.js"


def patched_text(rel):
    return "// light daemon overlay\n" + REPLACED[rel]


class OverlayFixture(unittest.TestCase):
    version = "0.15.3"
    package = "@getpaseo/server"

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = pathlib.Path(self.tmp.name)
        self.server = root / "server"
        self.overlay = root / "overlay"
        files = [
            {"path": rel, "stockSha256": light_daemon_overlay.sha_bytes(text.encode()),
             "patchedSha256": light_daemon_overlay.sha_bytes(patched_text(rel).encode())}
            for rel, text in REPLACED.items()
        ]
        files.append({"path": ADDED, "stockSha256": None,
                      "patchedSha256": light_daemon_overlay.sha_bytes(b"// overlay only\n")})
        self.write_manifest(files)
        for rel, text in REPLACED.items():
            self.write(self.server, rel, text.encode())
            self.write(self.overlay, rel, patched_text(rel).encode())
        self.write(self.overlay, ADDED, b"// overlay only\n")
        self.write(self.server, "package.json",
                   json.dumps({"name": self.package, "version": self.version}).encode())
        for rel in light_daemon_overlay.CONTROL_PATCH_FILES:
            if rel not in REPLACED:
                self.write(self.server, rel, ("// stock " + rel + "\n").encode())

    def tearDown(self):
        self.tmp.cleanup()

    def write(self, base, rel, data, mode=0o644):
        path = base / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        os.chmod(path, mode)

    def write_manifest(self, files):
        self.write(self.overlay, "MANIFEST.json", json.dumps({
            "package": self.package, "version": self.version, "files": files,
        }).encode())

    def run_tool(self, action, *extra):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = light_daemon_overlay.main(
                [action, "--server", str(self.server), "--overlay", str(self.overlay), *extra])
        return code, out.getvalue()

    def run_refused(self, action, *extra):
        out = io.StringIO()
        with contextlib.redirect_stdout(out), self.assertRaises(SystemExit) as caught:
            light_daemon_overlay.main(
                [action, "--server", str(self.server), "--overlay", str(self.overlay), *extra])
        return out.getvalue() + str(caught.exception.code)

    def byte_map(self):
        return {str(path.relative_to(self.server)): path.read_bytes()
                for path in sorted(self.server.rglob("*")) if path.is_file()}

    def backups(self):
        return sorted(path.name for path in self.server.rglob("*" + light_daemon_overlay.BACKUP_SUFFIX))


class RoundTrip(OverlayFixture):
    def test_apply_reapply_status_verify_revert(self):
        code, output = self.run_tool("status")
        self.assertEqual(code, 0)
        self.assertIn("overall: stock", output)

        code, _ = self.run_tool("apply")
        self.assertEqual(code, 0)
        for rel in REPLACED:
            self.assertEqual((self.server / rel).read_text(), patched_text(rel))
            self.assertEqual((self.server / (rel + light_daemon_overlay.BACKUP_SUFFIX)).read_text(),
                             REPLACED[rel])
        self.assertEqual((self.server / ADDED).read_text(), "// overlay only\n")
        for rel, stock in REPLACED.items():
            self.assertEqual(os.stat(self.server / rel).st_mode & 0o777, 0o644)

        code, output = self.run_tool("status")
        self.assertEqual(code, 0)
        self.assertIn("overall: patched", output)
        self.assertEqual(self.run_tool("verify")[0], 0)

        before = self.byte_map()
        self.assertEqual(self.run_tool("apply")[0], 0)
        self.assertEqual(self.byte_map(), before)  # idempotent: nothing rewritten

        code, _ = self.run_tool("revert")
        self.assertEqual(code, 0)
        for rel, stock in REPLACED.items():
            self.assertEqual((self.server / rel).read_text(), stock)
        self.assertFalse((self.server / ADDED).exists())
        self.assertEqual(self.run_tool("status")[1].splitlines()[-1], "overall: stock")
        self.run_refused("verify")

    def test_reapply_after_revert_uses_the_existing_backup(self):
        self.assertEqual(self.run_tool("apply")[0], 0)
        self.assertEqual(self.run_tool("revert")[0], 0)
        backups = self.backups()
        self.assertEqual(len(backups), len(REPLACED))
        self.assertEqual(self.run_tool("apply")[0], 0)
        self.assertEqual(self.backups(), backups)
        self.assertEqual((self.server / ADDED).read_text(), "// overlay only\n")

    def test_mixed_overlay_reports_mixed_and_round_trips(self):
        self.assertEqual(self.run_tool("apply")[0], 0)
        self.assertEqual(self.run_tool("revert")[0], 0)
        self.write(self.server, "dist/server/server/bootstrap.js", patched_text("dist/server/server/bootstrap.js").encode())
        code, output = self.run_tool("status")
        self.assertEqual(code, 0)
        self.assertIn("overall: mixed", output)
        self.assertEqual(self.run_tool("apply")[0], 0)
        self.assertIn("overall: patched", self.run_tool("status")[1])


class Refusals(OverlayFixture):
    def test_unknown_file_hash_refuses_without_writing(self):
        self.write(self.server, "dist/server/server/bootstrap.js", b"// someone else's build\n")
        before = self.byte_map()
        output = self.run_refused("apply")
        self.assertIn("unknown sha256", output)
        self.assertEqual(self.byte_map(), before)
        self.assertEqual(self.backups(), [])

    def test_unknown_file_hash_stops_status_and_revert(self):
        self.write(self.server, "dist/server/server/workspace-git-service.js", b"// other\n")
        code, output = self.run_tool("status")
        self.assertEqual(code, 1)
        self.assertIn("overall: unknown", output)
        self.run_refused("revert")

    def test_version_mismatch_refuses_before_writing(self):
        self.write(self.server, "package.json", json.dumps({"name": self.package, "version": "0.16.0"}).encode())
        before = self.byte_map()
        output = self.run_refused("apply")
        self.assertIn("0.16.0", output)
        self.assertEqual(self.byte_map(), before)

    def test_overlay_file_mismatch_refuses(self):
        self.write(self.overlay, "dist/server/server/bootstrap.js", b"// overlay drifted\n")
        before = self.byte_map()
        self.run_refused("apply")
        self.assertEqual(self.byte_map(), before)
        self.assertEqual(self.backups(), [])

    def test_manifest_cannot_escape_the_package(self):
        self.write_manifest([{"path": "../../etc/passwd", "stockSha256": None,
                              "patchedSha256": light_daemon_overlay.sha_bytes(b"x")}])
        self.run_refused("apply")

    def test_revert_without_a_stock_backup_refuses(self):
        self.assertEqual(self.run_tool("apply")[0], 0)
        (self.server / ("dist/server/server/bootstrap.js" + light_daemon_overlay.BACKUP_SUFFIX)).unlink()
        before = self.byte_map()
        output = self.run_refused("revert")
        self.assertIn("reinstall", output)
        self.assertEqual(self.byte_map(), before)


class Backups(OverlayFixture):
    def test_existing_matching_backup_is_kept(self):
        stock = REPLACED["dist/server/server/bootstrap.js"]
        self.write(self.server, "dist/server/server/bootstrap.js" + light_daemon_overlay.BACKUP_SUFFIX,
                   stock.encode(), mode=0o600)
        self.assertEqual(self.run_tool("apply")[0], 0)
        self.assertEqual((self.server / ("dist/server/server/bootstrap.js"
                                         + light_daemon_overlay.BACKUP_SUFFIX)).read_text(), stock)

    def test_foreign_backup_refuses_without_writing(self):
        self.write(self.server, "dist/server/server/bootstrap.js" + light_daemon_overlay.BACKUP_SUFFIX,
                   b"// not this file's stock\n")
        before = self.byte_map()
        output = self.run_refused("apply")
        self.assertIn("not the manifest's stock", output)
        self.assertEqual(self.byte_map(), before)


class DryRun(OverlayFixture):
    def test_dry_run_apply_and_revert_write_nothing(self):
        before = self.byte_map()
        code, output = self.run_tool("apply", "--dry-run")
        self.assertEqual(code, 0)
        self.assertIn("would replace", output)
        self.assertIn("would add", output)
        self.assertEqual(self.byte_map(), before)

        self.assertEqual(self.run_tool("apply")[0], 0)
        installed = self.byte_map()
        code, output = self.run_tool("revert", "--dry-run")
        self.assertEqual(code, 0)
        self.assertIn("would restore", output)
        self.assertIn("would remove", output)
        self.assertEqual(self.byte_map(), installed)

    def test_dry_run_is_rejected_for_status(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            self.run_tool("status", "--dry-run")


class Atomicity(OverlayFixture):
    def test_mid_apply_failure_restores_every_changed_file(self):
        real = light_daemon_overlay.atomic_write

        def flaky(path, data):
            if path.endswith("bootstrap.js"):
                raise OSError("injected failure")
            real(path, data)

        before = self.byte_map()
        with mock.patch.object(light_daemon_overlay, "atomic_write", flaky):
            output = self.run_refused("apply")
        self.assertIn("injected failure", output)
        self.assertEqual(self.byte_map(), before)
        self.assertEqual(self.backups(), [])

    def test_mid_apply_failure_removes_a_file_it_added(self):
        files = json.loads((self.overlay / "MANIFEST.json").read_text())["files"]
        files.reverse()  # the added file is written first
        self.write_manifest(files)
        real = light_daemon_overlay.atomic_write

        def flaky(path, data):
            if path.endswith("bootstrap.js"):
                raise OSError("injected failure")
            real(path, data)

        before = self.byte_map()
        with mock.patch.object(light_daemon_overlay, "atomic_write", flaky):
            self.run_refused("apply")
        self.assertEqual(self.byte_map(), before)
        self.assertFalse((self.server / ADDED).exists())


class Verify(OverlayFixture):
    def test_verify_names_the_file_that_is_not_installed(self):
        self.assertEqual(self.run_tool("apply")[0], 0)
        (self.server / ADDED).unlink()
        output = self.run_refused("verify")
        self.assertIn(ADDED, output)

    def test_verify_reports_the_other_patch_files(self):
        self.assertEqual(self.run_tool("apply")[0], 0)
        code, output = self.run_tool("verify")
        self.assertEqual(code, 0)
        for rel in light_daemon_overlay.CONTROL_PATCH_FILES:
            if rel not in REPLACED:
                self.assertIn(f"sha256 {light_daemon_overlay.installed_sha(str(self.server / rel))}", output)

    def test_verify_refuses_when_a_control_patch_file_is_missing(self):
        self.assertEqual(self.run_tool("apply")[0], 0)
        (self.server / "dist/server/server/agent/agent-manager.js").unlink()
        output = self.run_refused("verify")
        self.assertIn("agent-manager.js", output)


if __name__ == "__main__":
    unittest.main()
