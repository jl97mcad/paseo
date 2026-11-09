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
    def flaky_once(self):
        """Fail the first write of bootstrap.js only, so rollback can succeed."""
        real = light_daemon_overlay.atomic_write
        state = {"raised": False}

        def flaky(path, data):
            if path.endswith("bootstrap.js") and not state["raised"]:
                state["raised"] = True
                raise OSError("injected failure")
            real(path, data)

        return flaky

    def test_mid_apply_failure_restores_every_changed_file(self):
        flaky = self.flaky_once()

        before = self.byte_map()
        with mock.patch.object(light_daemon_overlay, "atomic_write", flaky):
            output = self.run_refused("apply")
        self.assertIn("injected failure", output)
        self.assertEqual(self.byte_map(), before)
        self.assertEqual(self.backups(), [])

    def test_mid_apply_failure_removes_a_file_it_added(self):
        files = json.loads((self.overlay / "MANIFEST.json").read_text())["files"]
        files.reverse()  # the added file is last in the manifest
        self.write_manifest(files)
        flaky = self.flaky_once()
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


class Ordering(OverlayFixture):
    """Added files are written before changed ones, and removed after they are restored."""

    def record(self):
        written = []
        real = light_daemon_overlay.atomic_write

        def recording(path, data):
            if not path.endswith(light_daemon_overlay.BACKUP_SUFFIX):
                written.append(os.path.basename(path))
            real(path, data)

        return written, mock.patch.object(light_daemon_overlay, "atomic_write", recording)

    def test_apply_writes_the_added_file_before_the_changed_files(self):
        written, patcher = self.record()
        with patcher:
            self.assertEqual(self.run_tool("apply")[0], 0)
        self.assertEqual(written, [os.path.basename(ADDED),
                                   "workspace-git-service.js", "bootstrap.js"])

    def test_apply_orders_added_first_whatever_the_manifest_order(self):
        files = json.loads((self.overlay / "MANIFEST.json").read_text())["files"]
        self.write_manifest(list(reversed(files)))  # added file last
        written, patcher = self.record()
        with patcher:
            self.assertEqual(self.run_tool("apply")[0], 0)
        self.assertEqual(written[0], os.path.basename(ADDED))
        self.assertEqual(self.run_tool("verify")[0], 0)

    def test_revert_removes_the_added_file_last(self):
        self.assertEqual(self.run_tool("apply")[0], 0)
        removed = []
        real_remove = os.remove

        def recording(path, *args, **kwargs):
            removed.append(os.path.basename(path))
            return real_remove(path, *args, **kwargs)

        written, patcher = self.record()
        with patcher, mock.patch.object(light_daemon_overlay.os, "remove", recording):
            self.assertEqual(self.run_tool("revert")[0], 0)
        self.assertEqual(written, ["bootstrap.js", "workspace-git-service.js"])
        self.assertEqual(removed[:1], [os.path.basename(ADDED)])


class BuilderOrdering(unittest.TestCase):
    """The build emits entries in the order the installer relies on."""

    def setUp(self):
        HERE_SPEC = importlib.util.spec_from_file_location(
            "build_overlay", HERE / "build_overlay.py")
        self.build = importlib.util.module_from_spec(HERE_SPEC)
        HERE_SPEC.loader.exec_module(self.build)
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def tree(self, name, files):
        for rel, text in files.items():
            path = self.root / name / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(text if isinstance(text, bytes) else text.encode())
        return str(self.root / name)

    def test_manifest_lists_added_files_before_changed_ones(self):
        changed = "dist/server/server/session.js"
        added = "dist/server/server/session/workspace-git-observer/workspace-liveness.js"
        stock = self.tree("stock", {changed: "// stock\n", "dist/server/server/quiet.js": "// same\n"})
        baseline = self.tree("baseline", {changed: "// stock\n", "dist/server/server/quiet.js": "// same\n"})
        patched = self.tree("patched", {changed: "// patched\n", added: "// added\n",
                                        "dist/server/server/quiet.js": "// same\n",
                                        "dist/server/server/map.js.map": "ignored"})
        out = str(self.root / "overlay")
        self.assertEqual(self.build.run(stock, baseline, patched, out,
                                        "@getpaseo/server", "0.15.3"), 0)
        manifest = json.loads((pathlib.Path(out) / "MANIFEST.json").read_text())
        self.assertEqual(manifest["version"], "0.15.3")
        self.assertEqual([entry["path"] for entry in manifest["files"]], [added, changed])
        self.assertIsNone(manifest["files"][0]["stockSha256"])
        self.assertEqual(manifest["files"][1]["stockSha256"],
                         light_daemon_overlay.sha_bytes(b"// stock\n"))
        self.assertEqual(manifest["files"][1]["sizeBytes"], len(b"// patched\n"))
        self.assertFalse((pathlib.Path(out) / "dist/server/server/map.js.map").exists())
        # The installer accepts what the builder wrote.
        installer = light_daemon_overlay.load_manifest(out)
        self.assertEqual([entry.path for entry in installer.entries], [added, changed])

    def test_a_dropped_file_refuses(self):
        gone = "dist/server/server/gone.js"
        stock = self.tree("stock", {gone: "// stock\n"})
        baseline = self.tree("baseline", {gone: "// stock\n"})
        patched = self.tree("patched", {"dist/server/server/new.js": "// new\n"})
        with self.assertRaises(SystemExit) as caught:
            self.build.run(stock, baseline, patched, str(self.root / "overlay"),
                           "@getpaseo/server", "0.15.3")
        self.assertIn(gone, str(caught.exception.code))


class FailureReporting(OverlayFixture):
    """A rollback that cannot restore must fail loudly instead of claiming success."""

    def test_apply_names_files_it_could_not_restore_and_stays_re_runnable(self):
        state = {"apply_failed": False}
        real = light_daemon_overlay.atomic_write

        def flaky(path, data):
            if path.endswith("bootstrap.js") and not state["apply_failed"]:
                state["apply_failed"] = True
                raise OSError("injected apply failure")
            if state["apply_failed"] and path.endswith("workspace-git-service.js"):
                raise OSError("injected restore failure")
            real(path, data)

        with mock.patch.object(light_daemon_overlay, "atomic_write", flaky):
            output = self.run_refused("apply")
        self.assertIn("could not restore 1 of 3 file(s)", output)
        self.assertIn("workspace-git-service.js", output)
        self.assertNotIn("restored 3 file(s), wrote nothing", output)
        self.assertNotIn("applied 3 file(s)", output)

        # The stock backups stay, so the same command finishes once the cause is fixed.
        self.assertIn("bootstrap.js" + light_daemon_overlay.BACKUP_SUFFIX, self.backups())
        self.assertEqual(self.run_tool("apply")[0], 0)
        self.assertEqual(self.run_tool("verify")[0], 0)

    def test_revert_names_files_it_could_not_restore(self):
        self.assertEqual(self.run_tool("apply")[0], 0)
        real = light_daemon_overlay.atomic_write

        def flaky(path, data):
            if path.endswith("workspace-git-service.js"):
                raise OSError("injected restore failure")
            real(path, data)

        with mock.patch.object(light_daemon_overlay, "atomic_write", flaky):
            output = self.run_refused("revert")
        self.assertIn("could not restore", output)
        self.assertIn("workspace-git-service.js", output)
        self.assertNotIn("reverted", output)
        self.assertEqual(len(self.backups()), len(REPLACED))
        self.assertEqual(self.run_tool("revert")[0], 0)
        self.assertEqual(self.run_tool("status")[1].splitlines()[-1], "overall: stock")


if __name__ == "__main__":
    unittest.main()
