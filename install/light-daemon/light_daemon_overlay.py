#!/usr/bin/env python3
"""Install a reviewed dist overlay over a published @getpaseo/server package.

A host that runs the published package cannot pick up server changes that only
exist in a later build. This tool replaces a small, listed set of built files
(files under `dist/`, plus any new files the build adds) with the reviewed
overlay copies, after checking every file hash against a manifest:

- stock files are byte-identical to the published package;
- patched files are byte-identical to the overlay copy.

Nothing else is written. An unknown file hash, a package name or version that
differs from the manifest, or an overlay file that does not match its manifest
hash stops the run before the first write. Added files are written before
changed ones, so a changed file never imports a file the run has not written
yet; `revert` walks that order backwards. Each replaced file keeps a
`<name>.light-daemon-stock` backup, an existing backup is never overwritten,
and a failure part-way through restores every file the run had already
changed. A restore that itself fails exits non-zero, names the files it could
not restore and keeps the backups, so the same command finishes the job once
the cause is fixed. Re-run `apply` after every reinstall or upgrade.

Usage:
  light_daemon_overlay.py status|verify|apply|revert \
      --server <installed @getpaseo/server directory> \
      --overlay <directory holding MANIFEST.json and the overlay files> \
      [--dry-run]

`status` reports stock, patched, mixed or unknown per file and overall.
`verify` requires the full overlay to be installed and reports the hashes of
files that the other local patches touch. `apply` installs the overlay,
`revert` restores the hash-checked stock backups and removes added files.
`--dry-run` prints the plan for apply and revert and writes nothing.

This tool never restarts a service. The running daemon keeps the old code until
it is restarted.
"""
from __future__ import annotations

import argparse
import collections
import hashlib
import json
import os
import re
import shutil
import sys

DEFAULT_SERVER = "/usr/local/lib/node_modules/@getpaseo/cli/node_modules/@getpaseo/server"
MANIFEST = "MANIFEST.json"
BACKUP_SUFFIX = ".light-daemon-stock"
TEMP_SUFFIX = ".light-daemon-tmp"
SHA = re.compile(r"[0-9a-f]{64}")

# Files the other local patch scripts edit. `verify` reports their hashes so an
# operator can compare them with the pairs those scripts document; the overlay
# itself replaces some of them.
CONTROL_PATCH_FILES = {
    "dist/server/server/agent/idle-runtime-cleanup/index.js": "scoped_idle",
    "dist/server/server/agent/agent-manager.js": "scoped_idle",
    "dist/server/server/workspace-git-service.js": "idle_workspace_watch",
    "dist/server/server/bootstrap.js": "idle_workspace_watch",
    "dist/server/services/github-service.js": "github_repo_pr_poll",
}


Entry = collections.namedtuple("Entry", "path stock patched overlay_bytes")
Manifest = collections.namedtuple("Manifest", "package version entries")


def sha_bytes(data):
    return hashlib.sha256(data).hexdigest()


def read_bytes(path):
    with open(path, "rb") as handle:
        return handle.read()


def installed_sha(path):
    return sha_bytes(read_bytes(path)) if os.path.exists(path) else None


def atomic_write(path, data):
    """Replace one file with `data`: temp file in the same directory, fsync, rename."""
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    tmp = path + TEMP_SUFFIX
    try:
        with open(tmp, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        if os.path.exists(path):
            shutil.copymode(path, tmp)
        os.replace(tmp, path)
        descriptor = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def load_manifest(overlay):
    path = os.path.join(overlay, MANIFEST)
    if not os.path.isfile(path):
        raise SystemExit(f"overlay: no {MANIFEST} in {overlay}")
    try:
        raw = json.loads(read_bytes(path).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SystemExit(f"overlay: {path}: {error}")
    if not isinstance(raw, dict):
        raise SystemExit(f"overlay: {path} must hold a JSON object")
    package, version, files = raw.get("package"), raw.get("version"), raw.get("files")
    if not isinstance(package, str) or not package:
        raise SystemExit(f"overlay: {path}: package must be a package name")
    if not isinstance(version, str) or not version:
        raise SystemExit(f"overlay: {path}: version must be a package version")
    if not isinstance(files, list) or not files:
        raise SystemExit(f"overlay: {path}: files must be a non-empty list")
    entries, seen = [], set()
    for index, item in enumerate(files):
        where = f"overlay: {path}: files[{index}]"
        if not isinstance(item, dict):
            raise SystemExit(f"{where} must be an object")
        rel = item.get("path")
        if not isinstance(rel, str) or not rel:
            raise SystemExit(f"{where}.path must be a relative file path")
        parts = rel.split("/")
        if rel.startswith("/") or os.path.isabs(rel) or ".." in parts or "." in parts or "" in parts:
            raise SystemExit(f"{where}.path must stay inside the package: {rel}")
        if rel == "package.json" or rel.endswith(BACKUP_SUFFIX) or rel.endswith(TEMP_SUFFIX):
            raise SystemExit(f"{where}.path is not an overlay target: {rel}")
        if rel in seen:
            raise SystemExit(f"{where}.path is listed twice: {rel}")
        seen.add(rel)
        stock, patched = item.get("stockSha256"), item.get("patchedSha256")
        if not isinstance(patched, str) or not SHA.fullmatch(patched):
            raise SystemExit(f"{where}.patchedSha256 must be a sha256")
        if stock is not None and (not isinstance(stock, str) or not SHA.fullmatch(stock)):
            raise SystemExit(f"{where}.stockSha256 must be a sha256 or null for an added file")
        if stock == patched:
            raise SystemExit(f"{where}: stockSha256 equals patchedSha256")
        source = os.path.join(overlay, rel)
        if not os.path.isfile(source):
            raise SystemExit(f"{where}: no overlay file at {source}")
        data = read_bytes(source)
        if sha_bytes(data) != patched:
            raise SystemExit(f"{where}: {rel} has sha256 {sha_bytes(data)}, not the manifest's {patched}")
        entries.append(Entry(rel, stock, patched, data))
    return Manifest(package, version, tuple(entries))


def check_package(server, manifest, action):
    path = os.path.join(server, "package.json")
    if not os.path.isfile(path):
        raise SystemExit(f"refusing: no package.json in {server}")
    try:
        raw = json.loads(read_bytes(path).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SystemExit(f"refusing: {path}: {error}")
    name, version = raw.get("name"), raw.get("version")
    if name != manifest.package or version != manifest.version:
        raise SystemExit(
            f"refusing: {server} is {name}@{version}, the overlay applies to "
            f"{manifest.package}@{manifest.version}"
        )
    if action != "status":
        print(f"package: {name}@{version}")


def file_state(entry, server):
    path = os.path.join(server, entry.path)
    digest = installed_sha(path)
    if digest is None:
        return "stock" if entry.stock is None else "unknown (missing)"
    if digest == entry.patched:
        return "patched"
    if entry.stock is not None and digest == entry.stock:
        return "stock"
    return f"unknown sha256 {digest}"


def overall_state(states):
    if any(value.startswith("unknown") for value in states):
        return "unknown"
    if all(value == "stock" for value in states):
        return "stock"
    if all(value == "patched" for value in states):
        return "patched"
    return "mixed"


def backup_path(server, entry):
    return os.path.join(server, entry.path + BACKUP_SUFFIX)


def check_backup(entry, server):
    """An existing backup must hold this file's stock bytes; it is never overwritten."""
    backup = backup_path(server, entry)
    if not os.path.exists(backup):
        return
    digest = installed_sha(backup)
    if digest != entry.stock:
        raise SystemExit(
            f"refusing: {backup} has sha256 {digest}, not the manifest's stock {entry.stock}"
        )


def current_states(server, manifest):
    states = {entry.path: file_state(entry, server) for entry in manifest.entries}
    unknown = [f"{rel}: {value}" for rel, value in states.items() if value.startswith("unknown")]
    if unknown:
        raise SystemExit(
            "refusing: these files match neither the stock nor the patched hash\n  " + "\n  ".join(unknown)
        )
    return states


def apply_order(entries):
    """Added files first: a changed file may import one the package does not hold yet.

    `session.js` imports `session/workspace-git-observer/workspace-liveness.js`, which the
    published package lacks, so writing the importer first leaves a broken tree for as long as
    the run takes. Reverting walks this order backwards, restoring changed files before the
    added files they may import are removed.
    """
    return sorted(entries, key=lambda entry: entry.stock is not None)


def rollback(journal, created_backups=()):
    """Undo a journal in reverse; return the files whose restore raised OSError.

    Backups are kept when any restore failed: they are the only copy of the stock bytes, and a
    re-run needs them. A file left patched is still a state `apply` or `revert` can finish from.
    """
    failed = []
    for target, previous in reversed(journal):
        try:
            if previous is None:
                if os.path.exists(target):
                    os.remove(target)
            else:
                atomic_write(target, previous)
        except OSError:
            failed.append(target)
    if not failed:
        for backup in created_backups:
            if os.path.exists(backup):
                os.remove(backup)
    return failed


def fail_with_restore(action, error, journal, failed):
    """Exit non-zero, naming what could not be restored and how to finish the run."""
    if failed:
        raise SystemExit(
            f"{action} failed: {error}; could not restore {len(failed)} of {len(journal)} "
            f"file(s):\n  " + "\n  ".join(failed)
            + "\nThe stock backups are kept; re-run the same command once the cause is fixed."
        )
    raise SystemExit(f"{action} failed: {error}; restored {len(journal)} file(s), wrote nothing")


def run_apply(server, manifest, dry_run):
    states = current_states(server, manifest)
    todo = [entry for entry in apply_order(manifest.entries) if states[entry.path] == "stock"]
    for entry in todo:
        if entry.stock is not None:
            check_backup(entry, server)
    if not todo:
        print("nothing to apply")
        return
    for entry in todo:
        target = os.path.join(server, entry.path)
        verb = "add" if entry.stock is None else "replace"
        print(f"{'would ' if dry_run else ''}{verb}: {target}")
    if dry_run:
        return
    journal, created_backups = [], []
    try:
        for entry in todo:
            target = os.path.join(server, entry.path)
            previous = read_bytes(target) if os.path.exists(target) else None
            journal.append((target, previous))
            if entry.stock is not None:
                backup = backup_path(server, entry)
                if not os.path.exists(backup):
                    created_backups.append(backup)
                    atomic_write(backup, previous)
                    shutil.copymode(target, backup)
            atomic_write(target, entry.overlay_bytes)
    except BaseException as error:
        fail_with_restore("apply", error, journal, rollback(journal, created_backups))
    print(f"applied {len(todo)} file(s)")


def run_revert(server, manifest, dry_run):
    states = current_states(server, manifest)
    plan = []
    for entry in apply_order(manifest.entries):
        state, target = states[entry.path], os.path.join(server, entry.path)
        if entry.stock is None:
            if state == "patched":
                plan.append((entry, target, None))
            continue
        if state != "patched":
            continue
        backup = backup_path(server, entry)
        if not os.path.isfile(backup):
            raise SystemExit(f"refusing: no stock backup at {backup}; reinstall the package to revert")
        stock = read_bytes(backup)
        if sha_bytes(stock) != entry.stock:
            raise SystemExit(
                f"refusing: {backup} has sha256 {sha_bytes(stock)}, not the manifest's stock {entry.stock}"
            )
        plan.append((entry, target, stock))
    plan.reverse()
    for entry, target, stock in plan:
        print(f"{'would ' if dry_run else ''}{'remove' if stock is None else 'restore'}: {target}")
    if not plan:
        print("nothing to revert")
        return
    if dry_run:
        return
    journal = []
    try:
        for entry, target, stock in plan:
            previous = read_bytes(target) if os.path.exists(target) else None
            journal.append((target, previous))
            if stock is None:
                os.remove(target)
            else:
                atomic_write(target, stock)
    except BaseException as error:
        fail_with_restore("revert", error, journal, rollback(journal))
    print(f"reverted {len(plan)} file(s)")


def run_status(server, manifest):
    states = []
    for entry in manifest.entries:
        value = file_state(entry, server)
        states.append(value)
        print(f"{value}: {os.path.join(server, entry.path)}")
    overall = overall_state(states)
    print(f"overall: {overall}")
    return 0 if overall != "unknown" else 1


def run_verify(server, manifest):
    bad = []
    for entry in manifest.entries:
        value = file_state(entry, server)
        print(f"{value}: {os.path.join(server, entry.path)}")
        if value != "patched":
            bad.append(f"{entry.path}: {value}")
    for rel, patch in sorted(CONTROL_PATCH_FILES.items()):
        path = os.path.join(server, rel)
        digest = installed_sha(path)
        if digest is None:
            bad.append(f"{rel}: missing ({patch})")
            print(f"missing: {path} ({patch})")
            continue
        backup = path + BACKUP_SUFFIX
        note = "backup present" if os.path.exists(backup) else "no backup"
        print(f"sha256 {digest} {path} ({patch}, {note})")
    if bad:
        raise SystemExit("refusing: the overlay is not fully installed\n  " + "\n  ".join(bad))
    print(f"verified {len(manifest.entries)} file(s)")
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("action", choices=["status", "verify", "apply", "revert"])
    parser.add_argument("--server", default=DEFAULT_SERVER)
    parser.add_argument("--overlay", required=True)
    parser.add_argument("--dry-run", action="store_true", help="print the plan for apply and revert")
    args = parser.parse_args(argv)
    if args.dry_run and args.action not in ("apply", "revert"):
        parser.error("--dry-run applies to apply and revert")
    server = os.path.abspath(args.server)
    manifest = load_manifest(os.path.abspath(args.overlay))
    check_package(server, manifest, args.action)
    if args.action == "status":
        return run_status(server, manifest)
    if args.action == "verify":
        return run_verify(server, manifest)
    if args.action == "apply":
        run_apply(server, manifest, args.dry_run)
    else:
        run_revert(server, manifest, args.dry_run)
    print("The running daemon keeps the old code until it is restarted.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
