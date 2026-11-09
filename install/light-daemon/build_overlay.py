#!/usr/bin/env python3
"""Build a light daemon overlay directory from two builds and the published package.

    build_overlay.py --stock <published @getpaseo/server dir> \
        --baseline <same version built from its tag> \
        --patched <build of the changed source> \
        --out <overlay dir> --version 0.15.3

`--stock` is the unpacked `npm pack` tarball for the version. `--baseline` is
built from the same tag the same way as `--patched`, so build-only differences
(hashed module names, absolute paths) cancel out; a file that differs between
stock and baseline but not between baseline and patched is printed as a note,
because that difference is not ours to ship.

Files are compared under `dist/` only, by package-relative path, ignoring
`*.map` (the package excludes them). A file the patched build adds is listed
with `stockSha256: null`; a file it drops is an error. Entries are ordered
added-first, which the installer relies on: a changed file may import an added
one (`session.js` imports
`session/workspace-git-observer/workspace-liveness.js`).
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import light_daemon_overlay as overlay  # noqa: E402


def dist_files(root):
    """Every file under `dist/`, package-relative, ignoring source maps."""
    found = {}
    base = os.path.join(root, "dist")
    for directory, _, names in os.walk(base):
        for name in names:
            if name.endswith(".map"):
                continue
            path = os.path.join(directory, name)
            found[os.path.relpath(path, root)] = overlay.read_bytes(path)
    return found


def manifest_entries(stock, baseline, patched):
    """Entries for files we changed or added, added-first, then by path."""
    entries, baseline_drift, dropped = [], [], []
    for rel in sorted(patched):
        ours, published, base = patched[rel], stock.get(rel), baseline.get(rel)
        if published == ours:
            if base != published:
                baseline_drift.append(rel)
            continue
        entries.append({
            "path": rel,
            "stockSha256": None if published is None else overlay.sha_bytes(published),
            "patchedSha256": overlay.sha_bytes(ours),
            "sizeBytes": len(ours),
            "_bytes": ours,
        })
    for rel in sorted(set(stock) - set(patched)):
        if rel in baseline:
            dropped.append(rel)
    return entries, baseline_drift, dropped


def run(stock_root, baseline_root, patched_root, out, package, version):
    stock, baseline, patched = (dist_files(root) for root in (stock_root, baseline_root, patched_root))
    entries, baseline_drift, dropped = manifest_entries(stock, baseline, patched)
    if dropped:
        raise SystemExit("refusing: the patched build drops files the package ships, which an "
                         "overlay cannot remove:\n  " + "\n  ".join(dropped))
    for rel in baseline_drift:
        print(f"note: the baseline build differs from the package at {rel}")
    if not entries:
        raise SystemExit("refusing: no file differs between the baseline and patched builds")
    # Added files first: the order light_daemon_overlay.apply uses, kept in the manifest too so a
    # human reading it sees the write order.
    entries.sort(key=lambda entry: entry["stockSha256"] is not None)
    for entry in entries:
        data = entry.pop("_bytes")
        path = os.path.join(out, entry["path"])
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as handle:
            handle.write(data)
    manifest = {"package": package, "version": version, "files": entries}
    with open(os.path.join(out, overlay.MANIFEST), "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)
        handle.write("\n")
    added = sum(1 for entry in entries if entry["stockSha256"] is None)
    print(f"wrote {len(entries)} file(s) to {out}: {added} added, {len(entries) - added} changed")
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--stock", required=True, help="unpacked published package")
    parser.add_argument("--baseline", required=True, help="that version built from its tag")
    parser.add_argument("--patched", required=True, help="build of the changed source")
    parser.add_argument("--out", required=True, help="overlay directory to write")
    parser.add_argument("--package", default="@getpaseo/server")
    parser.add_argument("--version", required=True)
    args = parser.parse_args(argv)
    return run(args.stock, args.baseline, args.patched, args.out, args.package, args.version)


if __name__ == "__main__":
    sys.exit(main())
