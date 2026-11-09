# Light daemon dist overlay

`light_daemon_overlay.py` installs a reviewed set of built `dist/` files over an
installed `@getpaseo/server` package. It is how a host that runs the published
package picks up server changes that exist only in a later build.

Every file is hash-checked before it is written. An unknown file hash, a package
version the overlay was not built for, or an overlay file that does not match its
manifest stops the run before the first write. The tool never restarts a service.

## Usage

```sh
python3 light_daemon_overlay.py status|verify|apply|revert \
    --server <installed @getpaseo/server directory> \
    --overlay <directory with MANIFEST.json and the overlay files> \
    [--dry-run]
```

`--server` defaults to `/usr/local/lib/node_modules/@getpaseo/cli/node_modules/@getpaseo/server`.
`--dry-run` prints the plan for `apply` and `revert` and writes nothing.

| Action   | What it does                                                                                                        | Writes | Restart |
| -------- | ------------------------------------------------------------------------------------------------------------------- | ------ | ------- |
| `status` | prints `stock`, `patched`, `mixed` or `unknown` per file and overall                                                | no     | no      |
| `verify` | requires every listed file to match `patchedSha256`, then reports the hashes of files the other local patches touch | no     | no      |
| `apply`  | backs up each stock file as `<name>.light-daemon-stock`, then installs the overlay files                            | yes    | yes     |
| `revert` | restores the hash-checked stock backups and removes added files                                                     | yes    | yes     |

Exit codes: `0` for a completed action and for `status` in `stock`, `patched` or
`mixed`; `1` for a refusal, an `unknown` file, or an incomplete `verify`.

### System install

```sh
O=<overlay directory>
S=/usr/local/lib/node_modules/@getpaseo/cli/node_modules/@getpaseo/server
sudo python3 light_daemon_overlay.py status --server "$S" --overlay "$O"
sudo python3 light_daemon_overlay.py apply  --server "$S" --overlay "$O" --dry-run
sudo python3 light_daemon_overlay.py apply  --server "$S" --overlay "$O"
sudo python3 light_daemon_overlay.py verify --server "$S" --overlay "$O"
sudo systemctl restart paseo-daemon.service
```

### Unprivileged install

Same commands without `sudo`, pointing `--server` at the package you installed:

```sh
S="$(npm root -g)/@getpaseo/cli/node_modules/@getpaseo/server"
python3 light_daemon_overlay.py status --server "$S" --overlay "$O"
```

Restart the daemon the way that host starts it; the tool does not.

### Scratch copy

To test an overlay without touching the running install, copy the package and
point `--server` at the copy. Nothing needs restarting, and `revert` restores it:

```sh
cp -a "$S" /tmp/server-under-test
python3 light_daemon_overlay.py apply --server /tmp/server-under-test --overlay "$O"
```

## Manifest

`MANIFEST.json` sits at the root of the overlay directory. Overlay files sit at
their package-relative paths beside it, so the overlay directory mirrors the
package layout.

```json
{
  "package": "@getpaseo/server",
  "version": "0.15.3",
  "files": [
    {
      "path": "dist/server/server/workspace-git-service.js",
      "stockSha256": "cb63ed73436928c3930138d825a092d9e75fcb94545b906fdd7afc4f75acc4a8",
      "patchedSha256": "c94e5a4a8b06e0bc4b2f7f6ef41e088299e5556cc9225226119c8ea8930cc724"
    },
    {
      "path": "dist/server/server/owned-worktree-watch.js",
      "stockSha256": null,
      "patchedSha256": "5c1b2f0c4e6a9d3b7f8e0a1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e"
    }
  ]
}
```

- `package` and `version` must match the installed package's `package.json`
  exactly. A new release needs its own manifest.
- `path` is relative to the package root, stays inside it, and is not
  `package.json`, a `*.light-daemon-stock` file or a temporary file.
- `stockSha256` is the published file's sha256. `null` marks an added file that
  the published package does not contain.
- `patchedSha256` is the overlay file's sha256. `apply` refuses when the file in
  the overlay directory does not hash to it.

## Coexistence with the other local patches

The other local patch scripts edit single published files in place. They and
this overlay check hashes and never inspect versions, so their states compose
only as documented here.

- **`scoped_idle`** edits `agent/idle-runtime-cleanup/index.js` and
  `agent/agent-manager.js`. It is independent of this overlay unless the overlay
  replaces `agent-manager.js`. Then the patch's stock hash is the overlay's
  previous file, not the published one: the patch needs a new stock→patched hash
  pair for the overlay's output before it can be applied again.
- **`idle_workspace_watch`** edits `workspace-git-service.js` and `bootstrap.js`,
  which this overlay replaces. The overlay supersedes it: revert that patch
  before `apply`, and do not apply it afterwards. Its stock hashes will not match
  the overlay's files, so `apply` and `revert` are refused rather than silently
  leaving a half-patched file.
- **`github_repo_pr_poll`** edits `services/github-service.js`. It is
  independent of this overlay when the overlay does not replace that file. When
  the overlay does replace it, the overlay supersedes the patch; revert the patch
  first.

`verify` prints the current sha256 of the files the other local patches target,
so an operator can compare them with the hash pairs those scripts document.
A file is never mixed: an overlay file is written whole or not at all.

## Upgrade and revert order

Apply the overlay for the exact version that will run, and restart once:

1. `npm install -g @getpaseo/cli@<version>`
2. `light_daemon_overlay.py status` — expect `stock`
3. `light_daemon_overlay.py apply`
4. `light_daemon_overlay.py verify`
5. any other local patch for that version
6. one daemon restart

Revert, then restart:

1. `light_daemon_overlay.py revert`
2. one daemon restart

Reinstalling the package also reverts the overlay: `npm` replaces every file and
leaves the `*.light-daemon-stock` backups behind. Re-run `apply` after every
reinstall or upgrade. A new package version stops `apply` with the version
mismatch, and a reverted-then-upgraded file stops it with `unknown sha256` until
an overlay is built for that version.

## What needs a restart

The daemon loads its server code at startup. `status`, `verify` and `--dry-run`
read files only. `apply` and `revert` change the files the next start uses; the
running process keeps the old code until it is restarted.

## Limits

- The overlay covers `dist/` files inside `@getpaseo/server` only. It does not
  touch the CLI, native packages, or anything under `node_modules` inside the
  package, and it does not install dependencies.
- It proves file content, not behaviour. A green `verify` says the reviewed
  files are installed, not that the daemon works.
- It never restarts or reloads anything, and it does not check whether the
  running daemon is using the files it replaced.
- Backups are left in place after `revert` so the same overlay can be applied
  again. They are the only copy of the stock file; a package reinstall gives you
  fresh stock files but does not clean them up.
- There is no merge: an overlay file replaces the published file completely. A
  later patch for the same file has to chain onto the overlay's hash.
