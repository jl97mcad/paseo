import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, rm, stat, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath, pathToFileURL } from "node:url";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const clientModule = path.join(repoRoot, "packages/server/dist/server/server/git-worker/client.js");
const workerModule = path.join(repoRoot, "packages/server/dist/server/server/git-worker/worker.js");

test("built daemon reads checkout facts through the compiled Git worker", async () => {
  assert.equal((await stat(workerModule)).isFile(), true);
  const { createWorkerGitDeps, resolveGitWorkerEntry } = await import(
    pathToFileURL(clientModule).href
  );

  // Compiled output must start the emitted entry, never a source file plus tsx.
  const entry = resolveGitWorkerEntry();
  assert.equal(entry.eval, false);
  assert.equal(new URL(entry.script).href, pathToFileURL(workerModule).href);

  const directory = await mkdtemp(path.join(os.tmpdir(), "git-worker-dist-"));
  const deps = createWorkerGitDeps({ size: 1 });
  try {
    await execFileAsync("git", ["init", "--initial-branch=main", directory]);
    await execFileAsync("git", ["-C", directory, "config", "user.email", "dist@paseo.local"]);
    await execFileAsync("git", ["-C", directory, "config", "user.name", "Dist Test"]);
    await writeFile(path.join(directory, "tracked.txt"), "one\ntwo\n");
    await execFileAsync("git", ["-C", directory, "add", "."]);
    await execFileAsync("git", ["-C", directory, "commit", "-m", "fixture"]);

    const facts = await deps.getCheckoutSnapshotFacts(directory, {});
    assert.equal(facts.isGit, true);
    assert.deepEqual(deps.workerStats(), {
      size: 1,
      enabled: true,
      workers: 1,
      inFlight: 0,
      queued: 0,
      dispatched: 1,
      fallbacks: 0,
      dropped: 0,
      retirements: 0,
    });
  } finally {
    await deps.disposeDeps();
    await rm(directory, { recursive: true, force: true });
  }
});
