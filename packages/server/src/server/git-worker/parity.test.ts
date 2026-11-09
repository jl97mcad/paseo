import { execFile } from "node:child_process";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { promisify } from "node:util";
import { afterEach, describe, expect, it } from "vitest";
import {
  getCheckoutRefDerivedState,
  getCheckoutShortstat,
  getCheckoutSnapshotFacts,
  getCheckoutStatus,
  getCheckoutWorktreeState,
  type CheckoutContext,
} from "../../utils/checkout-git.js";
import { createWorkerGitDeps, type WorkerGitDeps } from "./client.js";

const execFileAsync = promisify(execFile);
const temporaryDirectories: string[] = [];
const workerPools: WorkerGitDeps[] = [];

afterEach(async () => {
  await Promise.all(workerPools.splice(0).map((deps) => deps.disposeDeps()));
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { recursive: true, force: true, maxRetries: 5 })),
  );
});

async function git(cwd: string, ...args: string[]): Promise<void> {
  await execFileAsync("git", ["-c", "advice.detachedHead=false", ...args], { cwd });
}

async function createRepository(root: string, name: string): Promise<string> {
  const origin = path.join(root, `${name}-origin.git`);
  const work = path.join(root, name);
  await mkdir(root, { recursive: true });
  await execFileAsync("git", ["init", "--bare", "--initial-branch=main", origin]);
  await execFileAsync("git", ["clone", origin, work]);
  await git(work, "config", "user.email", "parity@paseo.local");
  await git(work, "config", "user.name", "Parity Fixture");
  await writeFile(path.join(work, "tracked.txt"), "one\ntwo\n");
  await writeFile(path.join(work, ".gitignore"), "build/\n");
  await git(work, "add", ".gitignore", "tracked.txt");
  await git(work, "commit", "-m", "fixture");
  await git(work, "push", "-u", "origin", "main");
  // One local commit: the workspace is ahead of origin afterwards.
  await writeFile(path.join(work, "local.txt"), "local\n");
  await git(work, "add", "local.txt");
  await git(work, "commit", "-m", "local work");
  // One upstream commit from another clone: the workspace is behind too.
  const other = path.join(root, `${name}-other`);
  await execFileAsync("git", ["clone", origin, other]);
  await git(other, "config", "user.email", "parity@paseo.local");
  await git(other, "config", "user.name", "Parity Fixture");
  await writeFile(path.join(other, "upstream.txt"), "upstream\n");
  await git(other, "add", "upstream.txt");
  await git(other, "commit", "-m", "upstream work");
  await git(other, "push", "origin", "main");
  await git(work, "fetch", "origin");
  return work;
}

async function makeDirty(work: string): Promise<void> {
  await writeFile(path.join(work, "tracked.txt"), "one\ntwo\nthree\nfour\n");
  await writeFile(path.join(work, "untracked.txt"), "alpha\nbeta\ngamma\n");
  await mkdir(path.join(work, "nested", "deeper"), { recursive: true });
  await writeFile(path.join(work, "nested", "deeper", "notes.txt"), "note one\nnote two\n");
}

describe("git worker parity with the in-thread reads", () => {
  it("returns identical facts and states for clean and dirty real repositories", async () => {
    const root = await mkdtemp(path.join(tmpdir(), "paseo-git-worker-parity-"));
    temporaryDirectories.push(root);
    const paseoHome = path.join(root, "paseo-home");
    await mkdir(paseoHome, { recursive: true });

    const dirty = await createRepository(root, "dirty");
    const clean = await createRepository(root, "clean");
    await makeDirty(dirty);

    const workerDeps = createWorkerGitDeps({ size: 1 });
    workerPools.push(workerDeps);

    for (const cwd of [dirty, clean]) {
      const context: CheckoutContext = { paseoHome };
      const inThreadFacts = await getCheckoutSnapshotFacts(cwd, context);
      const workerFacts = await workerDeps.getCheckoutSnapshotFacts(cwd, context);
      expect(workerFacts).toEqual(inThreadFacts);
      expect(workerFacts.isGit).toBe(true);
      if (!workerFacts.isGit) {
        throw new Error(`Fixture is not a Git checkout: ${cwd}`);
      }

      const withFacts: CheckoutContext = { paseoHome, facts: workerFacts };
      const workerStatus = await workerDeps.getCheckoutStatus(cwd, withFacts);
      expect(workerStatus).toEqual(await getCheckoutStatus(cwd, withFacts));
      expect(workerStatus.isGit).toBe(true);

      expect(await workerDeps.getCheckoutShortstat(cwd, withFacts, { force: true })).toEqual(
        await getCheckoutShortstat(cwd, withFacts, { force: true }),
      );
      const workerWorktree = await workerDeps.getCheckoutWorktreeState(cwd, withFacts);
      expect(workerWorktree).toEqual(await getCheckoutWorktreeState(cwd, withFacts));

      const current = {
        aheadBehind: workerStatus.isGit ? workerStatus.aheadBehind : null,
        diffStat: workerWorktree.diffStat,
      };
      const movedRemoteRefs = new Set(["refs/remotes/origin/main"]);
      expect(
        await workerDeps.getCheckoutRefDerivedState(
          cwd,
          workerFacts,
          current,
          movedRemoteRefs,
          withFacts,
        ),
      ).toEqual(
        await getCheckoutRefDerivedState(cwd, workerFacts, current, movedRemoteRefs, withFacts),
      );

      if (cwd === dirty) {
        // The fixture only proves something if the readings are non-trivial.
        expect(workerWorktree.isDirty).toBe(true);
        expect(workerWorktree.diffStat?.additions).toBeGreaterThan(0);
        if (!workerStatus.isGit) {
          throw new Error("Expected a Git status");
        }
        expect(workerStatus.aheadOfOrigin).toBe(1);
        expect(workerStatus.behindOfOrigin).toBe(1);
      }
    }

    // Every read went through a worker, and nothing fell back to main.
    expect(workerDeps.workerStats()).toMatchObject({
      dispatched: 10,
      fallbacks: 0,
      retirements: 0,
    });
  });

  it("matches the in-thread outcome and error shape for a missing directory", async () => {
    const root = await mkdtemp(path.join(tmpdir(), "paseo-git-worker-missing-"));
    temporaryDirectories.push(root);
    const workerDeps = createWorkerGitDeps({ size: 1 });
    workerPools.push(workerDeps);
    const missing = path.join(root, "not-there");

    const inThread = await getCheckoutSnapshotFacts(missing, {}).catch((error: unknown) => error);
    const worker = await workerDeps
      .getCheckoutSnapshotFacts(missing, {})
      .catch((error: unknown) => error);

    if (inThread instanceof Error) {
      expect(worker).toBeInstanceOf(Error);
      expect({ name: (worker as Error).name, message: (worker as Error).message }).toEqual({
        name: inThread.name,
        message: inThread.message,
      });
    } else {
      expect(worker).toEqual(inThread);
    }
    expect(workerDeps.workerStats()).toMatchObject({ dispatched: 1, fallbacks: 0 });
  });
});
