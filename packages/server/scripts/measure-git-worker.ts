import { execFile } from "node:child_process";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { promisify } from "node:util";
import pLimit from "p-limit";
import {
  getCheckoutRefDerivedState,
  getCheckoutShortstat,
  getCheckoutSnapshotFacts,
  getCheckoutStatus,
  getCheckoutWorktreeState,
} from "../src/utils/checkout-git.js";
import { createWorkerGitDeps, type WorkerGitDeps } from "../src/server/git-worker/client.js";
import { measureMainThreadWork } from "../src/test-utils/main-thread-work.js";

// Compares the main-thread cost of the per-workspace checkout reads in-thread
// against the same reads through the Git worker pool. The figure that matters is
// blockingMs: work the daemon's event loop cannot interleave. Wall time is
// reported because the worker path trades it for a lighter main thread.
//   npm run measure:git-worker --workspace=@getpaseo/server
//   PASEO_MEASURE_WORKSPACES=50 PASEO_MEASURE_ROUNDS=2 npm run measure:git-worker --workspace=@getpaseo/server

const execFileAsync = promisify(execFile);
const WORKSPACE_CONCURRENCY = 4;
const TRACKED_FILES = 5;

type SweepDeps = Pick<
  WorkerGitDeps,
  | "getCheckoutSnapshotFacts"
  | "getCheckoutStatus"
  | "getCheckoutShortstat"
  | "getCheckoutWorktreeState"
  | "getCheckoutRefDerivedState"
>;

interface SweepResult {
  blockingMs: number;
  filesystemRequests: number;
  yields: number;
  elapsedMs: number;
  reads: number;
}

function readInteger(value: string | undefined, fallback: number): number {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

async function git(cwd: string, ...args: string[]): Promise<void> {
  await execFileAsync("git", args, { cwd });
}

async function createWorkspace(root: string, index: number): Promise<string> {
  const cwd = path.join(root, `workspace-${index}`);
  await mkdir(cwd, { recursive: true });
  await git(cwd, "init", "--initial-branch=main");
  await git(cwd, "config", "user.email", "measurement@paseo.local");
  await git(cwd, "config", "user.name", "Paseo Measurement");
  for (let file = 0; file < TRACKED_FILES; file += 1) {
    const lines = Array.from({ length: 40 }, (_, line) => `tracked ${file} line ${line}`);
    await writeFile(path.join(cwd, `tracked-${file}.txt`), `${lines.join("\n")}\n`);
  }
  await writeFile(path.join(cwd, ".gitignore"), "build/\n");
  await git(cwd, "add", ".");
  await git(cwd, "commit", "-m", "fixture");
  // A dirty tree and untracked files give the reads something to parse.
  for (let file = 0; file < 3; file += 1) {
    const lines = Array.from({ length: 30 }, (_, line) => `untracked ${file} line ${line}`);
    await writeFile(path.join(cwd, `untracked-${file}.txt`), `${lines.join("\n")}\n`);
  }
  const edited = Array.from({ length: 60 }, (_, line) => `edited line ${line}`);
  await writeFile(path.join(cwd, "tracked-0.txt"), `${edited.join("\n")}\n`);
  return cwd;
}

async function sweep(input: {
  deps: SweepDeps;
  workspaces: string[];
  paseoHome: string;
}): Promise<number> {
  const limit = pLimit(WORKSPACE_CONCURRENCY);
  let reads = 0;
  await Promise.all(
    input.workspaces.map((cwd) =>
      limit(async () => {
        const facts = await input.deps.getCheckoutSnapshotFacts(cwd, {
          paseoHome: input.paseoHome,
        });
        const context = { paseoHome: input.paseoHome, facts };
        const status = await input.deps.getCheckoutStatus(cwd, context);
        const shortstat = await input.deps.getCheckoutShortstat(cwd, context, { force: true });
        await input.deps.getCheckoutWorktreeState(cwd, context);
        reads += 4;
        if (facts.isGit) {
          await input.deps.getCheckoutRefDerivedState(
            cwd,
            facts,
            { aheadBehind: status.isGit ? status.aheadBehind : null, diffStat: shortstat },
            new Set<string>(),
            context,
          );
          reads += 1;
        }
      }),
    ),
  );
  return reads;
}

async function measureSweep(input: {
  label: string;
  deps: SweepDeps;
  workspaces: string[];
  paseoHome: string;
}): Promise<SweepResult> {
  let reads = 0;
  const startedAt = Date.now();
  const measured = await measureMainThreadWork(input.label, async () => {
    reads = await sweep(input);
    return reads;
  });
  return {
    blockingMs: Math.round(measured.blockingMs),
    filesystemRequests: measured.filesystemRequests,
    yields: measured.yields,
    elapsedMs: Date.now() - startedAt,
    reads,
  };
}

async function main(): Promise<void> {
  const workspaceCount = readInteger(process.env.PASEO_MEASURE_WORKSPACES, 50);
  const rounds = readInteger(process.env.PASEO_MEASURE_ROUNDS, 2);
  const root = await mkdtemp(path.join(tmpdir(), "paseo-git-worker-measurement-"));
  const workerDeps = createWorkerGitDeps({ size: 2 });
  try {
    const workspaces: string[] = [];
    for (let index = 0; index < workspaceCount; index += 1) {
      workspaces.push(await createWorkspace(root, index));
    }
    const paseoHome = path.join(root, "paseo-home");
    await mkdir(paseoHome, { recursive: true });

    const inThreadDeps: SweepDeps = {
      getCheckoutSnapshotFacts,
      getCheckoutStatus,
      getCheckoutShortstat,
      getCheckoutWorktreeState,
      getCheckoutRefDerivedState,
    };

    // Warm both paths first so the comparison is not a cold-page-cache artefact.
    await sweep({ deps: inThreadDeps, workspaces, paseoHome });
    await sweep({ deps: workerDeps, workspaces, paseoHome });

    const comparisons: Array<{ round: number; inThread: SweepResult; worker: SweepResult }> = [];
    for (let round = 1; round <= rounds; round += 1) {
      const inThread = await measureSweep({
        label: `round ${round} in-thread`,
        deps: inThreadDeps,
        workspaces,
        paseoHome,
      });
      const worker = await measureSweep({
        label: `round ${round} worker`,
        deps: workerDeps,
        workspaces,
        paseoHome,
      });
      comparisons.push({ round, inThread, worker });
    }

    const summary = {
      workspaces: workspaceCount,
      rounds,
      inThreadBlockingMs: Math.min(
        ...comparisons.map((comparison) => comparison.inThread.blockingMs),
      ),
      workerBlockingMs: Math.min(...comparisons.map((comparison) => comparison.worker.blockingMs)),
      inThreadElapsedMs: Math.min(
        ...comparisons.map((comparison) => comparison.inThread.elapsedMs),
      ),
      workerElapsedMs: Math.min(...comparisons.map((comparison) => comparison.worker.elapsedMs)),
      workerStats: workerDeps.workerStats(),
    };
    const report = { generatedAt: new Date().toISOString(), summary, comparisons };
    process.stdout.write(`GIT_WORKER_MEASUREMENT ${JSON.stringify(report)}\n`);
    process.stdout.write(
      `main-thread blocking for ${workspaceCount} workspaces: in-thread ${summary.inThreadBlockingMs}ms, worker ${summary.workerBlockingMs}ms\n`,
    );
  } finally {
    await workerDeps.disposeDeps();
    await rm(root, { recursive: true, force: true, maxRetries: 5 });
  }
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
  process.exitCode = 1;
});
