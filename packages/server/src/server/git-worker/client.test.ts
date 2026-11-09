import { describe, expect, it, vi } from "vitest";
import { mainGitProcessPolicy, splitGitProcessPolicy } from "../../utils/git-process-scheduler.js";
import { runWithGitCommandPriority } from "../../utils/run-git-command.js";
import {
  GitWorkerPool,
  resolveGitWorkerCount,
  resolveGitWorkerEntry,
  type GitWorkerEntry,
  type GitWorkerPoolOptions,
} from "./client.js";
import type { GitWorkerCheckoutInput } from "./protocol.js";

const input: GitWorkerCheckoutInput = { cwd: "/tmp/workspace", facts: null };

interface FakeWorkerBehaviour {
  /** Thread attempts (from workerData) that never answer. */
  holdAttempts?: number[];
  /** Thread attempts that exit the moment they receive a request. */
  exitAttempts?: number[];
  /** Answer with an error reply instead of a value. */
  failWith?: { name: string; message: string; code?: string };
  /** Reply with the worker's own workerData instead of a request echo. */
  replyWithWorkerData?: boolean;
  /** Echo the request's operation and priority instead of a canned value. */
  echoRequest?: boolean;
  /** Set `[0] = 1` on the received cancel, then confirm it. */
  cancelFlags?: Int32Array;
  /** Receive the cancel but never confirm it. */
  ignoreCancel?: boolean;
  /** Record the `[concurrency, rate]` of every policy message it receives. */
  policyRecords?: Int32Array;
  /** Post this log event before answering any request. */
  logWith?: { level: "warn" | "error"; message: string; fields?: Record<string, unknown> };
}

function createFakeWorker(behaviour: FakeWorkerBehaviour = {}): GitWorkerEntry {
  const replyWith = replyExpression(behaviour);
  return {
    eval: true,
    script: `
const { parentPort, workerData } = require("node:worker_threads");
const holdAttempts = ${JSON.stringify(behaviour.holdAttempts ?? [])};
const exitAttempts = ${JSON.stringify(behaviour.exitAttempts ?? [])};
const failWith = ${JSON.stringify(behaviour.failWith ?? null)};
const logWith = ${JSON.stringify(behaviour.logWith ?? null)};
const ignoreCancel = ${behaviour.ignoreCancel === true};
const cancelFlags = (workerData && workerData.cancelFlags) || null;
const policyRecords = (workerData && workerData.policyRecords) || null;
const attempt = (workerData && workerData.attempt) || 0;
parentPort.on("message", (request) => {
  if (request && request.type === "cancel") {
    if (cancelFlags) {
      cancelFlags[0] = 1;
    }
    if (!ignoreCancel) {
      parentPort.postMessage({ type: "cancelled", id: request.id });
    }
    return;
  }
  if (request && request.type === "policy") {
    if (policyRecords) {
      policyRecords[0] = request.policy.maxProcessConcurrency;
      policyRecords[1] = request.policy.maxProcessesPerSecond;
    }
    return;
  }
  if (logWith) {
    parentPort.postMessage({ type: "log", ...logWith });
  }
  if (exitAttempts.includes(attempt)) {
    process.exit(0);
  }
  if (holdAttempts.includes(attempt)) {
    return;
  }
  if (failWith) {
    parentPort.postMessage({ id: request.id, ok: false, error: failWith });
    return;
  }
  parentPort.postMessage({ id: request.id, ok: true, value: ${replyWith} });
});
`,
  };
}

/** Fresh attempt number per worker the pool spawns. */
function createAttempts(): () => { attempt: number } {
  let attempt = 0;
  return () => ({ attempt: attempt++ });
}

function createPool(options: Partial<GitWorkerPoolOptions> = {}): GitWorkerPool {
  return new GitWorkerPool({
    size: 1,
    entry: createFakeWorker(),
    ...options,
  });
}

function fallbackValue(cwd: string): Promise<{ cwd: string; fallback: true }> {
  return Promise.resolve({ cwd, fallback: true });
}

function replyExpression(behaviour: FakeWorkerBehaviour): string {
  if (behaviour.replyWithWorkerData) {
    return "workerData";
  }
  if (behaviour.echoRequest) {
    return "{ op: request.op, priority: request.priority }";
  }
  return "{ cwd: request.input.cwd, attempt }";
}

function readSnapshot(pool: GitWorkerPool): Promise<unknown> {
  return pool.run({ op: "snapshotFacts", input }, () => fallbackValue(""));
}

function readStatus(pool: GitWorkerPool): Promise<unknown> {
  return pool.run({ op: "checkoutStatus", input }, () => fallbackValue(""));
}

function readShortstat(pool: GitWorkerPool): Promise<unknown> {
  return pool.run({ op: "shortstat", input: { ...input, force: true } }, () => fallbackValue(""));
}

function readWorktreeState(pool: GitWorkerPool): Promise<unknown> {
  return pool.run({ op: "worktreeState", input }, () => fallbackValue(""));
}

function readAtHighPriority(read: () => Promise<unknown>): Promise<unknown> {
  return runWithGitCommandPriority("high", read);
}

describe("git worker entry resolution", () => {
  it("registers tsx for source and points at the compiled entry for dist", () => {
    const entry = resolveGitWorkerEntry();
    // Tests run from source, where the entry is an eval'd tsx registration.
    expect(entry.eval).toBe(true);
    expect(String(entry.script)).toContain("worker.ts");
    expect(String(entry.script)).toContain("/tsx/");
    expect(String(entry.script)).toContain("register()");
  });

  it("parses a non-negative worker count and keeps the default otherwise", () => {
    expect(resolveGitWorkerCount({})).toBe(2);
    expect(resolveGitWorkerCount({ PASEO_GIT_WORKER_COUNT: "0" })).toBe(0);
    expect(resolveGitWorkerCount({ PASEO_GIT_WORKER_COUNT: "3" })).toBe(3);
    expect(resolveGitWorkerCount({ PASEO_GIT_WORKER_COUNT: "-1" })).toBe(2);
    expect(resolveGitWorkerCount({ PASEO_GIT_WORKER_COUNT: "many" })).toBe(2);
  });

  it("gives main the larger half of the cap and the workers the other half", () => {
    const full = { maxProcessConcurrency: 8, maxProcessesPerSecond: 64 };
    expect(splitGitProcessPolicy(full, 1)).toEqual({
      main: { maxProcessConcurrency: 4, maxProcessesPerSecond: 32 },
      worker: { maxProcessConcurrency: 4, maxProcessesPerSecond: 32 },
    });
    expect(splitGitProcessPolicy(full, 2)).toEqual({
      main: { maxProcessConcurrency: 4, maxProcessesPerSecond: 32 },
      worker: { maxProcessConcurrency: 2, maxProcessesPerSecond: 16 },
    });
    expect(splitGitProcessPolicy(full, 4)).toEqual({
      main: { maxProcessConcurrency: 4, maxProcessesPerSecond: 32 },
      worker: { maxProcessConcurrency: 1, maxProcessesPerSecond: 8 },
    });
    // Small caps keep one process and one start per second on each side.
    expect(
      splitGitProcessPolicy({ maxProcessConcurrency: 2, maxProcessesPerSecond: 2 }, 2),
    ).toEqual({
      main: { maxProcessConcurrency: 1, maxProcessesPerSecond: 1 },
      worker: { maxProcessConcurrency: 1, maxProcessesPerSecond: 1 },
    });
    expect(
      splitGitProcessPolicy({ maxProcessConcurrency: 1, maxProcessesPerSecond: 1 }, 1),
    ).toEqual({
      main: { maxProcessConcurrency: 1, maxProcessesPerSecond: 1 },
      worker: { maxProcessConcurrency: 1, maxProcessesPerSecond: 1 },
    });
  });

  it("keeps the whole cap on main while the pool is off", () => {
    const full = { maxProcessConcurrency: 8, maxProcessesPerSecond: 64 };
    expect(mainGitProcessPolicy(full, 0)).toEqual(full);
    expect(mainGitProcessPolicy(full, 2)).toEqual({
      maxProcessConcurrency: 4,
      maxProcessesPerSecond: 32,
    });
  });
});

describe("git worker pool", () => {
  it("round trips a plain-JSON request and leaves the worker reusable", async () => {
    const pool = createPool();
    try {
      const first = await pool.run({ op: "snapshotFacts", input }, () => fallbackValue(input.cwd));
      const second = await pool.run({ op: "checkoutStatus", input }, () =>
        fallbackValue(input.cwd),
      );
      expect(first).toEqual({ cwd: "/tmp/workspace", attempt: 0 });
      expect(second).toEqual({ cwd: "/tmp/workspace", attempt: 0 });
      expect(pool.getStats()).toMatchObject({
        dispatched: 2,
        fallbacks: 0,
        retirements: 0,
        workers: 1,
      });
    } finally {
      await pool.dispose();
    }
  });

  it("hands each worker its share of the Git process policy", async () => {
    const pool = createPool({
      size: 2,
      entry: createFakeWorker({ replyWithWorkerData: true }),
      workerData: () => ({
        gitProcessPolicy: splitGitProcessPolicy(
          { maxProcessConcurrency: 8, maxProcessesPerSecond: 64 },
          2,
        ).worker,
      }),
    });
    try {
      const data = await pool.run({ op: "snapshotFacts", input }, () => fallbackValue(""));
      expect(data).toEqual({
        gitProcessPolicy: { maxProcessConcurrency: 2, maxProcessesPerSecond: 16 },
      });
    } finally {
      await pool.dispose();
    }
  });

  it("sends the caller's Git priority with the request", async () => {
    const pool = createPool({ entry: createFakeWorker({ echoRequest: true }) });
    try {
      const high = await readAtHighPriority(() => readSnapshot(pool));
      const normal = await readStatus(pool);
      expect(high).toEqual({ op: "snapshotFacts", priority: "high" });
      expect(normal).toEqual({ op: "checkoutStatus", priority: "normal" });
    } finally {
      await pool.dispose();
    }
  });

  it("serves a high-priority request ahead of the normal requests already queued", async () => {
    const pool = createPool({
      entry: createFakeWorker({ holdAttempts: [0], echoRequest: true }),
      workerData: createAttempts(),
      requestTimeoutMs: 100,
    });
    const served: string[] = [];
    const record = (promise: Promise<unknown>) =>
      promise.then(
        (value) => {
          // The held request finishes in-thread, so only worker replies carry an op.
          const op = (value as { op?: string }).op;
          if (op) served.push(op);
          return undefined;
        },
        () => undefined,
      );
    try {
      // The first request holds the only slot, so the rest wait in the queue.
      const held = record(readSnapshot(pool));
      const firstNormal = record(readStatus(pool));
      const secondNormal = record(readShortstat(pool));
      const high = record(readAtHighPriority(() => readWorktreeState(pool)));

      await Promise.all([held, firstNormal, secondNormal, high]);
      expect(served).toEqual(["worktreeState", "checkoutStatus", "shortstat"]);
    } finally {
      await pool.dispose();
    }
  });

  it("cancels a timed-out request in the worker before terminating the thread", async () => {
    const cancelFlags = new Int32Array(new SharedArrayBuffer(4));
    const pool = createPool({
      entry: createFakeWorker({ holdAttempts: [0], cancelFlags }),
      workerData: () => ({ cancelFlags }),
      requestTimeoutMs: 100,
    });
    try {
      await expect(readSnapshot(pool)).resolves.toEqual({ cwd: "", fallback: true });
      await vi.waitFor(() => expect(cancelFlags[0]).toBe(1));
      expect(pool.getStats()).toMatchObject({ retirements: 1, fallbacks: 1 });
    } finally {
      await pool.dispose();
    }
  });

  it("terminates a worker that never confirms the cancel, bounded by the grace", async () => {
    const pool = createPool({
      entry: createFakeWorker({ holdAttempts: [0], ignoreCancel: true }),
      workerData: createAttempts(),
      requestTimeoutMs: 100,
      cancelGraceMs: 20,
    });
    try {
      await expect(readSnapshot(pool)).resolves.toEqual({ cwd: "", fallback: true });
      await expect(readStatus(pool)).resolves.toEqual({ cwd: "/tmp/workspace", attempt: 1 });
      expect(pool.getStats()).toMatchObject({ retirements: 1, workers: 1, fallbacks: 1 });
    } finally {
      await pool.dispose();
    }
  });

  it("preserves error name, message and code without retiring the worker", async () => {
    const pool = createPool({
      entry: createFakeWorker({
        failWith: {
          name: "NotGitRepoError",
          message: "Not a git repository: /tmp/workspace",
          code: "NOT_GIT_REPO",
        },
      }),
    });
    try {
      const error = await pool
        .run({ op: "snapshotFacts", input }, () => fallbackValue(""))
        .catch((cause: unknown) => cause);
      expect(error).toBeInstanceOf(Error);
      expect(error).toMatchObject({
        name: "NotGitRepoError",
        message: "Not a git repository: /tmp/workspace",
        code: "NOT_GIT_REPO",
      });
      expect(pool.getStats()).toMatchObject({ retirements: 0, workers: 1 });
      await expect(
        pool.run({ op: "snapshotFacts", input }, () => fallbackValue("")),
      ).rejects.toMatchObject({ name: "NotGitRepoError" });
    } finally {
      await pool.dispose();
    }
  });

  it("runs a slow read in-thread when the worker exceeds the request budget", async () => {
    const pool = createPool({
      entry: createFakeWorker({ holdAttempts: [0] }),
      workerData: createAttempts(),
      requestTimeoutMs: 100,
    });
    try {
      await expect(
        pool.run({ op: "snapshotFacts", input }, () => fallbackValue(input.cwd)),
      ).resolves.toEqual({ cwd: "/tmp/workspace", fallback: true });
      expect(pool.getStats()).toMatchObject({ dispatched: 1, retirements: 1, fallbacks: 1 });

      await expect(
        pool.run({ op: "checkoutStatus", input }, () => fallbackValue("")),
      ).resolves.toEqual({ cwd: "/tmp/workspace", attempt: 1 });
      expect(pool.getStats()).toMatchObject({
        dispatched: 2,
        retirements: 1,
        workers: 1,
        fallbacks: 1,
      });
    } finally {
      await pool.dispose();
    }
  });

  it("runs the in-flight read in-thread when the worker exits, then respawns", async () => {
    const pool = createPool({
      entry: createFakeWorker({ exitAttempts: [0] }),
      workerData: createAttempts(),
    });
    try {
      await expect(
        pool.run({ op: "shortstat", input: { ...input, force: true } }, () =>
          fallbackValue(input.cwd),
        ),
      ).resolves.toEqual({ cwd: "/tmp/workspace", fallback: true });
      await expect(
        pool.run({ op: "worktreeState", input }, () => fallbackValue("")),
      ).resolves.toEqual({ cwd: "/tmp/workspace", attempt: 1 });
      expect(pool.getStats()).toMatchObject({ dispatched: 2, retirements: 1, fallbacks: 1 });
    } finally {
      await pool.dispose();
    }
  });

  it("falls back in-thread once workers fail repeatedly, logging once", async () => {
    const logger = { warn: vi.fn(), error: vi.fn() };
    const pool = createPool({
      entry: createFakeWorker({ exitAttempts: [0, 1, 2, 3] }),
      workerData: createAttempts(),
      logger,
    });
    try {
      for (let attempt = 0; attempt < 4; attempt += 1) {
        await expect(
          pool.run({ op: "snapshotFacts", input }, () => fallbackValue(input.cwd)),
        ).resolves.toEqual({ cwd: "/tmp/workspace", fallback: true });
      }
      expect(pool.getStats()).toMatchObject({ enabled: false, fallbacks: 4 });

      const messages = logger.warn.mock.calls.map((call) => String(call[1]));
      expect(messages.filter((message) => message.includes("failed repeatedly"))).toHaveLength(1);
    } finally {
      await pool.dispose();
    }
  });

  it("tries worker threads again after the cooldown following rapid failures", async () => {
    const logger = { warn: vi.fn(), error: vi.fn() };
    const pool = createPool({
      entry: createFakeWorker({ exitAttempts: [0, 1, 2] }),
      workerData: createAttempts(),
      logger,
      recoveryCooldownMs: 20,
    });
    try {
      for (let attempt = 0; attempt < 3; attempt += 1) {
        await expect(
          pool.run({ op: "snapshotFacts", input }, () => fallbackValue(input.cwd)),
        ).resolves.toEqual({ cwd: "/tmp/workspace", fallback: true });
      }
      expect(pool.getStats()).toMatchObject({ enabled: false });

      await new Promise((resolve) => setTimeout(resolve, 25));

      await expect(
        pool.run({ op: "snapshotFacts", input }, () => fallbackValue(input.cwd)),
      ).resolves.toEqual({ cwd: "/tmp/workspace", attempt: 3 });
      expect(pool.getStats()).toMatchObject({ enabled: true, workers: 1 });
    } finally {
      await pool.dispose();
    }
  });

  it("runs work that is still queued when the pool gives up in-thread", async () => {
    const logger = { warn: vi.fn(), error: vi.fn() };
    const pool = createPool({
      entry: createFakeWorker({ exitAttempts: [0, 1, 2, 3] }),
      workerData: createAttempts(),
      logger,
    });
    try {
      const results = await Promise.allSettled([
        pool.run({ op: "snapshotFacts", input }, () => fallbackValue(input.cwd)),
        pool.run({ op: "checkoutStatus", input }, () => fallbackValue(input.cwd)),
        pool.run({ op: "shortstat", input: { ...input, force: true } }, () =>
          fallbackValue(input.cwd),
        ),
        pool.run({ op: "worktreeState", input }, () => fallbackValue(input.cwd)),
      ]);
      expect(results.map((result) => result.status)).toEqual([
        "fulfilled",
        "fulfilled",
        "fulfilled",
        "fulfilled",
      ]);
      for (const result of results) {
        expect(result).toEqual({
          status: "fulfilled",
          value: { cwd: "/tmp/workspace", fallback: true },
        });
      }
      expect(pool.getStats()).toMatchObject({ enabled: false, fallbacks: 4 });
    } finally {
      await pool.dispose();
    }
  });

  it("runs a saturated queue's reads in-thread, logging the saturation once", async () => {
    const logger = { warn: vi.fn(), error: vi.fn() };
    const pool = createPool({
      entry: createFakeWorker({ holdAttempts: [0] }),
      queueLimit: 1,
      logger,
    });
    try {
      const inFlight = pool
        .run({ op: "snapshotFacts", input }, () => fallbackValue(""))
        .catch(() => undefined);
      const queued = pool
        .run({ op: "checkoutStatus", input }, () => fallbackValue(""))
        .catch(() => undefined);
      await expect(
        pool.run({ op: "shortstat", input: { ...input, force: true } }, () =>
          fallbackValue(input.cwd),
        ),
      ).resolves.toEqual({ cwd: "/tmp/workspace", fallback: true });
      expect(pool.getStats()).toMatchObject({ queued: 1, dropped: 1, fallbacks: 1 });
      const messages = logger.warn.mock.calls.map((call) => String(call[1]));
      expect(messages.filter((message) => message.includes("queue is full"))).toHaveLength(1);
      await pool.dispose();
      await Promise.all([inFlight, queued]);
    } finally {
      await pool.dispose();
    }
  });

  it("posts a live policy change to the workers that are already running", async () => {
    const policyRecords = new Int32Array(new SharedArrayBuffer(8));
    const pool = createPool({
      entry: createFakeWorker({ policyRecords }),
      workerData: () => ({ policyRecords }),
    });
    try {
      await readSnapshot(pool);
      pool.updateGitProcessPolicy({ maxProcessConcurrency: 8, maxProcessesPerSecond: 64 });
      await vi.waitFor(() => expect(Array.from(policyRecords)).toEqual([4, 32]));
    } finally {
      await pool.dispose();
    }
  });

  it("posts worker warnings to the daemon logger and drops trace", async () => {
    const logger = { warn: vi.fn(), error: vi.fn() };
    const pool = createPool({
      entry: createFakeWorker({
        logWith: {
          level: "warn",
          message: "Git worktree discovery failed; treating directory as non-Git",
          fields: { cwd: "/tmp/workspace" },
        },
      }),
      logger,
    });
    try {
      await readSnapshot(pool);
      expect(logger.warn).toHaveBeenCalledWith(
        { cwd: "/tmp/workspace" },
        "Git worktree discovery failed; treating directory as non-Git",
      );
      expect(logger.error).not.toHaveBeenCalled();
    } finally {
      await pool.dispose();
    }
  });

  it("runs in-thread when the pool is disabled", async () => {
    const pool = createPool({ size: 0 });
    try {
      await expect(
        pool.run({ op: "snapshotFacts", input }, () => fallbackValue(input.cwd)),
      ).resolves.toEqual({ cwd: "/tmp/workspace", fallback: true });
      expect(pool.getStats()).toMatchObject({
        enabled: false,
        workers: 0,
        dispatched: 0,
        fallbacks: 1,
      });
    } finally {
      await pool.dispose();
    }
  });
});
