import { describe, expect, it, vi } from "vitest";
import type { GitProcessPolicy } from "../../utils/git-process-scheduler.js";
import {
  GitWorkerPool,
  GitWorkerQueueFullError,
  resolveGitWorkerCount,
  resolveGitWorkerEntry,
  shareGitProcessPolicy,
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
}

function createFakeWorker(behaviour: FakeWorkerBehaviour = {}): GitWorkerEntry {
  const replyWith = behaviour.replyWithWorkerData
    ? "workerData"
    : "{ cwd: request.input.cwd, attempt }";
  return {
    eval: true,
    script: `
const { parentPort, workerData } = require("node:worker_threads");
const holdAttempts = ${JSON.stringify(behaviour.holdAttempts ?? [])};
const exitAttempts = ${JSON.stringify(behaviour.exitAttempts ?? [])};
const failWith = ${JSON.stringify(behaviour.failWith ?? null)};
const attempt = (workerData && workerData.attempt) || 0;
parentPort.on("message", (request) => {
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

  it("divides the daemon Git process policy between the pool's threads", () => {
    const policy: GitProcessPolicy = { maxProcessConcurrency: 8, maxProcessesPerSecond: 64 };
    expect(shareGitProcessPolicy(policy, 2)).toEqual({
      maxProcessConcurrency: 4,
      maxProcessesPerSecond: 32,
    });
    expect(shareGitProcessPolicy(policy, 4)).toEqual({
      maxProcessConcurrency: 2,
      maxProcessesPerSecond: 16,
    });
    expect(
      shareGitProcessPolicy({ maxProcessConcurrency: 1, maxProcessesPerSecond: 1 }, 4),
    ).toEqual({ maxProcessConcurrency: 1, maxProcessesPerSecond: 1 });
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
        gitProcessPolicy: shareGitProcessPolicy(
          { maxProcessConcurrency: 8, maxProcessesPerSecond: 64 },
          2,
        ),
      }),
    });
    try {
      const data = await pool.run({ op: "snapshotFacts", input }, () => fallbackValue(""));
      expect(data).toEqual({
        gitProcessPolicy: { maxProcessConcurrency: 4, maxProcessesPerSecond: 32 },
      });
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

  it("retires a silent worker on timeout and serves the next request from a fresh one", async () => {
    const pool = createPool({
      entry: createFakeWorker({ holdAttempts: [0] }),
      workerData: createAttempts(),
      requestTimeoutMs: 100,
    });
    try {
      const error = await pool
        .run({ op: "snapshotFacts", input }, () => fallbackValue(""))
        .catch((cause: unknown) => cause);
      expect(error).toMatchObject({ name: "GitWorkerRequestTimeoutError" });
      expect((error as Error).message).toContain("snapshotFacts timed out after 100ms");
      expect(pool.getStats()).toMatchObject({ dispatched: 1, retirements: 1, workers: 0 });

      await expect(
        pool.run({ op: "checkoutStatus", input }, () => fallbackValue("")),
      ).resolves.toEqual({ cwd: "/tmp/workspace", attempt: 1 });
      expect(pool.getStats()).toMatchObject({
        dispatched: 2,
        retirements: 1,
        workers: 1,
        fallbacks: 0,
      });
    } finally {
      await pool.dispose();
    }
  });

  it("rejects the in-flight request when the worker exits, then respawns", async () => {
    const pool = createPool({
      entry: createFakeWorker({ exitAttempts: [0] }),
      workerData: createAttempts(),
    });
    try {
      await expect(
        pool.run({ op: "shortstat", input: { ...input, force: true } }, () => fallbackValue("")),
      ).rejects.toThrow(/Git worker exited/);
      await expect(
        pool.run({ op: "worktreeState", input }, () => fallbackValue("")),
      ).resolves.toEqual({ cwd: "/tmp/workspace", attempt: 1 });
      expect(pool.getStats()).toMatchObject({ dispatched: 2, retirements: 1, fallbacks: 0 });
    } finally {
      await pool.dispose();
    }
  });

  it("falls back in-thread once workers fail repeatedly, logging once", async () => {
    const logger = { warn: vi.fn() };
    const pool = createPool({
      entry: createFakeWorker({ exitAttempts: [0, 1, 2, 3] }),
      workerData: createAttempts(),
      logger,
    });
    try {
      for (let attempt = 0; attempt < 3; attempt += 1) {
        await expect(
          pool.run({ op: "snapshotFacts", input }, () => fallbackValue(input.cwd)),
        ).rejects.toThrow(/Git worker exited/);
      }
      await expect(
        pool.run({ op: "snapshotFacts", input }, () => fallbackValue(input.cwd)),
      ).resolves.toEqual({ cwd: "/tmp/workspace", fallback: true });
      expect(pool.getStats()).toMatchObject({ enabled: false, fallbacks: 1 });

      const messages = logger.warn.mock.calls.map((call) => String(call[1]));
      expect(messages.filter((message) => message.includes("failed repeatedly"))).toHaveLength(1);
    } finally {
      await pool.dispose();
    }
  });

  it("runs work that is still queued when the pool gives up in-thread", async () => {
    const logger = { warn: vi.fn() };
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
      expect(results.slice(0, 3).map((result) => result.status)).toEqual([
        "rejected",
        "rejected",
        "rejected",
      ]);
      expect(results[3]).toEqual({
        status: "fulfilled",
        value: { cwd: "/tmp/workspace", fallback: true },
      });
      expect(pool.getStats()).toMatchObject({ enabled: false, fallbacks: 1 });
    } finally {
      await pool.dispose();
    }
  });

  it("rejects work when the queue is full, logging the saturation once", async () => {
    const logger = { warn: vi.fn() };
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
        pool.run({ op: "shortstat", input: { ...input, force: true } }, () => fallbackValue("")),
      ).rejects.toBeInstanceOf(GitWorkerQueueFullError);
      expect(pool.getStats()).toMatchObject({ queued: 1, dropped: 1 });
      const messages = logger.warn.mock.calls.map((call) => String(call[1]));
      expect(messages.filter((message) => message.includes("queue is full"))).toHaveLength(1);
      await pool.dispose();
      await Promise.all([inFlight, queued]);
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
