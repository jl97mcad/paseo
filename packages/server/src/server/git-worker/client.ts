import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";
import { Worker } from "node:worker_threads";
import type pino from "pino";
import {
  getCheckoutRefDerivedState,
  getCheckoutShortstat,
  getCheckoutSnapshotFacts,
  getCheckoutStatus,
  getCheckoutWorktreeState,
  type CheckoutContext,
} from "../../utils/checkout-git.js";
import {
  resolveGitProcessPolicy,
  type GitProcessPolicy,
} from "../../utils/git-process-scheduler.js";
import type { WorkspaceGitServiceDependencies } from "../workspace-git-service.js";
import {
  reviveGitWorkerError,
  type GitWorkerCheckoutInput,
  type GitWorkerOutputs,
  type GitWorkerReply,
  type GitWorkerTask,
} from "./protocol.js";

export const DEFAULT_GIT_WORKER_COUNT = 2;
export const DEFAULT_GIT_WORKER_REQUEST_TIMEOUT_MS = 30_000;
export const DEFAULT_GIT_WORKER_QUEUE_LIMIT = 256;
/** Consecutive failures this close together mean the worker cannot hold work. */
const RAPID_FAILURE_WINDOW_MS = 5_000;
const RAPID_FAILURE_LIMIT = 3;
/** How a worker entry is started: an ordinary file/URL, or eval'd loader source. */
export interface GitWorkerEntry {
  script: string | URL;
  eval: boolean;
}

export interface GitWorkerStats {
  size: number;
  enabled: boolean;
  workers: number;
  inFlight: number;
  queued: number;
  dispatched: number;
  fallbacks: number;
  dropped: number;
  retirements: number;
}

export class GitWorkerQueueFullError extends Error {
  constructor(readonly limit: number) {
    super(`Git worker queue is full (${limit} requests waiting)`);
    this.name = "GitWorkerQueueFullError";
  }
}

export class GitWorkerRequestTimeoutError extends Error {
  constructor(
    readonly operation: string,
    readonly waitedMs: number,
  ) {
    super(`Git worker ${operation} timed out after ${waitedMs}ms`);
    this.name = "GitWorkerRequestTimeoutError";
  }
}

/**
 * Resolve the worker entry the same way for dist, tsx development and vitest:
 * compiled output imports the emitted `.js`, source registers tsx first. Mirrors
 * the plugin compiler; see src/server/plugins/compiler.ts.
 */
export function resolveGitWorkerEntry(): GitWorkerEntry {
  const source = import.meta.url.endsWith(".ts");
  const workerUrl = new URL(source ? "./worker.ts" : "./worker.js", import.meta.url);
  if (!source) {
    return { script: workerUrl, eval: false };
  }
  const tsxApi = pathToFileURL(createRequire(import.meta.url).resolve("tsx/esm/api")).href;
  return {
    script: `import(${JSON.stringify(tsxApi)}).then(({ register }) => { register(); return import(${JSON.stringify(workerUrl.href)}); });`,
    eval: true,
  };
}

/** `PASEO_GIT_WORKER_COUNT` accepts a non-negative integer; 0 keeps git on main. */
export function resolveGitWorkerCount(env: NodeJS.ProcessEnv): number {
  const raw = env.PASEO_GIT_WORKER_COUNT?.trim();
  if (!raw) {
    return DEFAULT_GIT_WORKER_COUNT;
  }
  const parsed = Number(raw);
  return Number.isInteger(parsed) && parsed >= 0 ? parsed : DEFAULT_GIT_WORKER_COUNT;
}

/** Split the daemon's Git process cap so the pool's threads share it. */
export function shareGitProcessPolicy(
  policy: GitProcessPolicy,
  poolSize: number,
): GitProcessPolicy {
  const share = Math.max(1, poolSize);
  return {
    maxProcessConcurrency: Math.max(1, Math.floor(policy.maxProcessConcurrency / share)),
    maxProcessesPerSecond: Math.max(1, Math.floor(policy.maxProcessesPerSecond / share)),
  };
}

interface QueuedRequest {
  request: GitWorkerTask & { id: number };
  fallback: () => Promise<unknown>;
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
}

interface InFlightRequest {
  queued: QueuedRequest;
  timer: ReturnType<typeof setTimeout>;
}

interface WorkerSlot {
  worker: Worker;
}

export interface GitWorkerPoolOptions {
  size: number;
  entry?: GitWorkerEntry;
  logger?: Pick<pino.Logger, "warn">;
  requestTimeoutMs?: number;
  queueLimit?: number;
  /** Structured-clone data handed to every worker; the Git policy share lives here. */
  workerData?: () => unknown;
}

/**
 * One thread per concurrent read, a bounded queue in front of them, and the
 * in-thread implementation as the fallback. A worker that errors, exits or
 * times out is retired and replaced lazily; repeated rapid failures disable the
 * pool so snapshots degrade to today's behaviour instead of failing.
 */
export class GitWorkerPool {
  private readonly slots = new Set<WorkerSlot>();
  private readonly inFlight = new Map<WorkerSlot, InFlightRequest>();
  private readonly queue: QueuedRequest[] = [];
  private readonly size: number;
  private readonly entry: GitWorkerEntry;
  private readonly logger: Pick<pino.Logger, "warn"> | undefined;
  private readonly requestTimeoutMs: number;
  private readonly queueLimit: number;
  private readonly workerData: (() => unknown) | undefined;
  private nextId = 1;
  private disposed = false;
  private unavailable = false;
  private disposePromise: Promise<void> | null = null;
  private rapidFailures = 0;
  private lastFailureAtMs = 0;
  private saturationLogged = false;
  private saturatedDrops = 0;
  private dispatched = 0;
  private fallbacks = 0;
  private dropped = 0;
  private retirements = 0;

  constructor(options: GitWorkerPoolOptions) {
    this.size = Math.max(0, options.size);
    this.entry = options.entry ?? resolveGitWorkerEntry();
    this.logger = options.logger;
    this.requestTimeoutMs = options.requestTimeoutMs ?? DEFAULT_GIT_WORKER_REQUEST_TIMEOUT_MS;
    this.queueLimit = options.queueLimit ?? DEFAULT_GIT_WORKER_QUEUE_LIMIT;
    this.workerData = options.workerData;
  }

  getStats(): GitWorkerStats {
    return {
      size: this.size,
      enabled: this.size > 0 && !this.disposed && !this.unavailable,
      workers: this.slots.size,
      inFlight: this.inFlight.size,
      queued: this.queue.length,
      dispatched: this.dispatched,
      fallbacks: this.fallbacks,
      dropped: this.dropped,
      retirements: this.retirements,
    };
  }

  run<K extends GitWorkerTask["op"]>(
    task: Extract<GitWorkerTask, { op: K }>,
    fallback: () => Promise<GitWorkerOutputs[K]>,
  ): Promise<GitWorkerOutputs[K]> {
    if (this.disposed || this.unavailable || this.size === 0) {
      this.fallbacks += 1;
      return fallback();
    }
    if (this.queue.length >= this.queueLimit) {
      this.dropped += 1;
      this.saturatedDrops += 1;
      this.logSaturation();
      return Promise.reject(new GitWorkerQueueFullError(this.queueLimit));
    }
    return new Promise<GitWorkerOutputs[K]>((resolve, reject) => {
      this.queue.push({
        request: { ...task, id: this.nextId++ } as QueuedRequest["request"],
        fallback: fallback as () => Promise<unknown>,
        resolve: resolve as (value: unknown) => void,
        reject,
      });
      this.pump();
    });
  }

  dispose(): Promise<void> {
    if (this.disposePromise) {
      return this.disposePromise;
    }
    this.disposed = true;
    const pending = this.queue.splice(0);
    for (const queued of pending) {
      queued.reject(new Error("Git worker pool is disposed"));
    }
    const slots = [...this.slots];
    this.slots.clear();
    const inFlight = [...this.inFlight.values()];
    this.inFlight.clear();
    for (const entry of inFlight) {
      clearTimeout(entry.timer);
      entry.queued.reject(new Error("Git worker pool is disposed"));
    }
    this.disposePromise = Promise.all(slots.map((slot) => slot.worker.terminate())).then(
      () => undefined,
    );
    return this.disposePromise;
  }

  private pump(): void {
    if (this.disposed) {
      return;
    }
    while (this.queue.length > 0) {
      const slot = this.acquireSlot();
      if (!slot) {
        // Nothing alive: a spawn failed outright, so run the rest on main.
        if (this.slots.size === 0) {
          this.drainToFallback();
        }
        return;
      }
      const queued = this.queue.shift()!;
      this.dispatch(slot, queued);
    }
    this.logDrain();
  }

  private acquireSlot(): WorkerSlot | null {
    for (const slot of this.slots) {
      if (!this.inFlight.has(slot)) {
        return slot;
      }
    }
    if (this.slots.size >= this.size) {
      return null;
    }
    return this.spawnSlot();
  }

  private spawnSlot(): WorkerSlot | null {
    let worker: Worker;
    try {
      worker = new Worker(this.entry.script, {
        eval: this.entry.eval,
        execArgv: [],
        ...(this.workerData ? { workerData: this.workerData() } : {}),
      });
    } catch (error) {
      this.noteFailure(error);
      return null;
    }
    const slot: WorkerSlot = { worker };
    this.slots.add(slot);
    worker.unref();
    worker.on("message", (reply: GitWorkerReply) => this.onMessage(slot, reply));
    worker.on("error", (error: Error) => this.onWorkerFailure(slot, error));
    worker.on("exit", (code: number) =>
      this.onWorkerFailure(slot, new Error(`Git worker exited (code ${code})`)),
    );
    return slot;
  }

  private dispatch(slot: WorkerSlot, queued: QueuedRequest): void {
    this.dispatched += 1;
    const timer = setTimeout(() => this.onTimeout(slot), this.requestTimeoutMs);
    this.inFlight.set(slot, { queued, timer });
    slot.worker.ref();
    slot.worker.postMessage(queued.request, []);
  }

  private onMessage(slot: WorkerSlot, reply: GitWorkerReply): void {
    const inFlight = this.inFlight.get(slot);
    if (!inFlight || inFlight.queued.request.id !== reply.id) {
      return;
    }
    this.finish(slot, inFlight);
    if (reply.ok) {
      this.rapidFailures = 0;
      inFlight.queued.resolve(reply.value);
    } else {
      // A failed read (no repository, missing ref) is not a broken worker.
      inFlight.queued.reject(reviveGitWorkerError(reply.error));
    }
    this.pump();
  }

  private onTimeout(slot: WorkerSlot): void {
    const inFlight = this.inFlight.get(slot);
    if (!inFlight) {
      return;
    }
    this.finish(slot, inFlight);
    const error = new GitWorkerRequestTimeoutError(
      inFlight.queued.request.op,
      this.requestTimeoutMs,
    );
    this.retire(slot, error);
    inFlight.queued.reject(error);
  }

  private onWorkerFailure(slot: WorkerSlot, error: Error): void {
    if (!this.slots.has(slot)) {
      return;
    }
    const inFlight = this.inFlight.get(slot);
    if (inFlight) {
      this.finish(slot, inFlight);
      inFlight.queued.reject(error);
    }
    this.retire(slot, error);
  }

  private finish(slot: WorkerSlot, inFlight: InFlightRequest): void {
    clearTimeout(inFlight.timer);
    this.inFlight.delete(slot);
    if (this.slots.has(slot)) {
      slot.worker.unref();
    }
  }

  private retire(slot: WorkerSlot, error: Error): void {
    this.slots.delete(slot);
    this.retirements += 1;
    void slot.worker.terminate();
    this.noteFailure(error);
    this.pump();
  }

  private noteFailure(cause: unknown): void {
    const now = Date.now();
    this.rapidFailures =
      now - this.lastFailureAtMs <= RAPID_FAILURE_WINDOW_MS ? this.rapidFailures + 1 : 1;
    this.lastFailureAtMs = now;
    if (this.rapidFailures >= RAPID_FAILURE_LIMIT && !this.unavailable) {
      this.unavailable = true;
      this.logger?.warn(
        { err: cause, failures: this.rapidFailures },
        "Git worker pool failed repeatedly; reading Git on the main thread instead",
      );
      this.drainToFallback();
      return;
    }
    this.logger?.warn({ err: cause }, "Retired a Git worker");
  }

  private drainToFallback(): void {
    const queued = this.queue.splice(0);
    for (const entry of queued) {
      this.fallbacks += 1;
      void entry.fallback().then(entry.resolve, entry.reject);
    }
  }

  private logSaturation(): void {
    if (this.saturationLogged) {
      return;
    }
    this.saturationLogged = true;
    this.logger?.warn(
      { limit: this.queueLimit, size: this.size },
      "Git worker queue is full; rejecting reads until it drains",
    );
  }

  private logDrain(): void {
    if (!this.saturationLogged) {
      return;
    }
    this.saturationLogged = false;
    this.logger?.warn({ dropped: this.saturatedDrops }, "Git worker queue drained");
    this.saturatedDrops = 0;
  }
}

export interface WorkerGitDepsOptions {
  logger?: Pick<pino.Logger, "warn">;
  env?: NodeJS.ProcessEnv;
  size?: number;
  requestTimeoutMs?: number;
  queueLimit?: number;
  entry?: GitWorkerEntry;
  getGitProcessPolicy?: () => GitProcessPolicy;
}

export type WorkerGitDeps = Pick<
  WorkspaceGitServiceDependencies,
  | "getCheckoutSnapshotFacts"
  | "getCheckoutStatus"
  | "getCheckoutShortstat"
  | "getCheckoutWorktreeState"
  | "getCheckoutRefDerivedState"
> & {
  disposeDeps: () => Promise<void>;
  workerStats: () => GitWorkerStats;
};

export function createWorkerGitDeps(options: WorkerGitDepsOptions = {}): WorkerGitDeps {
  const env = options.env ?? process.env;
  const size = options.size ?? resolveGitWorkerCount(env);
  const resolvePolicy = options.getGitProcessPolicy ?? (() => resolveGitProcessPolicy({ env }));
  const pool = new GitWorkerPool({
    size,
    logger: options.logger,
    entry: options.entry,
    requestTimeoutMs: options.requestTimeoutMs,
    queueLimit: options.queueLimit,
    workerData:
      size > 0
        ? () => ({ gitProcessPolicy: shareGitProcessPolicy(resolvePolicy(), size) })
        : undefined,
  });

  return {
    getCheckoutSnapshotFacts: (cwd, context) =>
      pool.run({ op: "snapshotFacts", input: toCheckoutInput(cwd, context) }, () =>
        getCheckoutSnapshotFacts(cwd, context),
      ),
    getCheckoutStatus: (cwd, context) =>
      pool.run({ op: "checkoutStatus", input: toCheckoutInput(cwd, context) }, () =>
        getCheckoutStatus(cwd, context),
      ),
    getCheckoutShortstat: (cwd, context, readOptions) =>
      pool.run(
        {
          op: "shortstat",
          input: { ...toCheckoutInput(cwd, context), force: readOptions?.force === true },
        },
        () => getCheckoutShortstat(cwd, context, readOptions),
      ),
    getCheckoutWorktreeState: (cwd, context) =>
      pool.run({ op: "worktreeState", input: toCheckoutInput(cwd, context) }, () =>
        getCheckoutWorktreeState(cwd, context),
      ),
    getCheckoutRefDerivedState: (cwd, facts, current, movedRemoteRefs, context) =>
      pool.run(
        {
          op: "refDerived",
          input: { ...toCheckoutInput(cwd, context), facts, current, movedRemoteRefs },
        },
        () => getCheckoutRefDerivedState(cwd, facts, current, movedRemoteRefs, context),
      ),
    disposeDeps: () => pool.dispose(),
    workerStats: () => pool.getStats(),
  };
}

function toCheckoutInput(cwd: string, context?: CheckoutContext): GitWorkerCheckoutInput {
  return {
    cwd,
    paseoHome: context?.paseoHome,
    worktreesRoot: context?.worktreesRoot,
    facts: context?.facts ?? null,
  };
}
