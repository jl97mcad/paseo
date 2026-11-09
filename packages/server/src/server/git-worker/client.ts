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
  type GitProcessPriority,
} from "../../utils/git-process-scheduler.js";
import { getGitCommandPriority } from "../../utils/run-git-command.js";
import type { WorkspaceGitServiceDependencies } from "../workspace-git-service.js";
import {
  isGitWorkerCancelConfirmation,
  reviveGitWorkerError,
  type GitWorkerCancel,
  type GitWorkerCheckoutInput,
  type GitWorkerOutputs,
  type GitWorkerReply,
  type GitWorkerResponse,
  type GitWorkerTask,
} from "./protocol.js";

export const DEFAULT_GIT_WORKER_COUNT = 2;
export const DEFAULT_GIT_WORKER_REQUEST_TIMEOUT_MS = 30_000;
export const DEFAULT_GIT_WORKER_QUEUE_LIMIT = 256;
/**
 * How long a retired worker may take to confirm that it stopped its Git
 * processes before the pool terminates the thread anyway.
 */
export const DEFAULT_GIT_WORKER_CANCEL_GRACE_MS = 1_000;
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
  request: GitWorkerTask & { id: number; priority: GitProcessPriority };
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
  exited: boolean;
  cancelWait?: { id: number; confirm: () => void };
}

export interface GitWorkerPoolOptions {
  size: number;
  entry?: GitWorkerEntry;
  logger?: Pick<pino.Logger, "warn">;
  requestTimeoutMs?: number;
  queueLimit?: number;
  cancelGraceMs?: number;
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
  // Two lanes: an archive waiting on a read does not queue behind the pollers'
  // background refreshes. The worker's Git scheduler uses the same split.
  private readonly highPriorityQueue: QueuedRequest[] = [];
  private readonly normalPriorityQueue: QueuedRequest[] = [];
  private readonly size: number;
  private readonly entry: GitWorkerEntry;
  private readonly logger: Pick<pino.Logger, "warn"> | undefined;
  private readonly requestTimeoutMs: number;
  private readonly queueLimit: number;
  private readonly cancelGraceMs: number;
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
    this.cancelGraceMs = options.cancelGraceMs ?? DEFAULT_GIT_WORKER_CANCEL_GRACE_MS;
    this.workerData = options.workerData;
  }

  private get queued(): number {
    return this.highPriorityQueue.length + this.normalPriorityQueue.length;
  }

  getStats(): GitWorkerStats {
    return {
      size: this.size,
      enabled: this.size > 0 && !this.disposed && !this.unavailable,
      workers: this.slots.size,
      inFlight: this.inFlight.size,
      queued: this.queued,
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
    if (this.queued >= this.queueLimit) {
      this.dropped += 1;
      this.saturatedDrops += 1;
      this.logSaturation();
      return Promise.reject(new GitWorkerQueueFullError(this.queueLimit));
    }
    return new Promise<GitWorkerOutputs[K]>((resolve, reject) => {
      const priority = getGitCommandPriority();
      const queued: QueuedRequest = {
        request: { ...task, id: this.nextId++, priority } as QueuedRequest["request"],
        fallback: fallback as () => Promise<unknown>,
        resolve: resolve as (value: unknown) => void,
        reject,
      };
      if (priority === "high") {
        this.highPriorityQueue.push(queued);
      } else {
        this.normalPriorityQueue.push(queued);
      }
      this.pump();
    });
  }

  dispose(): Promise<void> {
    if (this.disposePromise) {
      return this.disposePromise;
    }
    this.disposed = true;
    const pending = [...this.highPriorityQueue.splice(0), ...this.normalPriorityQueue.splice(0)];
    for (const queued of pending) {
      queued.reject(new Error("Git worker pool is disposed"));
    }
    const slots = [...this.slots];
    this.slots.clear();
    const requestIds = new Map<WorkerSlot, number>();
    for (const [slot, entry] of this.inFlight) {
      clearTimeout(entry.timer);
      requestIds.set(slot, entry.queued.request.id);
      entry.queued.reject(new Error("Git worker pool is disposed"));
    }
    this.inFlight.clear();
    // Dispose stops in-flight Git commands the same way a retirement does, so a
    // shutdown does not leave the last read's processes behind.
    this.disposePromise = Promise.all(
      slots.map((slot) => this.stopWorker(slot, requestIds.get(slot))),
    ).then(() => undefined);
    return this.disposePromise;
  }

  private pump(): void {
    if (this.disposed) {
      return;
    }
    while (this.queued > 0) {
      const slot = this.acquireSlot();
      if (!slot) {
        // Nothing alive: a spawn failed outright, so run the rest on main.
        if (this.slots.size === 0) {
          this.drainToFallback();
        }
        return;
      }
      const queued = this.highPriorityQueue.shift() ?? this.normalPriorityQueue.shift()!;
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
    const slot: WorkerSlot = { worker, exited: false };
    this.slots.add(slot);
    worker.unref();
    worker.on("message", (message: GitWorkerResponse) => this.onMessage(slot, message));
    worker.on("error", (error: Error) => this.onWorkerFailure(slot, error));
    worker.on("exit", (code: number) => {
      slot.exited = true;
      this.onWorkerFailure(slot, new Error(`Git worker exited (code ${code})`));
    });
    return slot;
  }

  private dispatch(slot: WorkerSlot, queued: QueuedRequest): void {
    this.dispatched += 1;
    const timer = setTimeout(() => this.onTimeout(slot), this.requestTimeoutMs);
    this.inFlight.set(slot, { queued, timer });
    slot.worker.ref();
    slot.worker.postMessage(queued.request, []);
  }

  private onMessage(slot: WorkerSlot, message: GitWorkerResponse): void {
    if (isGitWorkerCancelConfirmation(message)) {
      const wait = slot.cancelWait;
      if (wait && wait.id === message.id) {
        slot.cancelWait = undefined;
        wait.confirm();
      }
      return;
    }
    const reply: GitWorkerReply = message;
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
    this.retire(slot, error, inFlight.queued.request.id);
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
    this.retire(slot, error, inFlight?.queued.request.id);
  }

  private finish(slot: WorkerSlot, inFlight: InFlightRequest): void {
    clearTimeout(inFlight.timer);
    this.inFlight.delete(slot);
    if (this.slots.has(slot)) {
      slot.worker.unref();
    }
  }

  private retire(slot: WorkerSlot, error: Error, requestId?: number): void {
    this.slots.delete(slot);
    this.retirements += 1;
    void this.stopWorker(slot, requestId);
    this.noteFailure(error);
    this.pump();
  }

  /**
   * Terminating a thread does not kill the processes it spawned. Ask the worker
   * to abort the request it is holding, give it a bounded moment to confirm that
   * the abort took its Git processes with it, then terminate.
   */
  private async stopWorker(slot: WorkerSlot, requestId: number | undefined): Promise<void> {
    if (slot.exited) {
      await slot.worker.terminate();
      return;
    }
    if (requestId !== undefined) {
      await this.waitForCancelConfirmation(slot, requestId);
    }
    await slot.worker.terminate();
  }

  private waitForCancelConfirmation(slot: WorkerSlot, requestId: number): Promise<void> {
    let settle: () => void = () => undefined;
    const confirmed = new Promise<void>((resolve) => {
      settle = resolve;
    });
    // The grace is a bound, not a deadline: a worker that died without
    // confirming must not hold the slot's engine open.
    const timer = setTimeout(() => settle(), this.cancelGraceMs);
    timer.unref?.();
    slot.cancelWait = {
      id: requestId,
      confirm: () => {
        clearTimeout(timer);
        settle();
      },
    };
    try {
      slot.worker.postMessage({ type: "cancel", id: requestId } satisfies GitWorkerCancel, []);
    } catch {
      // The thread already died; the grace timer bounds this and terminate is a no-op.
    }
    return confirmed;
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
    const queued = [...this.highPriorityQueue.splice(0), ...this.normalPriorityQueue.splice(0)];
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
