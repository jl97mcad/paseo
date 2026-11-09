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
  splitGitProcessPolicy,
  type GitProcessPolicy,
  type GitProcessPriority,
} from "../../utils/git-process-scheduler.js";
import {
  DEFAULT_GIT_COMMAND_TIMEOUT_MS,
  getGitCommandPriority,
} from "../../utils/run-git-command.js";
import type { WorkspaceGitServiceDependencies } from "../workspace-git-service.js";
import {
  isGitWorkerCancelConfirmation,
  isGitWorkerLog,
  reviveGitWorkerError,
  type GitWorkerCancel,
  type GitWorkerCheckoutInput,
  type GitWorkerLogFields,
  type GitWorkerLogLevel,
  type GitWorkerOutputs,
  type GitWorkerPolicy,
  type GitWorkerReply,
  type GitWorkerResponse,
  type GitWorkerTask,
} from "./protocol.js";

export const DEFAULT_GIT_WORKER_COUNT = 2;
/**
 * One pooled request runs a whole read of several Git commands, each with the
 * per-command timeout, so the request budget is a multiple of it.
 */
export const DEFAULT_GIT_WORKER_REQUEST_TIMEOUT_MS = 4 * DEFAULT_GIT_COMMAND_TIMEOUT_MS;
export const DEFAULT_GIT_WORKER_QUEUE_LIMIT = 256;
/**
 * How long a retired worker may take to confirm that it stopped its Git
 * processes before the pool terminates the thread anyway.
 */
export const DEFAULT_GIT_WORKER_CANCEL_GRACE_MS = 1_000;
/** Consecutive failures this close together mean the worker cannot hold work. */
const RAPID_FAILURE_WINDOW_MS = 5_000;
const RAPID_FAILURE_LIMIT = 3;
/** How long the pool reads in-thread before it tries its threads again. */
const RAPID_FAILURE_COOLDOWN_MS = 60_000;
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

interface WorkerLogEvent {
  level: GitWorkerLogLevel;
  message: string;
  fields?: GitWorkerLogFields;
}

interface WorkerSlot {
  worker: Worker;
  exited: boolean;
  cancelWait?: { id: number; confirm: () => void };
}

export interface GitWorkerPoolOptions {
  size: number;
  entry?: GitWorkerEntry;
  /** Resolved only when the pool actually spawns a worker. */
  resolveEntry?: () => GitWorkerEntry;
  logger?: Pick<pino.Logger, "warn" | "error">;
  requestTimeoutMs?: number;
  queueLimit?: number;
  cancelGraceMs?: number;
  /** How long the pool reads in-thread after repeated rapid failures. */
  recoveryCooldownMs?: number;
  /** Structured-clone data handed to every worker; the Git policy share lives here. */
  workerData?: () => unknown;
}

/**
 * One thread per concurrent read, a bounded queue in front of them, and the
 * in-thread implementation as the fallback. Any pool failure runs that same
 * read in-thread, so the pool never rejects a read the in-thread path can
 * serve. A worker that errors, exits or times out is retired and replaced
 * lazily; repeated rapid failures disable the pool until a cooldown passes.
 */
export class GitWorkerPool {
  private readonly slots = new Set<WorkerSlot>();
  private readonly inFlight = new Map<WorkerSlot, InFlightRequest>();
  // Two lanes: an archive waiting on a read does not queue behind the pollers'
  // background refreshes. The worker's Git scheduler uses the same split.
  private readonly highPriorityQueue: QueuedRequest[] = [];
  private readonly normalPriorityQueue: QueuedRequest[] = [];
  private readonly size: number;
  private readonly resolveEntry: () => GitWorkerEntry;
  private entry: GitWorkerEntry | null;
  private readonly logger: Pick<pino.Logger, "warn" | "error"> | undefined;
  private readonly requestTimeoutMs: number;
  private readonly queueLimit: number;
  private readonly cancelGraceMs: number;
  private readonly recoveryCooldownMs: number;
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
    this.resolveEntry = options.resolveEntry ?? resolveGitWorkerEntry;
    // A pool that never spawns must not need the worker entry, and with it tsx.
    this.entry = this.size > 0 ? (options.entry ?? null) : null;
    this.logger = options.logger;
    this.requestTimeoutMs = options.requestTimeoutMs ?? DEFAULT_GIT_WORKER_REQUEST_TIMEOUT_MS;
    this.queueLimit = options.queueLimit ?? DEFAULT_GIT_WORKER_QUEUE_LIMIT;
    this.cancelGraceMs = options.cancelGraceMs ?? DEFAULT_GIT_WORKER_CANCEL_GRACE_MS;
    this.recoveryCooldownMs = options.recoveryCooldownMs ?? RAPID_FAILURE_COOLDOWN_MS;
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
    if (this.disposed || this.size === 0 || !this.recovered()) {
      this.fallbacks += 1;
      return fallback();
    }
    if (this.queued >= this.queueLimit) {
      // The queue protects the main thread from admitting work; it must not turn
      // a burst into a failed read, so the read runs in-thread instead.
      this.dropped += 1;
      this.saturatedDrops += 1;
      this.fallbacks += 1;
      this.logSaturation();
      return fallback();
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

  /** Re-enable the pool once the cooldown after the last rapid failure passed. */
  private recovered(): boolean {
    if (!this.unavailable) {
      return true;
    }
    if (Date.now() - this.lastFailureAtMs < this.recoveryCooldownMs) {
      return false;
    }
    this.unavailable = false;
    this.rapidFailures = 0;
    this.logger?.warn(
      { cooldownMs: this.recoveryCooldownMs },
      "Git worker pool cooled down; trying worker threads again",
    );
    return true;
  }

  private spawnSlot(): WorkerSlot | null {
    // Resolved on the first spawn, so a disabled pool never loads the entry.
    const entry = (this.entry ??= this.resolveEntry());
    let worker: Worker;
    try {
      worker = new Worker(entry.script, {
        eval: entry.eval,
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
    if (isGitWorkerLog(message)) {
      this.logWorker({ level: message.level, message: message.message, fields: message.fields });
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
    this.runFallback(inFlight.queued);
  }

  private onWorkerFailure(slot: WorkerSlot, error: Error): void {
    if (!this.slots.has(slot)) {
      return;
    }
    const inFlight = this.inFlight.get(slot);
    if (inFlight) {
      this.finish(slot, inFlight);
    }
    this.retire(slot, error, inFlight?.queued.request.id);
    if (inFlight) {
      this.runFallback(inFlight.queued);
    }
  }

  /** The in-thread path serves any request the pool could not. */
  private runFallback(queued: QueuedRequest): void {
    this.fallbacks += 1;
    void queued.fallback().then(queued.resolve, queued.reject);
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
      this.runFallback(entry);
    }
  }

  private logWorker(event: WorkerLogEvent): void {
    if (event.level === "error") {
      this.logger?.error(event.fields ?? {}, event.message);
      return;
    }
    this.logger?.warn(event.fields ?? {}, event.message);
  }

  private logSaturation(): void {
    if (this.saturationLogged) {
      return;
    }
    this.saturationLogged = true;
    this.logger?.warn(
      { limit: this.queueLimit, size: this.size },
      "Git worker queue is full; reading in-thread until it drains",
    );
  }

  /** A live policy change reaches the threads that are already running. */
  updateGitProcessPolicy(policy: GitProcessPolicy): void {
    if (this.size === 0) {
      return;
    }
    const workerPolicy = splitGitProcessPolicy(policy, this.size).worker;
    const message: GitWorkerPolicy = { type: "policy", policy: workerPolicy };
    for (const slot of this.slots) {
      try {
        slot.worker.postMessage(message, []);
      } catch {
        // An exiting thread is replaced with the new share on its next spawn.
      }
    }
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
  logger?: Pick<pino.Logger, "warn" | "error">;
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
  /** Re-derive the threads' policy share after a live Git policy change. */
  updateGitProcessPolicy: (policy: GitProcessPolicy) => void;
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
        ? () => ({ gitProcessPolicy: splitGitProcessPolicy(resolvePolicy(), size).worker })
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
    updateGitProcessPolicy: (policy) => pool.updateGitProcessPolicy(policy),
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
