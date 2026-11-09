import { setImmediate as scheduleImmediate } from "node:timers";
import pThrottle from "p-throttle";

export interface GitProcessPolicy {
  maxProcessesPerSecond: number;
  maxProcessConcurrency: number;
}

export const DEFAULT_GIT_PROCESS_POLICY: GitProcessPolicy = {
  maxProcessesPerSecond: 64,
  maxProcessConcurrency: 8,
};

export interface ScheduledGitProcess<T> {
  result: Promise<T>;
  exited: Promise<void>;
}

export type GitProcessPriority = "normal" | "high";

function parsePositiveInteger(value: string | undefined): number | undefined {
  if (value === undefined) {
    return undefined;
  }
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : undefined;
}

export function resolveGitProcessPolicy(input: {
  env: NodeJS.ProcessEnv;
  persisted?: Partial<GitProcessPolicy>;
}): GitProcessPolicy {
  return {
    maxProcessesPerSecond:
      parsePositiveInteger(input.env.PASEO_GIT_MAX_PROCESSES_PER_SECOND) ??
      input.persisted?.maxProcessesPerSecond ??
      DEFAULT_GIT_PROCESS_POLICY.maxProcessesPerSecond,
    maxProcessConcurrency:
      parsePositiveInteger(input.env.PASEO_GIT_MAX_PROCESS_CONCURRENCY) ??
      // COMPAT(gitConcurrencyEnv): renamed in v0.2.6; remove after 2027-02-02.
      parsePositiveInteger(input.env.PASEO_GIT_CONCURRENCY) ??
      input.persisted?.maxProcessConcurrency ??
      DEFAULT_GIT_PROCESS_POLICY.maxProcessConcurrency,
  };
}

/**
 * Split one daemon Git process policy between the main thread and the pool.
 * Main keeps the larger half; the pool divides the smaller half across its
 * threads, with one process each at minimum. Both limits are split, so the
 * pool adds no capacity beyond the configured cap unless the cap is smaller
 * than the number of threads.
 */
export function splitGitProcessPolicy(
  policy: GitProcessPolicy,
  poolSize: number,
): { main: GitProcessPolicy; worker: GitProcessPolicy } {
  const share = Math.max(1, poolSize);
  return {
    main: {
      maxProcessConcurrency: Math.max(1, Math.ceil(policy.maxProcessConcurrency / 2)),
      maxProcessesPerSecond: Math.max(1, Math.ceil(policy.maxProcessesPerSecond / 2)),
    },
    worker: {
      maxProcessConcurrency: Math.max(1, Math.floor(policy.maxProcessConcurrency / 2 / share)),
      maxProcessesPerSecond: Math.max(1, Math.floor(policy.maxProcessesPerSecond / 2 / share)),
    },
  };
}

/** The configured cap stays whole while Git reads in-thread. */
export function mainGitProcessPolicy(policy: GitProcessPolicy, poolSize: number): GitProcessPolicy {
  return poolSize > 0 ? splitGitProcessPolicy(policy, poolSize).main : policy;
}

export class GitProcessScheduler {
  private readonly startThrottled;
  private readonly highPriorityQueue: Array<() => void> = [];
  private readonly normalPriorityQueue: Array<() => void> = [];
  private admitted = 0;
  private running = 0;
  private waiting = 0;
  private drainScheduled = false;

  constructor(readonly policy: GitProcessPolicy) {
    const throttle = pThrottle({
      limit: policy.maxProcessesPerSecond,
      interval: 1_000,
      strict: true,
    });
    this.startThrottled = throttle((start: () => Promise<void>) => start());
  }

  get activeCount(): number {
    return this.running;
  }

  get pendingCount(): number {
    return this.waiting;
  }

  run<T>(
    start: () => ScheduledGitProcess<T>,
    options?: { priority?: GitProcessPriority },
  ): Promise<T> {
    this.waiting += 1;
    return new Promise<T>((resolve, reject) => {
      const admit = () => {
        void this.startThrottled(() => this.execute(start, resolve, reject)).catch((error) => {
          this.waiting = Math.max(0, this.waiting - 1);
          this.admitted = Math.max(0, this.admitted - 1);
          reject(error);
          this.scheduleDrain();
        });
      };
      const queue =
        options?.priority === "high" ? this.highPriorityQueue : this.normalPriorityQueue;
      queue.push(admit);
      if (this.admitted === 0 && !this.drainScheduled) {
        this.drainOne();
      } else {
        this.scheduleDrain();
      }
    });
  }

  private scheduleDrain(): void {
    if (this.drainScheduled || this.admitted >= this.policy.maxProcessConcurrency) {
      return;
    }
    if (this.highPriorityQueue.length === 0 && this.normalPriorityQueue.length === 0) {
      return;
    }
    this.drainScheduled = true;
    scheduleImmediate(() => this.drainOne());
  }

  private drainOne(): void {
    this.drainScheduled = false;
    if (this.admitted >= this.policy.maxProcessConcurrency) {
      return;
    }
    const admit = this.highPriorityQueue.shift() ?? this.normalPriorityQueue.shift();
    if (!admit) {
      return;
    }
    this.admitted += 1;
    admit();
    // Spawning can be synchronous and expensive on a throttled host. Let timers,
    // sockets, and child exits run before admitting another Git process.
    this.scheduleDrain();
  }

  private async execute<T>(
    start: () => ScheduledGitProcess<T>,
    resolve: (value: T | PromiseLike<T>) => void,
    reject: (reason?: unknown) => void,
  ): Promise<void> {
    this.waiting = Math.max(0, this.waiting - 1);
    this.running += 1;
    try {
      const process = start();
      void process.result.then(resolve, reject);
      await process.exited;
    } catch (error) {
      reject(error);
    } finally {
      this.running = Math.max(0, this.running - 1);
      this.admitted = Math.max(0, this.admitted - 1);
      this.scheduleDrain();
    }
  }
}
