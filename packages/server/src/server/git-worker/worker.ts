import { parentPort, workerData } from "node:worker_threads";
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
import { configureGitProcessPolicy, createRunGitCommand } from "../../utils/run-git-command.js";
import {
  toGitWorkerErrorPayload,
  type GitWorkerReply,
  type GitWorkerRequest,
  type GitWorkerTask,
} from "./protocol.js";

// This thread is one of a small pool, so it gets a share of the daemon's Git
// process cap rather than a second full cap. The pool passes the share it
// computed from the live policy; the fallback divides the environment policy by
// the default pool size.
const sharedPolicy = (workerData as { gitProcessPolicy?: GitProcessPolicy } | null)
  ?.gitProcessPolicy;
if (sharedPolicy) {
  configureGitProcessPolicy(sharedPolicy);
} else {
  const policy = resolveGitProcessPolicy({ env: process.env });
  configureGitProcessPolicy({
    maxProcessConcurrency: Math.max(1, Math.floor(policy.maxProcessConcurrency / 2)),
    maxProcessesPerSecond: Math.max(1, Math.floor(policy.maxProcessesPerSecond / 2)),
  });
}

const runGitCommand = createRunGitCommand("workspace-refresh:worker");
const port = parentPort!;

// The client admits one request per worker, so the port queues the first
// request until this entrypoint has loaded, including in source/tsx development.
port.on("message", (request: GitWorkerRequest) => {
  void run(request);
});

async function run(request: GitWorkerRequest): Promise<void> {
  try {
    const value = await execute(request);
    port.postMessage({ id: request.id, ok: true, value } satisfies GitWorkerReply);
  } catch (cause) {
    port.postMessage({
      id: request.id,
      ok: false,
      error: toGitWorkerErrorPayload(cause),
    } satisfies GitWorkerReply);
  }
}

function execute(task: GitWorkerTask): Promise<unknown> {
  // The caller's logger and provenance wrapper stay on main; the reads only
  // need the facts, the observation roots, and a command runner of their own.
  const context: CheckoutContext = {
    paseoHome: task.input.paseoHome,
    worktreesRoot: task.input.worktreesRoot,
    runGitCommand,
  };
  switch (task.op) {
    case "snapshotFacts":
      return getCheckoutSnapshotFacts(task.input.cwd, {
        ...context,
        facts: task.input.facts,
      });
    case "checkoutStatus":
      return getCheckoutStatus(task.input.cwd, { ...context, facts: task.input.facts });
    case "shortstat":
      return getCheckoutShortstat(
        task.input.cwd,
        { ...context, facts: task.input.facts },
        { force: task.input.force },
      );
    case "worktreeState":
      return getCheckoutWorktreeState(task.input.cwd, {
        ...context,
        facts: task.input.facts,
      });
    case "refDerived":
      return getCheckoutRefDerivedState(
        task.input.cwd,
        task.input.facts,
        task.input.current,
        task.input.movedRemoteRefs,
        { ...context, facts: task.input.facts },
      );
  }
}
