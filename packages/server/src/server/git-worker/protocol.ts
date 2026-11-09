import type {
  CheckoutRefDerivedState,
  CheckoutShortstat,
  CheckoutSnapshotFacts,
  CheckoutStatusResult,
  CheckoutWorktreeState,
} from "../../utils/checkout-git.js";
import type { GitProcessPolicy, GitProcessPriority } from "../../utils/git-process-scheduler.js";

/**
 * The checkout reads that run inside the Git worker. Every input and output is
 * structured-clone safe: no logger, no `runGitCommand`, no AbortSignal. The
 * worker rebuilds the `CheckoutContext` from these strings and plain facts, so
 * the caller's error handling and the service state machine stay on main.
 */
export interface GitWorkerCheckoutInput {
  cwd: string;
  paseoHome?: string;
  worktreesRoot?: string;
  facts: CheckoutSnapshotFacts | null;
}

export interface GitWorkerRefDerivedInput extends Omit<GitWorkerCheckoutInput, "facts"> {
  facts: Extract<CheckoutSnapshotFacts, { isGit: true }>;
  current: Pick<CheckoutRefDerivedState, "aheadBehind" | "diffStat">;
  movedRemoteRefs: ReadonlySet<string>;
}

export type GitWorkerTask =
  | { op: "snapshotFacts"; input: GitWorkerCheckoutInput }
  | { op: "checkoutStatus"; input: GitWorkerCheckoutInput }
  | { op: "shortstat"; input: GitWorkerCheckoutInput & { force: boolean } }
  | { op: "worktreeState"; input: GitWorkerCheckoutInput }
  | { op: "refDerived"; input: GitWorkerRefDerivedInput };

export interface GitWorkerOutputs {
  snapshotFacts: CheckoutSnapshotFacts;
  checkoutStatus: CheckoutStatusResult;
  shortstat: CheckoutShortstat | null;
  worktreeState: CheckoutWorktreeState;
  refDerived: CheckoutRefDerivedState;
}

export type GitWorkerRequest = GitWorkerTask & { id: number; priority: GitProcessPriority };

/** A live Git policy change, with the share the pool derived for this thread. */
export interface GitWorkerPolicy {
  type: "policy";
  policy: GitProcessPolicy;
}

/**
 * Retire-time abort. The pool sends it before terminating a worker so the
 * commands still running in that thread can kill their process trees.
 */
export interface GitWorkerCancel {
  type: "cancel";
  id: number;
}

/** The worker's promise that the cancelled request has settled. */
export interface GitWorkerCancelConfirmation {
  type: "cancelled";
  id: number;
}

export type GitWorkerLogLevel = "warn" | "error";
export type GitWorkerLogFields = Record<string, unknown>;

/**
 * A warning or error the worker logged while it rebuilt its read context; the
 * daemon logger writes it on main. Trace output stays in the worker.
 */
export interface GitWorkerLog {
  type: "log";
  level: GitWorkerLogLevel;
  message: string;
  fields?: GitWorkerLogFields;
}

export type GitWorkerMessage = GitWorkerRequest | GitWorkerCancel | GitWorkerPolicy;

export function isGitWorkerCancel(message: GitWorkerMessage): message is GitWorkerCancel {
  return "type" in message && message.type === "cancel";
}

export function isGitWorkerPolicy(message: GitWorkerMessage): message is GitWorkerPolicy {
  return "type" in message && message.type === "policy";
}

export interface GitWorkerErrorPayload {
  name: string;
  message: string;
  code?: string;
}

export type GitWorkerReply =
  | { id: number; ok: true; value: unknown }
  | { id: number; ok: false; error: GitWorkerErrorPayload };

export type GitWorkerResponse = GitWorkerReply | GitWorkerCancelConfirmation | GitWorkerLog;

export function isGitWorkerCancelConfirmation(
  message: GitWorkerResponse,
): message is GitWorkerCancelConfirmation {
  return "type" in message && message.type === "cancelled";
}

export function isGitWorkerLog(message: GitWorkerResponse): message is GitWorkerLog {
  return "type" in message && message.type === "log";
}

export function toGitWorkerErrorPayload(cause: unknown): GitWorkerErrorPayload {
  if (!(cause instanceof Error)) {
    return { name: "Error", message: String(cause) };
  }
  const code = (cause as { code?: unknown }).code;
  return {
    name: cause.name,
    message: cause.message,
    ...(typeof code === "string" ? { code } : {}),
  };
}

/** Rebuild just the error members callers branch on: `name`, `message`, `code`. */
export function reviveGitWorkerError(payload: GitWorkerErrorPayload): Error {
  const error = new Error(payload.message);
  error.name = payload.name;
  if (payload.code !== undefined) {
    (error as { code?: string }).code = payload.code;
  }
  return error;
}
