import type {
  CheckoutRefDerivedState,
  CheckoutShortstat,
  CheckoutSnapshotFacts,
  CheckoutStatusResult,
  CheckoutWorktreeState,
} from "../../utils/checkout-git.js";

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

export type GitWorkerRequest = GitWorkerTask & { id: number };

export interface GitWorkerErrorPayload {
  name: string;
  message: string;
  code?: string;
}

export type GitWorkerReply =
  | { id: number; ok: true; value: unknown }
  | { id: number; ok: false; error: GitWorkerErrorPayload };

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
