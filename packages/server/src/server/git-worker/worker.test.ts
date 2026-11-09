import { describe, expect, it } from "vitest";
import {
  getGitCommandPriority,
  type GitCommandResult,
  type RunGitCommand,
} from "../../utils/run-git-command.js";
import { createGitWorkerTaskHandler } from "./worker.js";
import type { GitWorkerResponse } from "./protocol.js";

const input = { cwd: "/tmp/workspace", facts: null };

/** A runner that records the priority its caller is running at, then fails. */
function recordingRunner(seen: string[]): RunGitCommand {
  return async () => {
    seen.push(getGitCommandPriority());
    throw new Error("Not a git repository");
  };
}

function collectingHandler(runGitCommand: RunGitCommand) {
  const replies: GitWorkerResponse[] = [];
  const handler = createGitWorkerTaskHandler({
    post: (message) => replies.push(message),
    runGitCommand,
  });
  return { handler, replies };
}

describe("git worker task handler", () => {
  it("runs a request's Git commands at the priority the request carried", async () => {
    for (const priority of ["high", "normal"] as const) {
      const seen: string[] = [];
      const { handler } = collectingHandler(recordingRunner(seen));

      await handler.run({ id: 1, op: "snapshotFacts", input, priority });

      expect(seen.length).toBeGreaterThan(0);
      expect(seen.every((entry) => entry === priority)).toBe(true);
    }
  });

  it("posts a read's warnings to main and drops its trace output", async () => {
    let warned = false;
    const runner: RunGitCommand = async (_args, options) => {
      options.logger?.trace("Spawning git command");
      if (!warned) {
        warned = true;
        options.logger?.warn(
          { cwd: "/tmp/workspace" },
          "Git worktree discovery failed; treating directory as non-Git",
        );
      }
      throw new Error("Not a git repository");
    };
    const { handler, replies } = collectingHandler(runner);

    await handler.run({ id: 5, op: "snapshotFacts", input, priority: "normal" });

    expect(replies.filter((reply) => "type" in reply && reply.type === "log")).toEqual([
      {
        type: "log",
        level: "warn",
        message: "Git worktree discovery failed; treating directory as non-Git",
        fields: { cwd: "/tmp/workspace" },
      },
    ]);
  });

  it("aborts a cancelled request's commands and confirms once they settle", async () => {
    let aborted = false;
    const runner: RunGitCommand = (_args, options) =>
      new Promise<GitCommandResult>((_resolve, reject) => {
        options.signal?.addEventListener("abort", () => {
          aborted = true;
          reject(options.signal?.reason);
        });
      });
    const { handler, replies } = collectingHandler(runner);

    const running = handler.run({ id: 7, op: "snapshotFacts", input, priority: "high" });
    await Promise.resolve();
    expect(aborted).toBe(false);

    handler.cancel(7);
    expect(aborted).toBe(true);
    await running;

    expect(
      replies
        .map((reply) => ("type" in reply ? reply.type : reply.id))
        .filter((entry) => entry !== "log"),
    ).toEqual([7, "cancelled"]);
  });

  it("replies without confirming a cancel for a request that already settled", async () => {
    const { handler, replies } = collectingHandler(recordingRunner([]));
    await handler.run({ id: 3, op: "snapshotFacts", input, priority: "normal" });

    handler.cancel(3);

    expect(replies).toHaveLength(1);
    expect(replies[0]).toMatchObject({ id: 3 });
  });
});
