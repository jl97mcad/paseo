import { describe, expect, it } from "vitest";
import { getGitCommandPriority, type RunGitCommand } from "../../utils/run-git-command.js";
import { createGitWorkerTaskHandler } from "./worker.js";
import type { GitWorkerReply } from "./protocol.js";

const input = { cwd: "/tmp/workspace", facts: null };

/** A runner that records the priority its caller is running at, then fails. */
function recordingRunner(seen: string[]): RunGitCommand {
  return async () => {
    seen.push(getGitCommandPriority());
    throw new Error("Not a git repository");
  };
}

describe("git worker task handler", () => {
  it("runs a request's Git commands at the priority the request carried", async () => {
    for (const priority of ["high", "normal"] as const) {
      const seen: string[] = [];
      const replies: GitWorkerReply[] = [];
      const handler = createGitWorkerTaskHandler({
        post: (message) => replies.push(message),
        runGitCommand: recordingRunner(seen),
      });

      await handler.run({ id: 1, op: "snapshotFacts", input, priority });

      expect(seen.length).toBeGreaterThan(0);
      expect(seen.every((entry) => entry === priority)).toBe(true);
      expect(replies).toHaveLength(1);
    }
  });
});
