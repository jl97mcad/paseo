// POSIX-only: a /bin/sh Git shim, ':' in PATH, and /proc process state.
import { execFile } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { promisify } from "node:util";
import { afterEach, describe, expect, it, vi } from "vitest";
import { isPlatform } from "../../test-utils/platform.js";
import { createWorkerGitDeps, type WorkerGitDeps } from "./client.js";

const execFileAsync = promisify(execFile);
const temporaryDirectories: string[] = [];
const workerPools: WorkerGitDeps[] = [];

afterEach(async () => {
  await Promise.all(workerPools.splice(0).map((deps) => deps.disposeDeps()));
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { recursive: true, force: true, maxRetries: 5 })),
  );
});

describe.skipIf(isPlatform("win32"))("git worker retirement POSIX-only", () => {
  it("kills the Git processes a timed-out worker was waiting on", async () => {
    const root = await mkdtemp(path.join(tmpdir(), "paseo-git-worker-orphans-"));
    temporaryDirectories.push(root);
    const binDirectory = path.join(root, "bin");
    const pidFile = path.join(root, "git-pids.txt");
    const markerFile = path.join(root, "blocked-once");
    const workspace = path.join(root, "workspace");
    await mkdir(binDirectory, { recursive: true });
    await mkdir(workspace, { recursive: true });
    const { stdout: realGit } = await execFileAsync("sh", ["-c", "command -v git"]);

    // The first git invocation on PATH records its pid and its child's, then
    // waits on the child. Everything after that runs the real git. `set -C`
    // makes claiming the marker atomic, so only one invocation blocks.
    await writeFile(
      path.join(binDirectory, "git"),
      `#!/bin/sh
if (set -C; : > "${markerFile}") 2>/dev/null; then
  echo $$ >> "${pidFile}"
  sleep 10 &
  echo $! >> "${pidFile}"
  wait
fi
exec "${realGit.trim()}" "$@"
`,
      { mode: 0o755 },
    );

    const originalPath = process.env.PATH;
    process.env.PATH = [binDirectory, originalPath ?? ""]
      .filter((entry) => entry.length > 0)
      .join(path.delimiter);
    // Generous enough for the source-mode worker thread to start and reach git.
    const workerDeps = createWorkerGitDeps({ size: 1, requestTimeoutMs: 3_000 });
    workerPools.push(workerDeps);
    try {
      const error = await workerDeps
        .getCheckoutStatus(workspace, {})
        .catch((cause: unknown) => cause);
      expect(error).toMatchObject({ name: "GitWorkerRequestTimeoutError" });
      expect(workerDeps.workerStats()).toMatchObject({ retirements: 1 });

      const pids = (await readFile(pidFile, "utf8"))
        .trim()
        .split("\n")
        .map((pid) => Number(pid));
      expect(pids).toHaveLength(2);
      await vi.waitFor(
        async () => {
          for (const pid of pids) {
            expect(await isGitProcessAlive(pid), `Git process ${pid} survived the retirement`).toBe(
              false,
            );
          }
        },
        { timeout: 5_000, interval: 50 },
      );
    } finally {
      process.env.PATH = originalPath;
    }
  });
});

/**
 * A killed process is finished even while it is still a zombie awaiting a
 * reaper, and `kill(pid, 0)` reports zombies as alive.
 */
async function isGitProcessAlive(pid: number): Promise<boolean> {
  try {
    const stat = await readFile(`/proc/${pid}/stat`, "utf8");
    const afterCommandName = stat.lastIndexOf(")") + 2;
    return stat.slice(afterCommandName, afterCommandName + 1) !== "Z";
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
      throw error;
    }
  }
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}
