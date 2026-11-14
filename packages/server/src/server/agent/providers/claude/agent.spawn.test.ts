import { EventEmitter } from "node:events";
import type { ChildProcess } from "node:child_process";
import { existsSync, readFileSync, statSync } from "node:fs";
import { dirname } from "node:path";
import type {
  Options,
  Query,
  SpawnOptions as ClaudeSpawnOptions,
} from "@anthropic-ai/claude-agent-sdk";
import { afterEach, describe, expect, test, vi } from "vitest";

import { createTestLogger } from "../../../../test-utils/test-logger.js";
import * as spawnUtils from "../../../../utils/spawn.js";
import { ClaudeAgentClient } from "./agent.js";
import type { ClaudeQueryInput } from "./query.js";

function createQueryMock(events: unknown[]): Query {
  let index = 0;
  return {
    next: vi.fn(async () =>
      index < events.length
        ? { done: false, value: events[index++] }
        : { done: true, value: undefined },
    ),
    return: vi.fn(async () => ({ done: true, value: undefined })),
    interrupt: vi.fn(async () => undefined),
    close: vi.fn(() => undefined),
    setPermissionMode: vi.fn(async () => undefined),
    setModel: vi.fn(async () => undefined),
    supportedModels: vi.fn(async () => [{ value: "opus", displayName: "Opus" }]),
    supportedCommands: vi.fn(async () => []),
    rewindFiles: vi.fn(async () => ({ canRewind: true })),
    [Symbol.asyncIterator]() {
      return this;
    },
  } as Query;
}

function createChildProcessStub(): ChildProcess {
  const child = new EventEmitter() as ChildProcess;
  child.stderr = new EventEmitter() as ChildProcess["stderr"];
  return child;
}

async function createSpawnHarness(): Promise<{
  spawn: NonNullable<Options["spawnClaudeCodeProcess"]>;
  close: () => Promise<void>;
}> {
  let capturedOptions: Options | undefined;
  const queryFactory = vi.fn(({ options }: ClaudeQueryInput) => {
    capturedOptions = options;
    return createQueryMock([
      {
        type: "system",
        subtype: "init",
        session_id: "claude-spawn-regression-session",
        permissionMode: "default",
        model: "opus",
      },
      { type: "assistant", message: { content: "done" } },
      {
        type: "result",
        subtype: "success",
        usage: { input_tokens: 1, cache_read_input_tokens: 0, output_tokens: 1 },
        total_cost_usd: 0,
      },
    ]);
  });
  const client = new ClaudeAgentClient({
    logger: createTestLogger(),
    queryFactory,
    resolveBinary: async () => "/test/claude/bin",
  });
  const session = await client.createSession({ provider: "claude", cwd: process.cwd() });
  await session.run("spawn regression");
  const spawn = capturedOptions?.spawnClaudeCodeProcess;
  if (!spawn) {
    await session.close();
    throw new Error("Claude spawn callback was not configured");
  }
  return { spawn, close: () => session.close() };
}

describe("Claude spawn override", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  test("keeps inline MCP credentials out of argv and removes config on child exit", async () => {
    const child = createChildProcessStub();
    const spawnSpy = vi.spyOn(spawnUtils, "spawnProcess").mockReturnValue(child);
    const harness = await createSpawnHarness();
    const credential = "Bearer FAKE_CLAUDE_MCP_TOKEN";
    const firstConfig = JSON.stringify({
      mcpServers: { paseo: { headers: { Authorization: credential } } },
    });
    const secondConfig = JSON.stringify({ mcpServers: { extra: { token: credential } } });
    const controller = new AbortController();

    try {
      harness.spawn({
        command: "node",
        args: ["claude.js", "--mcp-config", firstConfig, "--mcp-config", secondConfig],
        cwd: process.cwd(),
        env: {},
        signal: controller.signal,
      } satisfies ClaudeSpawnOptions);

      const call = spawnSpy.mock.calls.find(([, args]) => args[0] === "claude.js");
      expect(call).toBeDefined();
      const [, args, options] = call!;
      expect(args).not.toContain(firstConfig);
      expect(args).not.toContain(secondConfig);
      expect(args.join(" ")).not.toContain("Authorization");
      expect(args.join(" ")).not.toContain(credential);
      expect(args[1]).toBe("--mcp-config");
      const firstPath = args[2]!;
      const secondPath = args[4]!;
      expect(args).toEqual(["claude.js", "--mcp-config", firstPath, "--mcp-config", secondPath]);
      expect(readFileSync(firstPath, "utf8")).toBe(firstConfig);
      expect(readFileSync(secondPath, "utf8")).toBe(secondConfig);
      if (process.platform !== "win32") {
        expect(statSync(dirname(firstPath)).mode & 0o777).toBe(0o700);
        expect(statSync(firstPath).mode & 0o777).toBe(0o600);
      }
      expect(options?.shell).toBe(false);

      child.emit("exit", 0, null);
      expect(existsSync(firstPath)).toBe(false);
      expect(existsSync(secondPath)).toBe(false);
    } finally {
      await harness.close();
    }
  });

  test("removes the MCP config on abort", async () => {
    const child = createChildProcessStub();
    const spawnSpy = vi.spyOn(spawnUtils, "spawnProcess").mockReturnValue(child);
    const harness = await createSpawnHarness();
    const controller = new AbortController();

    try {
      harness.spawn({
        command: "node",
        args: ["claude.js", "--mcp-config", JSON.stringify({ mcpServers: {} })],
        cwd: process.cwd(),
        env: {},
        signal: controller.signal,
      });
      const args = spawnSpy.mock.calls[0]![1];
      const configPath = args[args.indexOf("--mcp-config") + 1]!;
      expect(existsSync(configPath)).toBe(true);
      controller.abort();
      expect(existsSync(configPath)).toBe(false);
    } finally {
      await harness.close();
    }
  });

  test("removes the MCP config on spawn failure without masking the error", async () => {
    let configPath: string | undefined;
    vi.spyOn(spawnUtils, "spawnProcess").mockImplementation((_command, args) => {
      configPath = args[args.indexOf("--mcp-config") + 1];
      throw new Error("synthetic spawn failure");
    });
    const harness = await createSpawnHarness();

    try {
      expect(() =>
        harness.spawn({
          command: "node",
          args: ["claude.js", "--mcp-config", JSON.stringify({ mcpServers: {} })],
          cwd: process.cwd(),
          env: {},
          signal: new AbortController().signal,
        }),
      ).toThrow("synthetic spawn failure");
      expect(configPath).toBeDefined();
      expect(existsSync(configPath!)).toBe(false);
    } finally {
      await harness.close();
    }
  });
});
