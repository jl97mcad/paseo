import { EventEmitter } from "node:events";
import type { ChildProcess } from "node:child_process";
import { PassThrough } from "node:stream";
import { existsSync, readFileSync, statSync } from "node:fs";
import { dirname } from "node:path";
import type {
  Options,
  Query,
  SpawnOptions as ClaudeSpawnOptions,
} from "@anthropic-ai/claude-agent-sdk";
import { describe, expect, test, vi } from "vitest";

import { claudeQuery, type ClaudeQueryContext } from "./query.js";

function createQueryMock(): Query {
  return {
    next: vi.fn(async () => ({ done: true, value: undefined })),
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
  return Object.assign(new EventEmitter(), {
    stdin: new PassThrough(),
    stdout: new PassThrough(),
    stderr: new PassThrough(),
  }) as ChildProcess;
}

function getConfigPath(args: string[], occurrence = 0): string {
  let index = -1;
  for (let current = 0; current <= occurrence; current += 1) {
    index = args.indexOf("--mcp-config", index + 1);
    if (index === -1) throw new Error("MCP config argument is missing");
  }
  const configPath = args[index + 1];
  if (!configPath) throw new Error("MCP config path is missing");
  return configPath;
}

function createSpawnHarness(spawnProcess: NonNullable<ClaudeQueryContext["spawnProcess"]>): {
  spawn: NonNullable<Options["spawnClaudeCodeProcess"]>;
} {
  let spawn: NonNullable<Options["spawnClaudeCodeProcess"]> | undefined;
  claudeQuery(
    { prompt: "", options: {} },
    {
      spawnProcess,
      queryFactory: ({ options }) => {
        if (!options.spawnClaudeCodeProcess) throw new Error("Claude spawn callback is missing");
        spawn = options.spawnClaudeCodeProcess;
        return createQueryMock();
      },
    },
  );
  if (!spawn) throw new Error("Claude spawn callback was not configured");
  return { spawn };
}

describe("Claude spawn override", () => {
  test("keeps every inline MCP credential out of argv and removes its directory on child exit", () => {
    const child = createChildProcessStub();
    const spawnCalls: Array<{ command: string; args: string[]; shell?: boolean }> = [];
    const harness = createSpawnHarness((command, args, options) => {
      spawnCalls.push({ command, args, shell: options?.shell });
      return child;
    });
    const credential = "Bearer FAKE_CLAUDE_MCP_TOKEN";
    const firstConfig = JSON.stringify({
      mcpServers: { paseo: { headers: { Authorization: credential } } },
    });
    const secondConfig = JSON.stringify({ mcpServers: { extra: { token: credential } } });

    harness.spawn({
      command: "/test/claude/bin",
      args: ["claude.js", "--mcp-config", firstConfig, "--mcp-config", secondConfig],
      cwd: process.cwd(),
      env: {},
      signal: new AbortController().signal,
    } satisfies ClaudeSpawnOptions);

    const call = spawnCalls[0];
    expect(call).toBeDefined();
    if (!call) throw new Error("Claude process was not spawned");
    const firstPath = getConfigPath(call.args);
    const secondPath = getConfigPath(call.args, 1);
    const configDir = dirname(firstPath);
    expect(call.command).toBe("/test/claude/bin");
    expect(call.args).toEqual(["claude.js", "--mcp-config", firstPath, "--mcp-config", secondPath]);
    expect(call.args.join(" ")).not.toContain("Authorization");
    expect(call.args.join(" ")).not.toContain(credential);
    expect(readFileSync(firstPath, "utf8")).toBe(firstConfig);
    expect(readFileSync(secondPath, "utf8")).toBe(secondConfig);
    if (process.platform !== "win32") {
      expect(statSync(configDir).mode & 0o777).toBe(0o700);
      expect(statSync(firstPath).mode & 0o777).toBe(0o600);
    }
    expect(call.shell).toBe(false);

    child.emit("exit", 0, null);
    expect(existsSync(configDir)).toBe(false);
  });

  test("removes the MCP config directory on abort", () => {
    const child = createChildProcessStub();
    const spawnCalls: Array<{ args: string[] }> = [];
    const harness = createSpawnHarness((_command, args) => {
      spawnCalls.push({ args });
      return child;
    });
    const controller = new AbortController();

    harness.spawn({
      command: "/test/claude/bin",
      args: ["claude.js", "--mcp-config", JSON.stringify({ mcpServers: {} })],
      cwd: process.cwd(),
      env: {},
      signal: controller.signal,
    } satisfies ClaudeSpawnOptions);
    const call = spawnCalls[0];
    expect(call).toBeDefined();
    if (!call) throw new Error("Claude process was not spawned");
    const configDir = dirname(getConfigPath(call.args));
    expect(existsSync(configDir)).toBe(true);

    controller.abort();
    expect(existsSync(configDir)).toBe(false);
  });

  test("removes the MCP config directory on spawn failure without masking the error", () => {
    let configDir: string | undefined;
    const harness = createSpawnHarness((_command, args) => {
      configDir = dirname(getConfigPath(args));
      throw new Error("synthetic spawn failure");
    });

    expect(() =>
      harness.spawn({
        command: "/test/claude/bin",
        args: ["claude.js", "--mcp-config", JSON.stringify({ mcpServers: {} })],
        cwd: process.cwd(),
        env: {},
        signal: new AbortController().signal,
      } satisfies ClaudeSpawnOptions),
    ).toThrow("synthetic spawn failure");
    expect(configDir).toBeDefined();
    if (!configDir) throw new Error("MCP config directory was not created");
    expect(existsSync(configDir)).toBe(false);
  });
});
