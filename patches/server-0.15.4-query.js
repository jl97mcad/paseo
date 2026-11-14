import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { query } from "@anthropic-ai/claude-agent-sdk";
import { createProviderEnvSpec } from "../../provider-launch-config.js";
import { buildSelfNodeCommand, createExternalProcessEnv } from "../../../paseo-env.js";
import { spawnProcess } from "../../../../utils/spawn.js";
function isChildProcessWithStreams(child) {
  return child.stdin !== null && child.stdout !== null && child.stderr !== null;
}
function resolveClaudeSpawnCommand(spawnOptions, runtimeSettings) {
  const commandConfig = runtimeSettings?.command;
  if (!commandConfig || commandConfig.mode === "default") {
    return {
      command: spawnOptions.command,
      args: [...spawnOptions.args],
    };
  }
  if (commandConfig.mode === "append") {
    return {
      command: spawnOptions.command,
      args: [...spawnOptions.args, ...(commandConfig.args ?? [])],
    };
  }
  return {
    command: commandConfig.argv[0],
    args: [...commandConfig.argv.slice(1), ...spawnOptions.args],
  };
}
function materializeInlineMcpConfigs(args) {
  const configIndexes = args.flatMap((arg, index) =>
    arg === "--mcp-config" && args[index + 1]?.trimStart().startsWith("{") ? [index + 1] : [],
  );
  if (configIndexes.length === 0) return { args };
  const dir = mkdtempSync(join(tmpdir(), "paseo-claude-mcp-"));
  let cleaned = false;
  const cleanup = () => {
    if (cleaned) return;
    cleaned = true;
    try {
      rmSync(dir, { recursive: true, force: true });
    } catch {}
  };
  try {
    const nextArgs = [...args];
    for (const [index, argIndex] of configIndexes.entries()) {
      const configPath = join(dir, "mcp-" + index + ".json");
      writeFileSync(configPath, args[argIndex], { encoding: "utf8", mode: 0o600 });
      nextArgs[argIndex] = configPath;
    }
    return { args: nextArgs, cleanup };
  } catch (error) {
    cleanup();
    throw error;
  }
}
function applyRuntimeSettingsToClaudeOptions(options, context) {
  const { runtimeSettings, launchEnv, onChildProcess } = context;
  return {
    ...options,
    spawnClaudeCodeProcess: (spawnOptions) => {
      const resolved = resolveClaudeSpawnCommand(spawnOptions, runtimeSettings);
      // When the SDK passes a default JS runtime ("node"/"bun"), replace it with
      // process.execPath — the actual node binary running the daemon. This avoids
      // PATH lookup failures in the managed runtime bundle.
      // When the SDK passes a native binary path (from pathToClaudeCodeExecutable)
      // or the user overrides the command via runtime settings, use that directly.
      const isDefaultRuntime = resolved.command === "node" || resolved.command === "bun";
      const providerEnvSpec = createProviderEnvSpec({
        baseEnv: spawnOptions.env,
        runtimeSettings,
        overlays: [launchEnv],
      });
      // Session-open hooks may explicitly supply a client identity. Keep inherited
      // SDK/parent identities cleared while honoring this launch-only contribution.
      providerEnvSpec.envOverlay.CLAUDE_CODE_ENTRYPOINT = launchEnv?.CLAUDE_CODE_ENTRYPOINT;
      const providerEnv = createExternalProcessEnv(spawnOptions.env, providerEnvSpec.envOverlay);
      const selfNodeCommand = isDefaultRuntime
        ? buildSelfNodeCommand(resolved.args, providerEnv)
        : null;
      const command = selfNodeCommand?.command ?? resolved.command;
      const mcpConfig = materializeInlineMcpConfigs(selfNodeCommand?.args ?? resolved.args);
      let child;
      try {
        child = spawnProcess(command, mcpConfig.args, {
          cwd: spawnOptions.cwd,
          ...(selfNodeCommand
            ? { env: selfNodeCommand.env, envMode: "internal" }
            : providerEnvSpec),
          signal: spawnOptions.signal,
          stdio: ["pipe", "pipe", "pipe"],
          // Bypass cmd.exe on Windows: the SDK passes --mcp-config with inline JSON
          // containing double quotes, which cmd.exe mangles (strips quotes, breaks parsing).
          // The command is always a resolved binary path, so shell routing is unnecessary.
          shell: false,
        });
      } catch (error) {
        mcpConfig.cleanup?.();
        throw error;
      }
      if (mcpConfig.cleanup) {
        const cleanup = mcpConfig.cleanup;
        const cleanupAfterChild = () => {
          spawnOptions.signal.removeEventListener("abort", cleanup);
          cleanup();
        };
        child.once("exit", cleanupAfterChild);
        child.once("error", cleanupAfterChild);
        spawnOptions.signal.addEventListener("abort", cleanup, { once: true });
      }
      onChildProcess?.(child);
      if (typeof options.stderr === "function") {
        child.stderr?.on("data", (chunk) => {
          options.stderr?.(chunk.toString());
        });
      }
      if (!isChildProcessWithStreams(child)) {
        throw new Error("Claude process was spawned without stdio streams");
      }
      return child;
    },
  };
}
export function claudeQuery(input, context = {}) {
  const launchQuery = context.queryFactory ?? query;
  return launchQuery({
    ...input,
    options: applyRuntimeSettingsToClaudeOptions(input.options, context),
  });
}
//# sourceMappingURL=query.js.map
