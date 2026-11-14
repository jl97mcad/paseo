import { isAbsolute } from "node:path";
import {
  executableExists,
  findExecutable,
} from "../../executable-resolution/executable-resolution.js";
import { createExternalProcessEnv } from "../paseo-env.js";
export {
  AgentProviderRuntimeSettingsMapSchema,
  ProviderCommandSchema,
  ProviderOverrideSchema,
  ProviderOverridesSchema,
  ProviderProfileModelSchema,
  ProviderRuntimeSettingsSchema,
} from "@getpaseo/protocol/provider-config";
import {
  ProviderOverrideSchema,
  ProviderOverridesSchema,
  ProviderRuntimeSettingsSchema,
} from "@getpaseo/protocol/provider-config";
function normalizeLaunchDefault(defaultBinary) {
  if (typeof defaultBinary === "string") {
    return { command: defaultBinary };
  }
  return defaultBinary;
}
async function resolveLaunchPath(command, env) {
  const found = await findExecutable(command, { env });
  if (found) {
    return found;
  }
  if (isAbsolute(command)) {
    return executableExists(command);
  }
  return null;
}
async function resolveDefaultLaunchPath(defaultBinary) {
  return defaultBinary.resolvePath
    ? await defaultBinary.resolvePath()
    : await resolveLaunchPath(defaultBinary.command);
}
export async function resolveProviderLaunch({ commandConfig, defaultBinary }) {
  if (commandConfig?.mode === "replace") {
    const command = commandConfig.argv[0];
    return {
      command,
      args: commandConfig.argv.slice(1),
      source: "override",
    };
  }
  if (defaultBinary === undefined) {
    throw new Error("defaultBinary is required when provider command is not replaced");
  }
  const normalizedDefault = normalizeLaunchDefault(defaultBinary);
  const args = commandConfig?.mode === "append" ? [...(commandConfig.args ?? [])] : [];
  return {
    command: normalizedDefault.command,
    args,
    source: commandConfig?.mode === "append" ? "append" : "default",
  };
}
export async function checkProviderLaunchAvailable(launch, defaultBinary) {
  const resolvedPath =
    defaultBinary && launch.source !== "override"
      ? await resolveDefaultLaunchPath(defaultBinary)
      : await resolveLaunchPath(launch.command, launch.env);
  return {
    available: resolvedPath !== null,
    resolvedPath,
  };
}
export async function resolveProviderCommandPrefix(commandConfig, resolveDefaultCommand) {
  if (commandConfig?.mode === "replace") {
    const launch = await resolveProviderLaunch({
      commandConfig,
    });
    return {
      command: launch.command,
      args: launch.args,
    };
  }
  const defaultCommand = await resolveDefaultCommand();
  const launch = await resolveProviderLaunch({
    commandConfig,
    defaultBinary: {
      command: defaultCommand,
      resolvePath: async () => defaultCommand,
    },
  });
  return {
    command: launch.command,
    args: launch.args,
  };
}
let cachedShellEnv = null;
export function resolveShellEnv() {
  if (cachedShellEnv) {
    return cachedShellEnv;
  }
  cachedShellEnv = { ...process.env };
  return cachedShellEnv;
}
export function migrateProviderSettings(raw, builtinProviderIds) {
  const migrated = {};
  const builtinProviderIdSet = new Set(builtinProviderIds);
  for (const [providerId, value] of Object.entries(raw)) {
    const parsedNew = ProviderOverrideSchema.safeParse(value);
    if (parsedNew.success) {
      migrated[providerId] = parsedNew.data;
      continue;
    }
    const parsedOld = ProviderRuntimeSettingsSchema.safeParse(value);
    if (!parsedOld.success) {
      continue;
    }
    const nextEntry = {};
    const command = parsedOld.data.command;
    if (command?.mode === "append") {
      continue;
    }
    if (command?.mode === "replace") {
      nextEntry.command = command.argv;
    }
    if (parsedOld.data.env) {
      nextEntry.env = parsedOld.data.env;
    }
    if (!builtinProviderIdSet.has(providerId) && nextEntry.extends === undefined) {
      delete nextEntry.extends;
    }
    migrated[providerId] = nextEntry;
  }
  return ProviderOverridesSchema.parse(migrated);
}
// Env vars that indicate a running Claude Code session. If the daemon itself is
// launched from inside Claude Code (e.g. by a Paseo agent), these leak into
// child processes and cause "cannot be launched inside another session" errors.
const PARENT_SESSION_ENV_VARS = [
  "CLAUDECODE",
  "CLAUDE_CODE_ENTRYPOINT",
  "CLAUDE_CODE_SSE_PORT",
  "CLAUDE_AGENT_SDK_VERSION",
];
function collectProviderEnvOverlays(runtimeSettings, overlays) {
  return [runtimeSettings?.env, ...overlays].filter((overlay) => !!overlay);
}
export function createProviderEnvSpec(options = {}) {
  const overlays = collectProviderEnvOverlays(options.runtimeSettings, options.overlays ?? []);
  const envOverlay = Object.assign({}, ...overlays);
  for (const key of PARENT_SESSION_ENV_VARS) {
    envOverlay[key] = undefined;
  }
  return {
    ...(options.baseEnv ? { baseEnv: options.baseEnv } : {}),
    envOverlay,
  };
}
export function createProviderEnv(options = {}) {
  const spec = createProviderEnvSpec(options);
  return createExternalProcessEnv(spec.baseEnv ?? process.env, spec.envOverlay);
}
export async function isProviderCommandAvailable(commandConfig, resolveDefaultCommand) {
  try {
    if (commandConfig?.mode === "replace") {
      const launch = await resolveProviderLaunch({
        commandConfig,
      });
      const availability = await checkProviderLaunchAvailable(launch);
      return availability.available;
    }
    const defaultCommand = await resolveDefaultCommand();
    const defaultBinary = {
      command: defaultCommand,
      resolvePath: async () => defaultCommand,
    };
    const launch = await resolveProviderLaunch({ commandConfig, defaultBinary });
    const availability = await checkProviderLaunchAvailable(launch, defaultBinary);
    return availability.available;
  } catch {
    return false;
  }
}
//# sourceMappingURL=provider-launch-config.js.map
